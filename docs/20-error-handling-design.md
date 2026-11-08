# 20 · Dagster 错误处理与故障恢复方案

> 适用版本：Dagster 1.13.22，Python 3.11。  
> 适用范围：本项目当前的 `retail.defs`，以及后续采用 Helm + Postgres + 多 code location 的生产部署。  
> 本文是设计方案，不替代具体实现。文中 API 名和行为已经在本地 1.13.22 源码中核对，并用临时资产
> 验证过 `dg.Failure`、`allow_retries` 和 `RetryPolicy` 的事件行为。

## 20.1 结论先行

建议把错误处理拆成三个平面，而不是在资产里到处写 `try/except`：

1. **预防**：契约校验、阻塞式 Asset Check、幂等写入、资源隔离，尽量让错误在污染下游前发生。
2. **收敛**：统一异常分类和错误元数据；短期错误在 step 内重试，数据、配置、权限类错误立即失败。
3. **恢复与告警**：`run_failure_sensor` 负责把失败转成 incident，外部存储负责去重、升级、关闭和补偿；
   再增加一个对账 job，避免 sensor tick 丢失后告警永久遗漏。

最关键的工程决策有四个：

- **不要给所有资产都加 `RetryPolicy`**。Dagster 的 `RetryPolicy` 重试整个 step，而且默认会重试该 step
  抛出的普通异常；可预期的数据、配置、权限错误必须用 `dg.Failure(..., allow_retries=False)` 明确禁止重试。
- **Asset Check 负责“数据是否可信”，`Failure` 负责“这次执行为什么失败”，`run_failure_sensor` 负责
  “失败后通知谁”，三者不要互相替代。**
- **成功必须以提交和校验完成为准**。Parquet 写入要原子替换，DuckDB 更新要放在同一事务里；否则重试会
  掩盖半成品，甚至删除旧的好数据。
- **跨 run 的业务重试不能只依赖 Dagster**。`RetryPolicy` 只覆盖当前 run 内的 step；跨小时的退避、人工审核、
  最大尝试次数和死信队列，需要 incident/task 表和补偿 job 自己管理。

## 20.2 目标与边界

本方案解决以下问题：

- 失败原因可被机器识别，不依赖开发人员阅读一段自由文本日志。
- 可恢复错误有限次、可观测、带退避地自动重试；不可恢复错误快速失败，不浪费时间和成本。
- 脏数据、半成品和错误分区不进入下游。
- 故障能定位到 asset、partition、run、code location 和 owner。
- 告警不重复轰炸，重试耗尽后只发一次，恢复后能自动关闭。
- 失败恢复动作有明确入口：重跑 step、重跑分区、重建数仓层、隔离坏数据或回滚代码。

本方案暂不把 Dagster 当作完整工单系统，也不建议直接读取 Dagster 内部的 Postgres 表。run、step、
event、asset check 的权威记录仍由 Dagster instance 保存；业务侧只维护自己的 incident 与恢复状态。

## 20.3 当前项目盘点

| 现有能力 | 当前实现 | 主要缺口 |
| --- | --- | --- |
| step 重试 | `raw_orders` 使用指数退避重试 | 重试策略只挂在读文件资产；未区分异常类型；其他 I/O 资产没有统一策略 |
| 数据质量 | 3 个 Asset Check，其中 1 个 blocking | 只有事实表收入检查阻断下游；缺少 schema、分区日期、行数波动、主数据完整性等关键门禁 |
| run 失败通知 | `retail_failure_alert` 只写日志 | 未设置 `default_status=RUNNING`；只监控 `daily_pipeline_job`；没有结构化错误码、去重和关闭 |
| 运行级重试 | Helm 中 `runRetries.enabled: false` | worker 崩溃、Pod 被驱逐等非 step 故障没有 run 级恢复策略 |
| 产物写入 | Parquet 直接写目标文件；DuckDB 先删后插 | 非原子写入可能留下半成品；DuckDB 失败时可能已删除旧分区 |
| 告警闭环 | 无 incident 存储、无对账、无恢复通知 | sensor tick 失败或漏处理后，无法补偿；无法统计 MTTR 和重复故障 |
| 故障定位 | metadata 中有路径、行数、分区等 | 失败事件还没有统一 `error_code`、owner、runbook 等字段 |

当前最应该先修的不是再增加一个“大而全”的异常捕获器，而是建立统一错误契约，并把现有几个关键失败点
接入该契约。

## 20.4 失败模型

### 20.4.1 四种处理结果

每次失败都应落到以下四种结果之一：

| 结果 | 适用场景 | Dagster 落点 |
| --- | --- | --- |
| 自动重试 | 网络抖动、限流、临时锁、短暂上游不可用 | `RetryPolicy` 或显式 `dg.RetryRequested` |
| 快速阻断 | schema 漂移、配置错误、权限错误、关键质量规则失败 | `dg.Failure(allow_retries=False)` 或 blocking `AssetCheck` |
| 隔离降级 | 少量坏行、非关键字段异常、单个文档解析失败 | 写入 quarantine/异常表，主链路继续；指标或 WARN check 记录 |
| 观察 | 可疑但不足以阻断的数据波动 | `severity=WARN`、metadata、dashboard、日报 |

不要让同一个错误既自动重试又立即告警。推荐先消耗重试预算，再进入 incident；只有安全、权限、
代码加载失败等明确不会因等待而恢复的错误才立即告警。

### 20.4.2 失败阶段

故障定位至少区分以下阶段：

| 阶段 | 典型问题 | 首选处理 |
| --- | --- | --- |
| 定义加载 | import 失败、循环依赖、资源 key 拼错 | CI `dagster definitions validate` + 立即失败 |
| 触发 | schedule 未启动、sensor 游标错误、文件未到 | tick 监控、`SkipReason`、单独的上游到达检查 |
| 读取输入 | 文件缺失、对象存储超时、schema 不符 | 缺失数据快速失败；临时 I/O 错误重试 |
| 计算 | 业务规则异常、空指针、第三方 API 错误 | 分类后重试或 `Failure` |
| 写出 | 写入冲突、事务失败、对象存储失败 | 原子写 + 幂等键 + 重试 |
| 质量门禁 | 行数、唯一性、金额范围异常 | blocking check 阻断下游 |
| 下游消费 | BI 读取旧数据、外部 API 回调失败 | freshness/incident + 对账，不靠重跑上游猜测 |

### 20.4.3 失败作用域

- **step 级**：只影响当前 asset op，适合 `RetryPolicy`。
- **asset partition 级**：影响某一天、某个文档或某个租户的数据，是日常回填的主要单位。
- **run 级**：影响一次 job 执行，适合 `run_monitoring` 和 run 级重试。
- **backfill 级**：影响一批分区，需要部分成功、失败清单和可恢复回填。
- **业务任务级**：例如“一篇文档抽取失败”，需要业务库记录状态、尝试次数和人工介入，不能只看 run 状态。

## 20.5 错误分类与处置矩阵

建议在代码中固定以下错误码。错误码一旦对外使用，不要随意改名；如果语义变化，新增版本而不是复用旧码。

| 错误码 | 含义 | 示例 | 默认重试 | 默认级别 | 处理方式 |
| --- | --- | --- | --- | --- | --- |
| `E_TRANSIENT_IO` | 临时 I/O 错误 | S3 超时、DuckDB 暂时锁住 | 是，2-3 次 | P1 | step 重试；耗尽后 incident |
| `E_RATE_LIMIT` | 上游限流/配额不足 | 429、模型网关限流 | 是，带更长退避 | P1 | 重试；持续失败时降并发或熔断 |
| `E_UPSTREAM_NOT_READY` | 上游尚未产出数据 | 当天文件未到、依赖分区未物化 | 否 | P2 | 等待下一 tick/下次 backfill；不刷错误页 |
| `E_DATA_MISSING` | 必需输入缺失 | 客户快照缺失、分区文件不存在 | 否 | P1 | 立即失败；修复上游或补文件后重跑 |
| `E_DATA_CONTRACT` | schema/类型/枚举不符合契约 | 必需列消失、日期格式变化 | 否 | P1 | 立即失败；暂停下游；通知数据源 owner |
| `E_DATA_QUALITY` | 内容违反质量规则 | 负数收入、孤儿客户过高 | 否或 WARN | P1/P2 | blocking check 阻断；隔离坏数据并给出样本 |
| `E_AUTH` | 凭证无效或过期 | 401/403、Secret 过期 | 否 | P0 | 立即告警；轮换密钥后从失败处重跑 |
| `E_PERMISSION` | 无权限执行操作 | 无写目标表/桶权限 | 否 | P0/P1 | 立即失败；联系平台或安全 owner |
| `E_CONFIG` | 配置不合法 | 资源路径为空、参数组合非法 | 否 | P1 | 立即失败；修复部署配置 |
| `E_RESOURCE` | 资源不足 | OOM、磁盘满、Pod 驱逐 | 有限重试 | P1 | 扩容/调整资源后重跑；不要无限重试 |
| `E_TIMEOUT` | 执行超时或卡死 | API 挂起、查询超过 SLA | 视原因 | P1 | `run_monitoring` 终止 + 限次重试 |
| `E_BUG` | 未预期代码缺陷 | KeyError、空指针、逻辑错误 | 默认不重试 | P0/P1 | 回滚或修复 code version 后重跑 |

P0/P1/P2 是告警优先级，不是 Dagster 的 run 状态。建议定义：

- P0：数据对外不可用、核心链路完全中断、安全或权限事故，立即通知值班。
- P1：核心链路部分失败、数据可信度受影响，工作时间内及时处理。
- P2：可延迟处理的数据波动或上游未就绪，进入日报或趋势面板。

## 20.6 Dagster 原语选型

| 需求 | 使用的 Dagster 机制 | 不应做的事 |
| --- | --- | --- |
| 当前 step 内的临时错误重试 | `dg.RetryPolicy` | 不要把整个 job 的所有失败都交给全局 run 重试 |
| 在代码中明确请求重试 | `dg.RetryRequested(max_retries=..., seconds_to_wait=...)` | 不要和 `RetryPolicy` 叠加形成双重重试 |
| 可预期的业务失败 | `dg.Failure(description, metadata, allow_retries=False)` | 不要打印日志后静默 `return` |
| 检测数据质量 | `@dg.asset_check` + `dg.AssetCheckResult` | 不要用 check 代替输入读取异常处理 |
| 阻断脏数据进入下游 | `blocking=True` + `severity=ERROR` | 不要所有 check 都设为 blocking |
| 失败通知 | `@dg.run_failure_sensor` | 不要把 sensor 当成可靠消息队列 |
| 成功/失败状态回推 | `@dg.run_status_sensor` | 不要读取 Dagster 内部数据库表 |
| 超时和卡死 | `run_monitoring` | 不要只依赖业务代码里的 `timeout` |
| 资源保护 | `pool`、`concurrency_key`、run 并发上限 | 不要在重试时绕过配额 |
| 分批回填 | `BackfillPolicy`、分区选择器 | 不要一次回填几百个分区且无并发上限 |

### 20.6.1 `RetryPolicy` 的真实语义

在 1.13.22 中，资产 step 发生普通异常时，若配置了 `RetryPolicy`，Dagster 会把异常转换成
`RetryRequestedFromPolicy` 并请求重试。`dg.Failure` 默认也会遵循重试策略；只有显式设置
`allow_retries=False` 才会绕过它并立即失败。

本机临时验证结果：

- `dg.Failure(..., allow_retries=False)` 即使配置了 `RetryPolicy(max_retries=2)`，也只执行一次；
- 普通 `TimeoutError` 在同样策略下产生 2 个 `STEP_UP_FOR_RETRY`，第 3 次失败后进入 `STEP_FAILURE`；
- `Failure` 的 metadata 会出现在 `step_failure_data.user_failure_data.metadata`，可被 failure sensor 读取。

这给了一个清晰模式：**允许重试的资产可以保留 `RetryPolicy`，但所有已知的不可重试错误都包装成
`dg.Failure(..., allow_retries=False)`。**

## 20.7 统一错误契约

### 20.7.1 错误规格

建议新增一个公共包，例如 `platform/error_handling/`，至少包含：

```text
platform/error_handling/
├── taxonomy.py       # ErrorSpec、错误码、严重级别
├── dagster.py        # 转换为 dg.Failure、元数据清洗
├── alerting.py       # incident upsert、去重、通知 outbox
├── sensors.py        # failure/status sensor
└── reconciliation.py # 漏告警对账 job
```

核心数据结构示例：

```python
from dataclasses import dataclass
from typing import Literal, Mapping, NoReturn

Severity = Literal["P0", "P1", "P2"]


@dataclass(frozen=True)
class ErrorSpec:
    code: str
    severity: Severity
    retryable: bool
    owner: str
    runbook: str


DATA_CONTRACT = ErrorSpec(
    code="E_DATA_CONTRACT",
    severity="P1",
    retryable=False,
    owner="data-platform@example.com",
    runbook="https://runbook.example.com/data-contract",
)

TRANSIENT_IO = ErrorSpec(
    code="E_TRANSIENT_IO",
    severity="P1",
    retryable=True,
    owner="data-platform@example.com",
    runbook="https://runbook.example.com/transient-io",
)
```

### 20.7.2 转换为 Dagster Failure

```python
import dagster as dg


def raise_pipeline_failure(
    spec: ErrorSpec,
    description: str,
    *,
    metadata: Mapping[str, object] | None = None,
) -> NoReturn:
    safe_metadata: dict[str, object] = {
        "error_code": spec.code,
        "severity": spec.severity,
        "retryable": spec.retryable,
        "owner": spec.owner,
        "runbook": spec.runbook,
    }
    caller_metadata = dict(metadata or {})
    reserved = set(safe_metadata)
    conflicts = reserved.intersection(caller_metadata)
    if conflicts:
        raise ValueError(f"metadata cannot override reserved keys: {sorted(conflicts)}")
    safe_metadata.update(caller_metadata)

    raise dg.Failure(
        description=description,
        metadata=safe_metadata,
        allow_retries=spec.retryable,
    )
```

元数据约定：

| key | 必填 | 说明 |
| --- | --- | --- |
| `error_code` | 是 | 稳定机器码，用于告警路由和统计 |
| `severity` | 是 | `P0`、`P1`、`P2` |
| `retryable` | 是 | 是否允许 step 重试 |
| `owner` | 是 | 处置责任人或团队 |
| `runbook` | 是 | 故障手册链接 |
| `asset_key` | 建议 | 资产名，可从 context 补充 |
| `partition_key` | 建议 | 分区，可从 context 补充 |
| `source_system` | 建议 | 上游系统名 |
| `bad_rows` | 建议 | 命中坏数据数量，不放大样本 |
| `sample_id` | 建议 | 便于定位的单条样本 ID，注意敏感信息 |
| `fingerprint` | 建议 | 用于告警去重的稳定哈希 |

禁止把凭证、完整 URL query、用户个人信息、超大 DataFrame 或完整堆栈放进 metadata。日志可以保留
受控的堆栈，但 metadata 应该是可索引、可展示的结构化摘要。

### 20.7.3 fingerprint

不要直接对整段异常消息做哈希，因为消息里经常带随机的请求 ID、时间或文件路径，导致同一个故障
每次都生成新 incident。建议：

```text
fingerprint = hash(env, code_location, job_or_asset, partition_scope, error_code, cause_class)
```

如果确实需要区分不同的上游接口，再加入稳定的 `source_system` 或 `operation`。不要在 fingerprint 中
包含随机 trace ID。

## 20.8 资产边界写法

### 20.8.1 推荐的异常处理顺序

资产内部按以下顺序处理：

1. 校验输入是否存在、配置是否完整。
2. 对已知临时错误进行有限重试。
3. 对 schema、权限、数据质量等已知永久错误抛 `Failure(allow_retries=False)`。
4. 未知异常不要吞掉，让 Dagster 保留原始堆栈。
5. 只有成功写入并完成必要校验后才返回结果。

### 20.8.2 读取资产示例

```python
import dagster as dg
import pandas as pd

from platform.error_handling.dagster import raise_pipeline_failure
from platform.error_handling.taxonomy import DATA_MISSING, DATA_CONTRACT, TRANSIENT_IO

REQUIRED_COLUMNS = {"order_id", "customer_id", "amount", "status", "order_date"}


@dg.asset(
    partitions_def=DAILY,
    group_name="bronze",
    retry_policy=dg.RetryPolicy(
        max_retries=2,
        delay=2,
        backoff=dg.Backoff.EXPONENTIAL,
        jitter=dg.Jitter.PLUS_MINUS,
    ),
    io_manager_key="parquet_io",
)
def raw_orders(context: dg.AssetExecutionContext) -> pd.DataFrame:
    path = RAW_DIR / f"orders_{context.partition_key}.csv"

    try:
        df = pd.read_csv(path)
    except FileNotFoundError as exc:
        raise_pipeline_failure(
            DATA_MISSING,
            f"缺少订单文件 {path.name}",
            metadata={
                "asset_key": "raw_orders",
                "partition_key": context.partition_key,
                "path": str(path),
            },
        )
        raise exc
    except (TimeoutError, ConnectionError) as exc:
        raise_pipeline_failure(
            TRANSIENT_IO,
            f"读取订单文件失败: {type(exc).__name__}",
            metadata={
                "asset_key": "raw_orders",
                "partition_key": context.partition_key,
                "path": str(path),
            },
        )

    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise_pipeline_failure(
            DATA_CONTRACT,
            "订单文件缺少必需字段",
            metadata={
                "asset_key": "raw_orders",
                "partition_key": context.partition_key,
                "missing_columns": sorted(missing),
            },
        )

    return df
```

注意：

- `raise_pipeline_failure` 返回 `NoReturn`，表示调用后控制流一定不会继续。
- 实际项目应按资源类型捕获精确异常，例如 S3/HTTP 客户端自己的超时和限流异常；不要用宽泛的
  `OSError` 把所有权限问题都误判成临时错误。
- `DATA_MISSING` 不应该重试。文件没到时由 sensor 下一次 tick 或人工补偿处理。
- `TRANSIENT_IO` 通过 `RetryPolicy` 消耗重试预算；重试耗尽后才会成为最终失败。
- 不要捕获 `Exception` 后统一重试，否则 `E_CONFIG`、`E_DATA_CONTRACT` 也会被重跑。

### 20.8.3 写入资产示例

写入资产要区分“计算失败”和“提交失败”：

```python
@dg.asset(...)
def warehouse_daily_sales(...):
    try:
        staging, summary = build_staging(...)
    except ValueError as exc:
        raise_pipeline_failure(
            DATA_QUALITY,
            "事实表校验失败，未进入数仓",
            metadata={"partition_key": context.partition_key},
        )

    commit_partition_transactionally(
        warehouse=warehouse,
        partition_key=context.partition_key,
        staging=staging,
    )
    return dg.MaterializeResult(metadata=summary)
```

`commit_partition_transactionally` 必须保证：失败时旧分区仍然存在，成功时新分区完整替换。否则
“先删后插”的失败会制造比原始故障更严重的数据事故。

## 20.9 Asset Check 与隔离策略

### 20.9.1 Check 的职责

Asset Check 处理“数据已经算出，是否可信”，适合：

- 行数、空值率、唯一性、枚举范围；
- 分区日期是否与 partition key 一致；
- 上下游行数是否守恒；
- 主数据命中率；
- 金额、数量、比例的合理区间；
- 关键业务指标的同比/环比波动。

运行时异常（文件打不开、网络超时、schema 无法解析）不适合写成 check，应在资产计算阶段处理。

### 20.9.2 阻断级别

| 规则 | 建议 |
| --- | --- |
| 可能导致下游错误或合规风险 | `severity=ERROR` + `blocking=True` |
| 需要关注但可容忍 | `severity=WARN` + `blocking=False` |
| 只做趋势观察 | 写入资产 metadata 或单独指标资产 |

当前项目中：

- `revenue_non_negative` 是 blocking，符合“污染下游前止损”的原则。
- `no_orphan_customers` 是 WARN，适合观察主数据质量，但若孤儿客户比例超过阈值，应升级为 blocking。
- `row_count_sane` 目前是普通 ERROR check，建议补充“相对昨日波动”和“分区非空”两类规则。

### 20.9.3 隔离而不是静默丢弃

对于只有少量坏行的场景，推荐同时产出两份结果：

1. 主资产只保留合法记录；
2. quarantine 资产保存坏行、拒绝原因、源文件、分区和规则 ID。

当坏行比例低于阈值时，主链路继续，WARN check 记录比例；超过阈值时，blocking check 失败，主链路停止。
这样既不会因为一条坏记录让整个批次停摆，也不会让坏数据悄悄消失。

## 20.10 重试分层

### 20.10.1 三层重试

| 层级 | 执行者 | 适用 | 建议 |
| --- | --- | --- | --- |
| step 重试 | Dagster `RetryPolicy` / `RetryRequested` | 当前 run 内的临时错误 | 2-3 次，指数退避 + jitter |
| run 重试 | Dagster auto re-execution | worker 崩溃、Pod 驱逐、进程异常终止 | 只重试基础设施失败，不重试 step failure |
| 业务重试 | incident/task 表 + 补偿 job | 跨小时等待上游、人工修复、租户任务 | 有最大尝试次数、下次执行时间和死信状态 |

### 20.10.2 step 重试规则

推荐默认策略：

```python
TRANSIENT_RETRY = dg.RetryPolicy(
    max_retries=2,
    delay=2,
    backoff=dg.Backoff.EXPONENTIAL,
    jitter=dg.Jitter.PLUS_MINUS,
)
```

适用对象：

- 远程 API 读；
- 对象存储读；
- 数据库短暂锁冲突；
- 可重复的纯计算。

不适用对象：

- 文件永久缺失；
- schema 漂移；
- 配置/权限错误；
- 已确定的数据质量阻断；
- 非幂等的外部副作用。

### 20.10.3 run 重试规则

目标态建议把 run 重试限定为“基础设施恢复”，而不是业务失败重试：

```yaml
run_retries:
  enabled: true
  max_retries: 1
  retry_on_asset_or_op_failure: false
```

含义：

- Pod 被驱逐、进程异常退出等 worker 故障可以重新执行；
- asset/op 失败只依赖资产级 `RetryPolicy`，避免同一故障被重试两遍；
- 生产上仍要用 `run_monitoring` 限制超时和最大运行时间，防止卡死的 run 占满并发槽。

当前 Helm 配置将 `runRetries.enabled` 设为 `false`，可以作为第一阶段保守策略。等 asset 级错误分类稳定后，
再打开“只重试基础设施失败”的 run 重试。

### 20.10.4 业务重试

跨 run 的重试必须有外部状态，例如：

```text
task_id, asset_key, partition_key, attempt_no, max_attempts,
next_retry_at, status, last_error_code, root_run_id, resolved_at
```

补偿 job 每次只选择 `status=pending AND next_retry_at <= now()` 的任务，按退避时间发起 backfill。
超过 `max_attempts` 后进入 `dead_letter`，不再自动重试，要求人工处置。

不要在 sensor 里写 `while` 循环等待，也不要在失败回调里无限制地启动新 run。

## 20.11 写入和提交的原子性

### 20.11.1 Parquet IO Manager

当前 `ParquetIOManager` 直接写目标路径。建议改成：

```python
tmp_path = path.with_suffix(path.suffix + ".tmp")
obj.to_parquet(tmp_path, index=False)
tmp_path.replace(path)
```

并在 metadata 中记录：

- `target_path`；
- `rows`；
- `columns`；
- `file_size`；
- `write_mode="atomic_replace"`。

临时文件名应包含 run_id 或 UUID，避免同一分区并发写入时互相覆盖。生产环境的对象存储应使用
对象存储自己的原子替换/提交语义，不要把本地 `Path.replace` 生搬到 S3。

### 20.11.2 DuckDB 分区更新

当前 `DELETE` 和 `INSERT` 必须放在同一事务中：

```python
with warehouse.get_connection() as conn:
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute(
            "DELETE FROM marts.daily_sales WHERE order_date = ?",
            [context.partition_key],
        )
        conn.execute(
            "INSERT INTO marts.daily_sales SELECT ... FROM incoming"
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
```

或者使用“写 staging 表，校验后原子换表”的模式。关键验收标准是：故意让 `INSERT` 失败时，
已有分区不能被删除，run 不能显示成功。

### 20.11.3 外部副作用

调用邮件、IM、业务 API、发布系统时：

- 使用业务幂等键；
- 先写本地 outbox，再由独立 worker 发送；
- 记录外部请求 ID；
- 失败通知不能阻塞核心数据落库；
- 不要在数据事务提交前发送“成功”通知。

## 20.12 告警与 incident 闭环

### 20.12.1 目标链路

```text
asset step failure
  -> dg.Failure(metadata=error_code/severity/owner/runbook)
  -> run_failure_sensor
  -> incident upsert + outbox
  -> dedupe / aggregation
  -> Slack / PagerDuty / ticket
  -> 成功 run_status_sensor 或成功 materialization
  -> close incident
  -> reconciliation job 补偿漏掉的 sensor tick
```

### 20.12.2 failure sensor 的职责

`run_failure_sensor` 应做四件事：

1. 从 step failure event 中提取结构化 metadata；
2. 计算 fingerprint；
3. 对 incident 做幂等 upsert，而不是每次都直接发消息；
4. 将通知写入 outbox，由单独发送器负责重试和限流。

建议平台级 sensor 使用 `monitor_all_code_locations=True`，放在独立 code location，而不是每个业务
location 各写一份。这样 code location 的失败上报和业务资产解耦，也便于统一 owner 路由。

读取失败元数据的核心代码形状如下：

```python
@dg.run_failure_sensor(
    name="failure_incident_router",
    monitor_all_code_locations=True,
    default_status=dg.DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=30,
)
def failure_incident_router(context: dg.RunFailureSensorContext) -> None:
    # run 级自动重试的中间 attempt 不应生成最终 Pager。
    if context.dagster_run.tags.get("dagster/will_retry") == "true":
        return

    for event in context.get_step_failure_events():
        failure_data = event.step_failure_data
        user_data = failure_data.user_failure_data

        if user_data is None:
            metadata = {
                "error_code": "E_BUG",
                "severity": "P1",
                "owner": "data-platform@example.com",
            }
            summary = failure_data.error.message if failure_data.error else "unknown failure"
        else:
            metadata = {key: value.value for key, value in user_data.metadata.items()}
            summary = user_data.description or "pipeline failure"

        emit_incident_event(
            run_id=context.dagster_run.run_id,
            job_name=context.dagster_run.job_name,
            step_key=event.step_key,
            partition_key=context.partition_key,
            tags=dict(context.dagster_run.tags),
            metadata=metadata,
            summary=summary,
        )
```

`emit_incident_event` 应调用平台统一的 incident ingestion API，或写入独立的 outbox，而不是在 sensor
内直接完成所有通知逻辑。普通未知异常没有 `user_failure_data`，应回退为 `E_BUG` 并保留原始错误类名和
受控堆栈摘要。

### 20.12.3 重试期间不要告警

step 重试期间的 `STEP_UP_FOR_RETRY` 只记录指标，不发最终告警。最终告警只在：

- 重试耗尽；
- 达到 run 级重试上限；
- 明确不可重试的 `Failure(allow_retries=False)`；
- 资源超时或 run monitor 判定失败。

如果使用 run 级自动重试，要结合 `dagster/will_retry`、`dagster/retry_number` 和 run group 判断是否是
中间失败。不要把中间 attempt 当成最终 incident。

### 20.12.4 incident 去重

建议以以下 key 做 upsert：

```text
(env, code_location, job_name, asset_key, partition_scope, error_code, fingerprint)
```

同一 key 在冷却窗口内只增加 `occurrence_count` 和 `last_seen_at`，不重复发消息。冷却窗口可按级别配置：

- P0：5 分钟；
- P1：30 分钟；
- P2：24 小时或日报。

### 20.12.5 恢复和关闭

incident 不能仅因为“后来有任意 run 成功”就关闭。建议关闭条件：

- 同一 asset 的同一 partition 成功物化；
- 必要的 blocking check 恢复通过；
- 若故障影响全量数据，需要确认补偿 backfill 完成；
- 外部副作用有明确的幂等确认。

可以用 `run_status_sensor` 监听成功事件，也可以在每个分区的成功 materialization 后写一条 recovery
event。对于 stop-the-world 故障，还需人工确认 incident 关闭，避免一次偶然成功掩盖批量问题。

### 20.12.6 对账 job

sensor tick 本身可能失败、daemon 可能短暂不可用，因此必须有对账 job：

- 按时间窗口扫描 Dagster run 记录，不读内部表；
- 将未写入 incident 的终态 run 补写；
- 将已恢复但仍 open 的 incident 关闭；
- 将长时间 queued、无心跳、超时的 run 标记为待处理；
- 对 notification outbox 做重试和死信。

对账 job 应使用 cursor 和幂等 upsert，重复执行不会产生重复通知。

### 20.12.7 两级可观测面

业务告警不能只依赖宿主机上的一个 sensor。建议：

- Dagster 内部：run/step/asset check/failure sensor；
- 平台外部：daemon heartbeat、code location 可用性、run queue 延迟、sensor tick 连续失败、
  Postgres/对象存储/Slack 网关健康检查。

否则当 code location 或 sensor 本身故障时，会出现“没有告警就是一切正常”的假象。

## 20.13 Incident 数据模型

建议在业务库中维护两张表，而不是把业务状态塞进 Dagster tag。

### 20.13.1 `ops.incident`

| 字段 | 说明 |
| --- | --- |
| `incident_id` | 主键 UUID |
| `env` | dev/staging/prod |
| `code_location` | code location 名 |
| `job_name` | Dagster job 名 |
| `asset_key` | 资产名 |
| `partition_key` | 分区，非分区时为空 |
| `error_code` | 统一错误码 |
| `severity` | P0/P1/P2 |
| `fingerprint` | 去重指纹 |
| `status` | open/ack/resolved/suppressed |
| `owner` | 责任人/团队 |
| `first_run_id` | 首次失败 run |
| `latest_run_id` | 最近失败 run |
| `root_run_id` | 重试链路根 run |
| `first_seen_at` | 首次发生 |
| `last_seen_at` | 最近发生 |
| `occurrence_count` | 发生次数 |
| `resolved_at` | 关闭时间 |
| `runbook_url` | 手册链接 |
| `last_error_summary` | 脱敏摘要 |

### 20.13.2 `ops.notification_outbox`

| 字段 | 说明 |
| --- | --- |
| `outbox_id` | 主键 |
| `incident_id` | 关联 incident |
| `channel` | slack/pagerduty/email |
| `payload_json` | 已脱敏消息 |
| `status` | pending/sending/sent/failed |
| `attempt_no` | 发送次数 |
| `next_attempt_at` | 下次发送时间 |
| `external_id` | 外部系统消息 ID |

这样 Dagster 的失败通知失败时，不会反过来污染任务执行；发送器可以独立重试、限流和人工重放。

## 20.14 恢复手册

| 错误码 | 自动动作 | 人工动作 | 验证 |
| --- | --- | --- | --- |
| `E_TRANSIENT_IO` | step 退避重试 | 检查对象存储/DB 状态 | 重试成功且 materialization 正常 |
| `E_RATE_LIMIT` | 更长退避；必要时降并发 | 调整配额或错峰 | 连续成功且无持续 429 |
| `E_UPSTREAM_NOT_READY` | 等待下一 tick | 跟进上游 owner | 输入文件/分区到达 |
| `E_DATA_MISSING` | 不重试 | 补文件、补分区或修调度 | 缺失检查通过 |
| `E_DATA_CONTRACT` | 不重试；阻断下游 | 对齐 schema，修复上游 | 新 run 通过 schema check |
| `E_DATA_QUALITY` | 阻断或隔离 | 查看 bad samples 和规则 | 质量指标回到阈值内 |
| `E_AUTH`/`E_PERMISSION` | 不重试 | 轮换 Secret、补权限 | 认证/授权探测成功 |
| `E_CONFIG` | 不重试 | 修 Helm/环境变量/配置 | `definitions validate` + 试跑 |
| `E_RESOURCE` | 限次重试 | 扩资源、调 pool 或拆分分区 | 监控 CPU/内存/磁盘正常 |
| `E_TIMEOUT` | run monitor 终止 | 检查慢查询/外部 API | 时长回到 SLA 内 |
| `E_BUG` | 默认不自动重试 | 回滚 code version 或修复 | 从失败处 re-execute 成功 |

### 20.14.1 重跑粒度

- 业务逻辑修复：从失败 asset 开始重跑，必要时带上游。
- 仅数仓写入失败：使用 `retail_warehouse_reload` 重跑 gold 层。
- 单分区数据问题：只重跑对应 partition，不扩大范围。
- 批量补数：走 backfill，设置并发上限和 `BackfillPolicy`。
- code version 变化：确认下游 `code_version` 触发重算符合预期后再回填。

### 20.14.2 失败现场保留

当前 Helm 已配置失败 run Pod 保留一段时间，计算日志落对象存储是生产必选项。建议保留：

- 失败 run 的 compute logs；
- 失败 asset 的输入路径、分区、版本和命中样本；
- 对应 code location 镜像 tag；
- 当天的资源/并发配置；
- 外部依赖请求 ID。

不要只保留一行 `message`，否则同一条错误在几天后很难复现。

## 20.15 指标与 SLO

建议至少观测：

| 指标 | 用途 |
| --- | --- |
| run success rate | 总体稳定性 |
| step retry rate / exhausted retry rate | 区分偶发抖动和持续故障 |
| incident count by error_code | 找主要故障来源 |
| MTTA / MTTR | 告警响应和恢复效率 |
| failed partition age | 多少数据分区仍在坏状态 |
| queued run wait time | 并发或 daemon 容量问题 |
| stale asset count | 数据新鲜度不达标 |
| check failure rate by rule | 质量规则是否过严或失效 |
| code location load failures | 发布/依赖/镜像问题 |
| sensor tick failure streak | 触发机制是否失效 |

Dagster 的 FreshnessPolicy / freshness check 可用于“数据是否按时更新”，但要区分“资产很久没物化”
和“物化失败”。二者在告警上应分别归因。

## 20.16 部署配置建议

### 20.16.1 `dagster.yaml`

目标态可参考：

```yaml
run_monitoring:
  enabled: true
  start_timeout_seconds: 600
  max_runtime_seconds: 28800
  poll_interval_seconds: 120

run_retries:
  enabled: true
  max_retries: 1
  retry_on_asset_or_op_failure: false
```

如果仍使用当前 Helm 的保守配置，至少保留 `run_monitoring`，并将所有 step 重试限定在有幂等保证的
资产上。

### 20.16.2 Sensor 状态

- 生产告警 sensor 必须显式 `default_status=RUNNING`；
- 文件到达 sensor 可以先 `STOPPED`，验收后逐个打开；
- 平台级失败 sensor 建议 `monitor_all_code_locations=True`；
- sensor 的 tick 失败、连续 skip 和游标异常要进入运维面板；
- 告警发送失败不应让业务 run 重跑。

### 20.16.3 当前配置需要补的点

1. 给 `retail_failure_alert` 加 `default_status=dg.DefaultSensorStatus.RUNNING`。
2. 至少监控 `daily_pipeline_job` 与 `warehouse_reload_job`，或改用平台级全量监控。
3. 把 `RetailSettings` 的日志告警替换成可注入的 AlertGateway；sensor 只负责生成结构化事件。
4. 生产环境增加外部 daemon/code location 健康检查，避免 Dagster 自身故障时静默。

## 20.17 分阶段落地

### 阶段 0：契约和测试

产出：

- `ErrorSpec`、错误码表、owner/runbook 映射；
- `raise_pipeline_failure` 和 metadata 脱敏规则；
- 失败注入测试，验证可重试与不可重试分支。

验收：

- 任何预期业务失败都能在 step failure event 中看到 `error_code`；
- 数据契约错误在配置 `RetryPolicy` 时仍然只执行一次；
- 临时错误会产生明确的 retry event，耗尽后只产生一个最终失败。

### 阶段 1：资产和写入安全

产出：

- `raw_orders`、`raw_customers`、`stg_orders`、`warehouse_daily_sales` 接入统一错误契约；
- Parquet 原子替换；
- DuckDB 分区事务更新；
- 关键 schema/分区日期/行数 checks。

验收：

- 故意制造缺失列，blocking check/输入校验阻止下游；
- 故意让写入失败，旧分区保留；
- 同一分区重跑两次结果一致；
- 失败 run 的 metadata 能直接定位到文件和分区。

### 阶段 2：incident 闭环

产出：

- `ops.incident`、`ops.notification_outbox`；
- 平台级 `run_failure_sensor`；
- P0/P1/P2 路由、冷却窗口、恢复通知；
- 对账 job 和 notification sender。

验收：

- 同一故障重复 5 次只产生 1 个 open incident；
- retry 中间失败不发最终 Pager；
- 修复后成功物化可关闭对应 incident；
- 手工制造 sensor tick 失败后，对账 job 能补写 incident；
- 发送失败进入 outbox 重试，不影响核心 run。

### 阶段 3：自动恢复和运营

产出：

- 临时错误自动恢复；
- 可安全自动重跑的分区白名单；
- run 级基础设施重试；
- dashboard、runbook、季度故障演练。

验收：

- 平均恢复时间、重试耗尽率、重复故障率可观测；
- 单次故障可以在 1 分钟内看到 asset、partition、owner、runbook 和对应 run；
- 回填失败可按分区重试，不会重复写入或扩大量级。

## 20.18 测试清单

### 20.18.1 单元测试

- `ErrorSpec` 到 `dg.Failure` 的字段映射；
- 不可重试错误不会进入重试；
- 临时错误在重试耗尽后产生失败；
- fingerprint 对同一根因稳定、对随机 trace ID 不敏感；
- 敏感字段被脱敏；
- incident upsert 幂等；
- outbox 重试和死信状态转换。

### 20.18.2 集成测试

- `dg.materialize(..., raise_on_error=False)` 后读取 step failure metadata；
- blocking check 失败后下游不执行；
- 非 blocking WARN check 不阻断运行；
- sensor 从 run failure event 生成 incident；
- 恢复 run 成功后关闭 incident；
- 写入事务失败时旧数据保留。

### 20.18.3 故障演练

建议每季度至少演练：

1. 上游文件晚到 2 小时；
2. 上游 schema 删除一列；
3. 对象存储不可用 5 分钟；
4. 数仓写入中途失败；
5. 一个分区数据量异常暴涨；
6. 告警 webhook 不可用；
7. code location 镜像发布失败；
8. daemon 停机 10 分钟后的对账恢复。

演练目标不是“证明系统不会失败”，而是证明失败时能快速止损、定位和恢复。

## 20.19 反模式

| 反模式 | 后果 | 建议 |
| --- | --- | --- |
| 所有资产统一 `RetryPolicy(max_retries=5)` | 配置/数据错误被重复执行，成本高 | 只对可重试路径配置，永久错误 `allow_retries=False` |
| `except Exception: return []` | 数据静默丢失，run 显示成功 | 抛结构化 `Failure` 或写 quarantine |
| 每个 step failure 直接发 Pager | 重试期间告警风暴 | incident 去重 + 终态告警 |
| 用 failure sensor 直接执行业务补偿 | sensor 失败后状态不一致，无重试保证 | incident + outbox + 对账 job |
| 只记录日志文本，不写错误码 | 无法统计、路由和去重 | 统一 metadata |
| 先删旧分区再尝试插入 | 插入失败后旧数据丢失 | 同事务更新或 staging 换表 |
| 把 Dagster 当工单系统 | 业务状态与 run 状态混杂 | 业务库 incident/task 表 |
| 直接读 Dagster 内部 Postgres 表 | 升级后 schema 变化导致故障 | 使用 `DagsterInstance` 公共 API 或 GraphQL |
| 在 metadata 中放大段数据 | event log 膨胀、泄露敏感信息 | 只放摘要、规则 ID、样本 ID |

## 20.20 建议的下一步

建议按以下顺序提交变更，避免一次改动过大：

1. 新增 `error_handling` 公共包和失败注入测试。
2. 接入 `raw_orders` 与 `warehouse_daily_sales` 两个高风险边界，先验证模式。
3. 把 `retail_failure_alert` 改成结构化 incident 入口，并打开 RUNNING。
4. 增加 Parquet 原子写和 DuckDB 事务测试。
5. 实现 incident 对账 job，再接入真实 Slack/PagerDuty。
6. 最后再评估是否打开 run 级基础设施重试和跨 run 业务重试。

这套方案的核心不是“捕获更多异常”，而是让每一种失败都有明确的归属、处理边界、重试预算和恢复路径。
