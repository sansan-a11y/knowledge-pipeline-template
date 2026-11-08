# 19 · 知识抽取管道的过程校验：Asset Check 方案清单

> 适用对象：`knowledge_extraction` code location（手册抽取链路：VLM 逐页分类 → 动态分区图谱抽取 → 汇总 → 向量索引）
> 前置阅读：`docs/06`（Asset Check 语法与实测坑）、`docs/09`（资产链与权责划分）、`docs/13`（GraphRAG 参考实现）、`docs/18`（版本与 metadata 契约）
> 本文所有 API 形态均在本机 dagster 1.13.22 实测通过，验证记录见 19.10。

## 19.0 三句话原则

1. **硬软分层。** 能卡住下游的硬契约用 `ERROR + blocking`；靠统计或模型判断的软质量用 `WARN`，只做观测与趋势，别一上来就挡路。
2. **LLM 的"正确性"不直接判。** check 里放**零成本、确定性的代理指标**（schema 合法性、枚举范围、图内引用一致性、覆盖率、行数守恒）；昂贵的 LLM-as-judge 与召回评估只做**采样 + WARN**。
3. **check 是控制面，不是业务状态。** 业务状态不写进 check；BFF 在 `run_status_sensor` 回调里读该 run 的 check 结果，全绿才 `PUBLISHED`（对应 `docs/09` §9.6：`RUN_SUCCESS` ≠ `PUBLISHED`）。

## 19.1 三种定义形态与选型

| 形态 | 写法 | 什么时候用 | 代价 |
| --- | --- | --- | --- |
| 外挂式 | `@dg.asset_check(asset=...)` | 默认选择：check 逻辑与资产解耦，可单独迭代 | check 拿不到资产内部中间量 |
| 内联式 | `@dg.asset(..., check_specs=[...])` + 资产内 `yield` | **分区资产首选**：天然拿到 `context.partition_key`，避开 preview API | 逻辑与资产耦合在同一函数 |
| 批量式 | `@dg.multi_asset_check(specs=[...])` | 一次遍历产出多个结论（如扫一遍 chunk 同时判 4 条） | 需自己保证每个 spec 都产出一条结果 |

### 外挂式

```python
@dg.asset_check(
    asset=document_chunks,
    name="chunk_token_bound",
    description="单块 token 不得超过模型上下文预算",
    blocking=True,          # 失败 => 下游不执行
)
def chunk_token_bound(document_chunks: pd.DataFrame) -> dg.AssetCheckResult:
    bad = document_chunks[document_chunks["n_tokens"] > 512]
    return dg.AssetCheckResult(
        passed=bad.empty,
        metadata={
            "rule_id": dg.MetadataValue.text("KE-CHUNK-001"),
            "threshold": dg.MetadataValue.int(512),
            "offending_chunk_ids": dg.MetadataValue.json(bad["chunk_id"].tolist()[:20]),
        },
    )
```

### 内联式（推荐给分区资产）

```python
@dg.asset(check_specs=[
    dg.AssetCheckSpec(name="chunk_token_bound", asset="document_chunks", blocking=True),
    dg.AssetCheckSpec(name="chunk_count_sane",  asset="document_chunks"),
])
def document_chunks(context: dg.AssetExecutionContext, ...):
    sizes = [10, 480, 520]
    yield dg.MaterializeResult(metadata={"chunks": dg.MetadataValue.int(len(sizes))})
    yield dg.AssetCheckResult(check_name="chunk_token_bound", passed=max(sizes) <= 512)
    yield dg.AssetCheckResult(check_name="chunk_count_sane", passed=0 < len(sizes) < 10_000,
                              severity=dg.AssetCheckSeverity.WARN)
```

要点：

- `AssetCheckSpec(name=..., asset=...)` 里的 **`asset` 是必填关键字参数**（省略会报 `TypeError: missing 1 required keyword-only argument`）。
- 内联 check 失败会**连同资产 step 一起 fail**；`blocking=True` 时下游被跳过（实测）。
- 一个资产可以 `check_specs` 挂多条，各自独立 `blocking`。
- 不要在内联 spec 上写 `partitions_def`：它会触发 `PreviewWarning`，且必须与资产的 `PartitionsDefinition` 完全一致；内联形态本来就能从 `context` 拿分区，不需要它。

### 批量式

```python
@dg.multi_asset_check(specs=[
    dg.AssetCheckSpec(name="schema_valid",   asset="extracted_knowledge", blocking=True),
    dg.AssetCheckSpec(name="enums_valid",    asset="extracted_knowledge", blocking=True),
    dg.AssetCheckSpec(name="graph_consistent", asset="extracted_knowledge"),
])
def extraction_contract_checks(context, extracted_knowledge):
    yield dg.AssetCheckResult(check_name="schema_valid", passed=...)
    yield dg.AssetCheckResult(check_name="enums_valid", passed=...)
    yield dg.AssetCheckResult(check_name="graph_consistent", passed=...)
```

## 19.2 severity / blocking / 门禁的真实语义

| 配置 | 结果 |
| --- | --- |
| `severity=ERROR`（默认）+ `blocking=True` | 断言失败 → 资产 step 失败 → **下游被跳过** |
| `severity=ERROR` + `blocking=False` | 记录失败事件，下游继续 |
| `severity=WARN` | 只记警告，不影响 run 状态（**`blocking=True` 也不会生效**） |

两个容易踩的语义点（源码 `AssetCheckSpec` docstring 原文）：

- **门禁只对"失败结果"生效**：如果某条 check 压根没产出结果，下游**照常执行**，只会记一条 warning。所以"必须判"的 check 要保证任何分支都 `yield`。
- **`severity` 是 `AssetCheckResult` 的参数**，不是 `@dg.asset_check` 的参数（写错会报 unexpected keyword argument）。

### 门禁（对应 §9.6 的 `PUBLISHED`）

声明式触发侧可用（实测 `AutomationCondition` 组合能正常解析）：

```python
gate = dg.AutomationCondition.all_checks_match(
    dg.AutomationCondition.check_passed(),
    blocking_only=True,          # blocking_only 只关心 blocking 断言
)

@dg.asset(automation_condition=dg.AutomationCondition.eager() & gate)
def knowledge_document(...):
    ...
```

可用原子条件：`check_passed()`、`check_failed()`；组合器：`all_checks_match(...)`、`any_checks_match(...)`（注意**没有** `all_checks_pass`）。

但**真正决定 `PUBLISHED` 的口径仍在 BFF**：`run_status_sensor` 回调时拉取该 run 的 check evaluations，`all(c.passed for c in ...)` 为真且 run 成功才落 `PUBLISHED`。check 侧只负责"如实产出结果"。

## 19.3 check 怎么拿到数据（对齐"只传 URI + metadata"的架构）

`docs/09` §9.10 明确了"大文件走默认 IO Manager 会丢"的反模式，所以抽取链的资产大概率**不把 DataFrame 当输出**。这直接决定 check 的输入方式：

| 方式 | 声明 | 适用 | 注意 |
| --- | --- | --- | --- |
| 默认参数注入 | `def f(extracted_knowledge: pd.DataFrame)` | 资产确实用 IO Manager 落 DataFrame | 轻量、只读当前分区 |
| `additional_ins` | `additional_ins={"chunks": dg.AssetIn("document_chunks")}` | check 需要**额外读一个上游资产**做交叉校验 | 会作为函数参数注入，需要 IO Manager 能加载 |
| `additional_deps` | `additional_deps=[dg.AssetDep("raw_document")]` | 只声明依赖、**不注入数据**（纯 metadata check，或自己用 Resource 去对象存储/业务库读） | 不加载对象，最省 |
| `required_resource_keys` | `required_resource_keys={"object_store"}` | check 里要用平台 Resource（对象存储、LLM 网关、业务库） | 对应资源 key = 函数参数名 |

```python
@dg.asset_check(
    asset=extracted_knowledge,
    name="chunk_coverage",
    additional_deps=[dg.AssetDep("document_chunks")],   # 不注入，自己去对象存储读
    required_resource_keys={"object_store"},
    blocking=True,
)
def chunk_coverage(context, object_store) -> dg.AssetCheckResult:
    produced = object_store.count_prefix(f"ke/mentions/{context.partition_key}/")
    expected = object_store.load_json(f"ke/chunk_index/{context.partition_key}")["chunk_count"]
    return dg.AssetCheckResult(
        passed=produced == expected,
        metadata={"produced": dg.MetadataValue.int(produced),
                  "expected": dg.MetadataValue.int(expected)},
    )
```

## 19.4 逐阶段 check 清单

总表（★ = 建议第一批就做）：

| 阶段 | 必须（`ERROR` + blocking 候选） | 建议（`WARN` 观测） |
| --- | --- | --- |
| `raw_document` | ★ 原件可读、MIME/后缀正确、`page_count > 0`、checksum 非空、`doc_version` 与业务库一致 | 文件大小分布异常 |
| `parsed_document` | ★ 解析覆盖率（成功页/总页）≥ 阈值、页数与原件一致 | 单页字符数分布、乱码率、低置信度分类页占比、OCR 兜底页占比 |
| `document_chunks` | ★ 单块 token ≤ 上限、空块数 = 0、重组覆盖原文 ≥ 阈值 | 块数区间、n-gram 重复率 |
| `extracted_knowledge` | ★ JSON/Pydantic 校验通过率 = 100%、枚举全在 `Literal` 内、relation 端点 id 必须存在于 entities、与 chunks 的产出完整性守恒 | groundedness 采样、空抽取率、数量相对上批 ±X%、tokens/延迟/重试率 |
| `knowledge_document` | 引用可回溯到源 chunk、实体计数不与上游冲突 | 非空、长度区间 |
| `vector_index` | ★ 维度一致、无 NaN/Inf、向量条数 == 应索引条数、id 无重复 | 范数分布、抽样召回冒烟、索引新鲜度 |

### 19.4.1 `raw_document`：上游就坏，后面全白跑

- **可读性**：对象存储 HEAD 成功、大小 > 0、`Content-Type` 与后缀一致。
- **完整性**：`page_count > 0`；`doc_version` 与业务库登记一致（防止拿错版本）。
- **可解析性**：PDF 头合法 / 加密标志为假。
- 级别：`ERROR + blocking`。原件拿不到，下游没有意义；这类失败应回推 `FAILED` 而不是烧 token。

### 19.4.2 `parsed_document`：VLM 逐页分类的校验

- ★ **解析覆盖率** `成功页 / 总页 ≥ 0.95`：直接决定下游抽到多少知识，是最该 blocking 的一条。
- ★ **页数一致性**：`parsed.page_count == raw.page_count`。
- **文本可得性**：非图页的字符数下限（挡住"整页空白"）。
- `WARN`：单页字符数分布离群、乱码/替换字符率、**低置信度分类页占比**、页类型分布相对上一批漂移。
- `WARN`：`valid_page` 动态分区数为 0（说明整篇没有可抽取页）。
- 相关文档：`docs/04`（动态分区）与 `docs/18`（版本与 metadata 契约）。

### 19.4.3 `document_chunks`：切块参数一定错就要早停

- ★ **单块 token 上限**（如 ≤ 512）：超了会污染 prompt 预算，下游 LLM 调用直接不可用。
- ★ **空块数 = 0**。
- ★ **覆盖率**：chunk 文本重组后覆盖原文 token 比例 ≥ 阈值，防切块丢内容。
- `WARN`：块数区间、**n-gram 重复率**（切块 bug 或重叠参数写错会重复烧 token）。
- 建议把切块参数（目标 token、重叠句数、最小块数）落进资产 metadata，便于 check 引用同一套阈值。

### 19.4.4 `extracted_knowledge`：最值钱的一段，分三组

**A 组：硬契约（纯确定性，零成本，全部 `ERROR + blocking`）**

1. **结构合法性**：每条输出 JSON 可解析 + Pydantic/`response_schema` 校验通过率 = 100%。
2. **枚举合法性**：entity/relation type 全部落在 `Literal` 内（`docs/13` 的 12 类实体 / 15 类关系就是现成枚举）。
3. **图内一致性**：relation 的两个端点 id 必须存在于 entities 集合中——**零成本、零误判、能挡住大部分幻觉**，是最该 blocking 的一条。
4. **产出完整性**：抽取结果覆盖的 chunk 集合 == `document_chunks` 的 chunk 集合（行数守恒），防止静默漏抽。

**B 组：语义质量代理（`WARN`；可信后再收紧到 blocking）**

1. **groundedness**：抽取出的实体名能在源 chunk 原文（或别名字典）里命中，命中率 ≥ 阈值。
2. **空抽取率**：返回 0 实体的 chunk 占比 ≤ 阈值——这条最能提前发现 **prompt 静默失效 / 模型换代回归**。
3. **数量漂移**：本次实体数/关系数相对上批次 ±X%。
4. **重复与冲突**：归一化前的实体重复率、同一实体冲突属性数。

**C 组：成本与运行时（`WARN`，写成 metadata + bounds check，不做重计算）**

- 把 `tokens_in` / `tokens_out` / `llm_calls` / `retry_count` / `latency_ms` 写进 `MaterializeResult` metadata，再用 `build_metadata_bounds_checks` 断言上下界（见 19.5）。
- 好处：不需要在 check 里重跑任何 LLM，就能对成本和延迟异常告警。

**采样型 LLM-as-judge（`WARN`，可选）**：不要全量、不要 blocking，只在 **抽样 chunk** 上跑评分模型：

```python
@dg.asset_check(
    asset=extracted_knowledge,
    name="judge_sample_quality",
    additional_ins={"chunks": dg.AssetIn("document_chunks")},
    required_resource_keys={"judge"},
)
def judge_sample_quality(context, chunks, judge) -> dg.AssetCheckResult:
    sample = chunks.sample(n=min(20, len(chunks)))      # 采样，控制成本
    score = judge.score(sample)
    return dg.AssetCheckResult(
        passed=score >= 0.8,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={"judge_score": dg.MetadataValue.float(score),
                  "sampled": dg.MetadataValue.int(len(sample)),
                  "model": dg.MetadataValue.text("ke-judge-v2")},
    )
```

**顺手把版本写进 metadata**（`docs/18` 的契约）：`model` / `prompt_version` / `schema_version` / `parser_version`。否则 check 失败时无法归因到底是 prompt、模型还是解析器变了。

### 19.4.5 `knowledge_document`：汇总层的可追溯性

- **引用可回溯**：摘要/结论里的实体与断言都能回指源 chunk id（`WARN` 起步，抽检后可升 `ERROR`）。
- **计数守恒**：文档内实体/关系数不与上游冲突（不能凭空多出）。
- `WARN`：非空、长度区间。

### 19.4.6 `vector_index`：入库前的最后一道

- ★ **维度一致**、**无 NaN/Inf**、**向量条数 == 应索引条数**、**id 无重复**（全部 `ERROR + blocking`——坏向量入索引会造成污染且难清理）。
- `WARN`：范数分布、抽样召回冒烟（几个已知 query 的 top-k 命中，慢，适合夜间/巡检 job 而非每次 run）。
- **新鲜度**：用 `build_last_update_freshness_checks`（见 19.5）。

## 19.5 内置工厂：不写代码就能上的 check

```python
fresh = dg.build_last_update_freshness_checks(
    assets=[extracted_knowledge],
    lower_bound_delta=datetime.timedelta(hours=6),   # 6 小时内必须有新物化
    severity=dg.AssetCheckSeverity.WARN,
)

sla = dg.build_time_partition_freshness_checks(
    assets=[vector_index],
    deadline_cron="0 9 * * *",                       # 每天 09:00 前本分区必须完成
)

drift = dg.build_column_schema_change_checks(assets=[extracted_knowledge])   # 字段漂移

cost = dg.build_metadata_bounds_checks(
    assets=[extracted_knowledge],
    metadata_key="tokens_in", min_value=0, max_value=2_000_000,
)

freshness_sensor = dg.build_sensor_for_freshness_checks(freshness_checks=fresh)
```

- `build_column_schema_change_checks` 在**换 prompt / 换模型 / 换解析器**时特别有用，能第一时间发现产物 schema 悄悄变了。
- freshness 类 check 的告警要配 `build_sensor_for_freshness_checks` 产出的 sensor 才有人收到。

## 19.6 可复用骨架

建议在 `knowledge_extraction/` 下：

```text
knowledge_extraction/
├── defs.py               # Definitions：load_assets_from_modules + load_asset_checks_from_modules
├── checks/
│   ├── __init__.py
│   ├── contracts.py      # A 组硬契约（schema / 枚举 / 图内一致 / 守恒）
│   ├── quality.py        # B 组语义质量（groundedness / 空抽取率 / 漂移）
│   ├── infra.py          # C 组（freshness / 成本 bounds / schema change）
│   └── judge.py          # 采样型 LLM-as-judge
└── ...
```

与 `docs/13` 借鉴的装载方式一致：

```python
defs = dg.Definitions(
    assets=dg.load_assets_from_package_module(assets),
    asset_checks=dg.load_asset_checks_from_modules([checks.contracts, checks.quality,
                                                    checks.infra, checks.judge]),
    resources=...,
)
```

**统一 metadata 契约**（CI 可校验）：每条 check 的结果里固定带 `rule_id`、`threshold`、`observed`，失败时带 `offending_*` 样本。这样告警、看板、回溯三处都不用再写解析逻辑。

## 19.7 阈值怎么定：WARN 先行，再收紧

死板的固定阈值在 LLM 管道上大概率误报。建议流程：

1. 新 check 一律先 `severity=WARN`、`blocking=False`，把 `observed` 写进 metadata。
2. 跑够一个批次窗口（建议 ≥ 2 周 / ≥ 200 个分区），用 UI 或查询统计 `observed` 的分布（P1 / P50 / P99）。
3. 用分布定阈值：只在 **P1/P99 之外的明显异常**上收紧为 `ERROR + blocking`。
4. 收紧后保持"可解释"：metadata 里带 `rule_id`，BFF 的失败原因直接引用它。
5. 阈值本身要版本化——放进配置或资产 metadata，改阈值要能回溯。

判断哪些必须 blocking，一个简单判据：**这条问题如果流到下游，会不会污染用户看到的知识，或造成不可逆的索引污染？** 会 → `ERROR + blocking`；只是"可能不准" → `WARN`。

## 19.8 反模式

| 反模式 | 后果 | 更好做法 |
| --- | --- | --- |
| 全量跑 LLM-as-judge 当门禁 | 每个 run 成本翻倍、延迟爆炸 | 采样 + `WARN` |
| 用 check 做重计算（全量召回评估） | run 时间不可控 | 独立巡检 job / 夜间跑 |
| 软质量一上来就 blocking | 误报卡死正常发布 | 先 `WARN`，看分布再收紧 |
| 门禁 check 存在"不产出结果"的分支 | 门禁静默失效（源码：无结果则下游照常执行） | 保证所有分支都 `yield` |
| 给分区资产写 `partitions_def` 的外挂 check | preview 警告 + 全分区读取报错 | 用内联 check 或 `additional_deps` |
| check 里绕过平台 Resource 直连对象存储 | 凭证扩散、CI 卡 | `required_resource_keys` 拿平台 Resource |
| check 结果不带 `rule_id` / 阈值 / 样本 | 告警无法归因，支持成本高 | 统一 metadata 契约 |
| 把 check 结果当业务状态 | UI 直接暴露 Dagster 术语 | BFF 映射成 §9.6 的领域状态 |

## 19.9 落地路线与验收

| 阶段 | 动作 | 验收 |
| --- | --- | --- |
| 一（1 周） | A 组硬契约：`extracted_knowledge` 的 schema/枚举/图内一致/守恒 + `vector_index` 的维度与非空 | `dagster definitions validate -m knowledge_extraction.defs` 通过；故意构造坏数据能拦住下游 |
| 二（2~3 周） | 覆盖率类争议阈值全部以 `WARN` 上线：解析覆盖率、chunk 覆盖、空抽取率；加 freshness 与成本 bounds | UI/看板能看到 `observed` 分布；跑满一个批次窗口 |
| 三（持续） | 收紧 2~3 条为 `blocking`；接 BFF 的 `PUBLISHED` 门禁；上采样 judge 与召回冒烟 | 发布门禁生效；check 失败可一键归因到 `rule_id` |

**CI 必跑**（`docs/09` §9.9 的交付流程）：

```powershell
python -m dagster definitions validate -m knowledge_extraction.defs
python -m pytest -q
```

单测里用 `dg.materialize([...], raise_on_error=False)` + `result.get_asset_check_evaluations()` 把 check 当断言（`docs/06` §6.4、`docs/07`）。

## 19.10 本文 API 验证记录（dagster 1.13.22 / 本机实测）

| 形态 | 结论 |
| --- | --- |
| `@dg.asset(check_specs=[...])` + 资产内 `yield dg.AssetCheckResult(check_name=...)` | ✅ 通过；blocking check 失败时资产 step fail，下游 step 被跳过 |
| `dg.AssetCheckSpec(name=..., asset=...)` | ✅ `asset` 为必填关键字参数，省略报 `TypeError` |
| `AssetCheckSpec` 字段 | `name / asset_key / description / additional_deps / blocking / metadata / automation_condition / partitions_def`（`partitions_def` 会触发 `PreviewWarning`） |
| `@dg.asset_check(additional_ins=..., required_resource_keys=...)` | ✅ 通过，可同时注入上游数据与平台 Resource |
| 仅依赖 `context` 的 check（无数据输入） | ✅ 通过 |
| `dg.AssetCheckResult` 字段 | `passed / asset_key / check_name / metadata / severity / description` |
| `AutomationCondition.all_checks_match(check_passed(), blocking_only=True)` 及 `eager() & ...` | ✅ 定义可解析（`Definitions.resolve_asset_graph()`） |
| `dg.AutomationCondition.all_checks_pass` | ❌ 不存在（用 `all_checks_match` / `any_checks_match`） |
| `build_last_update_freshness_checks` / `build_time_partition_freshness_checks` / `build_column_schema_change_checks` / `build_metadata_bounds_checks` / `build_sensor_for_freshness_checks` | ✅ 均存在于 1.13.22 |

相关章节：`docs/06`（语法与坑）、`docs/07`（测试）、`docs/09`（门禁与权责）、`docs/13`（抽取契约）、`docs/18`（版本与 metadata）。