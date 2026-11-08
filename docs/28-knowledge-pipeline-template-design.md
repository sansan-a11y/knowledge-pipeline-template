# 28 · 知識抽取 Pipeline Template 調研與設計

> 2026-11-06 更新：正式 v1 的固定 stage 已調整為
> `ingest -> parse -> extraction -> assemble -> validation -> publish`。
> 本文的方案比較、狀態投影、錯誤處理與落地原則仍然有效；最新階段契約以 `docs/30` 為準。

> 基線版本：Dagster `1.13.22`、Python 3.11。
> 調研日期：2026-11-03。
> 本文回答三個問題：
> 1. 固定 stage 應該由 Dagster 的哪一層表達？
> 2. 自研前端如何穩定獲得 stage 級執行狀態？
> 3. Pipeline 開發者接入時，Template、平台 SDK、BFF 各自提供什麼？

## 28.0 結論先行

建議採用 **「固定 Stage 邊界 + 自由內部 Asset」** 的混合方案：

- 平台固定六個知識抽取 stage：`INGEST -> PARSE -> EXTRACT -> CURATE -> ASSEMBLE -> PUBLISH`。
- 每個 stage 只有一個可被前端依賴的 **terminal asset**；stage 內部可以再拆任意多個 asset。
- 跨 stage 的依賴只能接上一個 stage 的 terminal asset，不能直接接內部 asset。
- Stage 完成狀態由「terminal asset 在本次 run 的 materialization + blocking check」判定，不以 run 狀態直接代替。
- Stage 開始與失敗由 template 發出的 `AssetObservation` 補足；Dagster 原生的 run/asset/check 事件仍是唯一真相源。
- BFF 將 Dagster 事件投影成穩定的領域狀態 API；前端只認固定的 `stage_id` 與狀態枚舉，不直接理解 asset、run、partition。
- 新 Pipeline 不改 BFF、不改前端、不改平台實例配置，只提供一份符合契約的 `Definitions` 與 deployment values。

一句話概括：

> Template 約束的是「可觀察的階段邊界」，不是開發者的內部實作方式。

## 28.1 問題拆解

目前 Pipeline 團隊接入 Dagster 的成本主要不在寫業務函式，而在以下幾件事：

| 成本 | 典型症狀 | Template 應負責什麼 |
| --- | --- | --- |
| 心智模型 | 分不清 Asset、Job、Run、Partition、Check | 提供固定骨架與最小可運行範例 |
| 接入樣板 | 每次重寫 Definitions、resources、job、tests | 由 scaffold 生成 |
| 契約漂移 | asset 命名、metadata、partition 各寫各的 | CI 自動驗證 |
| 狀態語義 | run 成功不代表知識已可發布 | 固定 stage 與發布門禁 |
| 前端耦合 | 前端直接依賴 asset key 或 GraphQL 欄位 | BFF 只暴露領域狀態 |
| 本地調試 | 必須連 MinIO、LLM、向量庫 | 提供 fake resources 與 smoke test |
| 上線流程 | 不知道要改哪些 Helm/workspace 檔案 | deployment template 與註冊清單 |

因此 Template 不應只是一份範例程式，而應包含四層交付物：

1. **專案骨架**：讓新的 code location 可直接啟動。
2. **契約 SDK**：固定 stage、metadata、錯誤與 runtime 訊號。
3. **驗證工具**：本地測試與 CI 契約檢查。
4. **部署與升級規範**：鏡像、Helm values、版本相容性。

## 28.2 Dagster 能力調研

本節只使用本機安裝的 Dagster 1.13.22 原始碼與 1.13.26 GraphQL schema 核對過的機制。

### 28.2.1 Dagster 已經提供的能力

| 能力 | Dagster 原語 | 對本設計的用途 |
| --- | --- | --- |
| 產物血緣 | Asset dependency graph | 表達 stage 內與 stage 間依賴 |
| 精準重跑 | Partition、Backfill | 單文件、單版本重跑 |
| 資料品質 | Asset Check | Stage gate 與發布門禁 |
| 版本判定 | `code_version`、`data_version`、stale status | 判斷舊結果能否復用 |
| 執行歷史 | Run、Step event、Materialization | 狀態與排障的真相源 |
| 併發治理 | Pool、run concurrency、tag concurrency | 保護 LLM、VLM、向量庫 |
| 外部程式接入 | Dagster Pipes | 保留既有 CLI/container pipeline |
| 前端查詢 | GraphQL | BFF 查 asset、run、materialization、check、stale |

### 28.2.2 Dagster 沒有直接提供的能力

| 缺口 | 影響 | 自研落點 |
| --- | --- | --- |
| 業務 stage 概念 | Asset 可以任意拆分，沒有固定的「使用者可見階段」 | Stage contract + terminal asset |
| 可靠的 stage start 事件 | Materialization 只代表完成，不表示開始 | `AssetObservation` progress signal |
| 業務任務狀態 | Run 狀態不知道「文件是否已發布」 | BFF task/stage projection |
| 事件可靠推送 | Sensor 可能漏 tick | 回推 + 定時對賬 |
| 前端領域 API | GraphQL 充滿 Dagster 名詞 | BFF API |
| Template 升級 | 複製貼上後會分叉 | Copier 或內部 scaffold CLI |

### 28.2.3 關鍵 API 事實

| 事實 | 設計含意 |
| --- | --- |
| `Definitions.metadata` 存在，但不顯示在 UI，也不適合作為前端 catalog | 固定 stage metadata 要掛在 asset 上 |
| `AssetObservation` 可帶 `asset_key`、`partition`、`metadata`、`tags` | 可在不產生 materialization 的前提下回報開始/失敗 |
| `ObservationEvent` 出現在 run 的 `eventConnection` | BFF 可按 run 增量讀取 stage signal |
| Asset Check 可 `blocking=True` | 可阻止臟資料流向下游 |
| `WARN` check 不影響 run 狀態 | UI 要區分阻斷錯誤與品質警告 |
| `Run` 有 `stepStats`、`assetMaterializations`、`assetChecks` | 可重建一次 run 的階段進度 |
| GraphQL 沒有 run 狀態訂閱 | 想即時推送需 `run_status_sensor` 或 BFF polling |
| OSS Dagster 無完整用戶/RBAC | 前端與 GraphQL 的存取邊界必須在 BFF |

## 28.3 方案比較

| 方案 | 做法 | 優點 | 缺點 | 結論 |
| --- | --- | --- | --- | --- |
| A. 前端直接讀 asset graph | 任意 asset 被當成 stage | 最快做出來 | Asset 改名就破壞 UI；內部節點太多；狀態語義不穩 | 否 |
| B. 固定 asset 名稱鏈 | 平台強制六個 asset | 契約最簡單 | 無法按網路/CPU/失敗模式拆資產；開發者容易繞過 | 否 |
| C. 固定 stage 邊界 + 自由內部 asset | 每 stage 一個 terminal，內部可拆 | 前端穩定，內部自由，兼顧血緣 | 需要一條契約驗證 | **推薦** |
| D. Stage 全部用 op/graph | 用 op 表示流程 | 看似接近工作流 | 丟失資料產物語義、partition/check/stale 能力 | 否 |
| E. 自建 stage 狀態表，不依賴 Dagster 事件 | 每個 asset 自己寫業務庫 | UI 完全自由 | 與 Dagster 狀態漂移；補償成本高 | 僅作投影緩存 |
| F. 每個 stage 一個 run | Stage 間用 sensor 串 | 失敗隔離強 | run 數與事件成本高；跨 run 狀態更複雜 | 非必要不採用 |

方案的關鍵分界是：

- **Stage 是產品契約**，必須穩定。
- **Asset 是工程實作**，需要保留彈性。
- **Run 是執行實例**，不應直接顯示給業務使用者。

## 28.4 固定 Stage 設計

### 28.4.1 Stage 枚舉

平台固定以下六個 stage。Pipeline 可以標記某 stage 為 optional，但不可自創 stage id。

| 順序 | `stage_id` | 前端名稱建議 | 責任 | 典型 terminal asset |
| ---: | --- | --- | --- | --- |
| 1 | `INGEST` | 取得文件 | 驗證來源、版本、可讀性，取得原件或登記外部文件 | `raw_document` |
| 2 | `PARSE` | 解析內容 | 頁面解析、OCR、版面辨識、分類、清洗、切塊 | `document_chunks` |
| 3 | `EXTRACT` | 抽取知識 | 呼叫 LLM/VLM 抽取實體、關係、片段 | `extracted_knowledge` |
| 4 | `CURATE` | 整理知識 | 去重、合併、實體對齊、衝突處理、引用校驗 | `curated_knowledge` |
| 5 | `ASSEMBLE` | 組裝文件 | 形成知識文件、圖譜投影、主題摘要 | `knowledge_document` |
| 6 | `PUBLISH` | 發布索引 | 寫入向量庫/圖庫、建立可檢索版本 | `published_document` |

約束：

- `stage_id` 使用固定大寫枚舉，大小寫與拼寫不可變。
- 顯示名稱與翻譯由前端維護，Pipeline 不傳 UI 文案。
- `CLASSIFY` 若需要，放在 `PARSE` 內；索引細節放在 `PUBLISH` 內。
- 若某條 Pipeline 不需要 `CURATE`，在契約中標記 optional 並讓 BFF 回傳 `NOT_APPLICABLE`，不要用空 asset 假裝執行。

### 28.4.2 Stage 邊界規則

```text
INGEST terminal
      |
      v
PARSE terminal        <-- PARSE 內部可以有 page_image / page_classification / chunks
      |
      v
EXTRACT terminal      <-- EXTRACT 內部可以有 page_extract / chunk_extract
      |
      v
CURATE terminal
      |
      v
ASSEMBLE terminal
      |
      v
PUBLISH terminal
```

契約規則：

1. 每個 enabled stage 恰好一個 terminal asset。
2. terminal asset 必須是 materializable asset，不能只是沒有實作的 `AssetSpec`。
3. 跨 stage 的 consumer 只能依賴上一個或多個上游 stage 的 terminal asset。
4. Stage 內部 asset 可以任意 fan-out/fan-in，但不能直接被其他 stage 當接口。
5. Stage 內所有 asset 都放在 canonical group，例如 `km__manual_extraction__extract`。
6. 所有 stage asset 使用同一套 partition definition。
7. Stage 間傳遞持久化 handle/URI 與 metadata，不依賴只在單一 Pod 記憶體中存在的物件。
8. 每個 terminal asset 至少有一條 blocking check，或由平台明確審批 waiver。

這樣做的效果：

- 前端看到六個穩定階段，不需要理解內部 asset graph。
- 開發者仍可依「網路密集 / CPU 密集 / GPU 密集」拆分內部 asset。
- Stage 失敗後，可以只重跑該 stage 及其內部 asset，不必重跑整條鏈。

## 28.5 定義期契約

### 28.5.1 Pipeline identity

每條 Pipeline 定義以下穩定欄位：

| Metadata key | 範例 | 用途 |
| --- | --- | --- |
| `km_contract_version` | `1` | Template/contract schema 版本 |
| `km_pipeline_id` | `manual_extraction` | 跨版本穩定的 Pipeline id |
| `km_pipeline_version` | `2026.11.1` | 發佈版本或 release tag |
| `km_stage_id` | `EXTRACT` | 固定 stage id |
| `km_stage_role` | `terminal` | 只有 terminal 進前端階段狀態 |
| `owner` | `team:knowledge` | 告警與審批路由 |

`km_pipeline_id` 不要使用 Git repo 名或 Kubernetes release 名，這兩個都可能變。Pipeline 團隊要把它視為 API。

### 28.5.2 Definition metadata

建議的 helper：

```python
from dataclasses import dataclass
from enum import StrEnum

import dagster as dg


class Stage(StrEnum):
    INGEST = "INGEST"
    PARSE = "PARSE"
    EXTRACT = "EXTRACT"
    CURATE = "CURATE"
    ASSEMBLE = "ASSEMBLE"
    PUBLISH = "PUBLISH"


@dataclass(frozen=True)
class StageSpec:
    stage: Stage
    terminal_asset_key: dg.AssetKey
    required: bool = True


def stage_group(pipeline_id: str, stage: Stage) -> str:
    return f"km__{pipeline_id}__{stage.value.lower()}"


def terminal_metadata(
    *,
    pipeline_id: str,
    stage: Stage,
    pipeline_version: str,
    owner: str,
    contract_version: int = 1,
) -> dict[str, object]:
    return {
        "km_contract_version": contract_version,
        "km_pipeline_id": pipeline_id,
        "km_pipeline_version": pipeline_version,
        "km_stage_id": stage.value,
        "km_stage_role": "terminal",
        "owner": owner,
    }
```

```python
PIPELINE_ID = "manual_extraction"
PIPELINE_VERSION = "2026.11.1"
OWNER = "team:knowledge"

INGEST = dg.AssetKey(["manual_extraction", "raw_document"])
PARSE = dg.AssetKey(["manual_extraction", "document_chunks"])
EXTRACT = dg.AssetKey(["manual_extraction", "extracted_knowledge"])
CURATE = dg.AssetKey(["manual_extraction", "curated_knowledge"])
ASSEMBLE = dg.AssetKey(["manual_extraction", "knowledge_document"])
PUBLISH = dg.AssetKey(["manual_extraction", "published_document"])

STAGES = [
    StageSpec(Stage.INGEST, INGEST),
    StageSpec(Stage.PARSE, PARSE),
    StageSpec(Stage.EXTRACT, EXTRACT),
    StageSpec(Stage.CURATE, CURATE),
    StageSpec(Stage.ASSEMBLE, ASSEMBLE),
    StageSpec(Stage.PUBLISH, PUBLISH),
]

STAGE_GROUPS = [stage_group(PIPELINE_ID, spec.stage) for spec in STAGES]
```

`Definitions.metadata` 可以保留一份完整 manifest 供 Python 端測試，但前端 catalog **只認 asset metadata**，避免依賴一個 GraphQL 沒有穩定暴露面的欄位。

### 28.5.3 Runtime metadata

每次 terminal materialization 至少要自動寫入：

| Metadata key | 來源 | 用途 |
| --- | --- | --- |
| `km_contract_version` | template | UI/事件格式相容性 |
| `km_pipeline_id` | template | Pipeline 識別 |
| `km_stage_id` | template | Stage 識別 |
| `doc_id` | partition/task | 文件識別 |
| `doc_version` | partition/task | 文件版本 |
| `pipeline_version` | 發佈版本 | 產物追溯 |
| `parser_version` / `model_version` / `prompt_version` | stage 實作 | 品質歸因 |
| `items_total` / `items_done` | stage 實作 | Stage 內進度 |
| `cost` / `tokens_in` / `tokens_out` | stage 實作 | 成本治理 |

不要把完整文本、嵌入向量、Prompt、大段 JSON 或憑證寫入 metadata。Event log 是控制面資料，不是物件儲存。

## 28.6 Runtime stage signal

### 28.6.1 為什麼需要額外訊號

Materialization 只能證明 stage 已完成，不能可靠回答以下問題：

- Stage 何時開始？
- 失敗發生在哪個 stage？
- Stage 正在重試第幾次？
- 使用者看到的是「排隊」「執行」還是「卡住」？

因此 Template 需要一個非常薄的 runtime helper，在 stage 開始與失敗時發出 `AssetObservation`。

### 28.6.2 Stage context manager

```python
import time
from contextlib import contextmanager

import dagster as dg


@contextmanager
def stage_execution(
    context: dg.AssetExecutionContext,
    *,
    pipeline_id: str,
    stage: Stage,
    attempt: int = 1,
):
    started = time.monotonic()
    common = {
        "km_signal_version": 1,
        "km_pipeline_id": pipeline_id,
        "km_stage_id": stage.value,
        "km_stage_attempt": attempt,
    }
    context.log_event(
        dg.AssetObservation(
            asset_key=context.asset_key,
            partition=context.partition_key,
            metadata={**common, "km_stage_state": "STARTED"},
            description=f"{stage.value} started",
        )
    )

    try:
        yield
    except Exception as exc:
        error_metadata = getattr(exc, "metadata", {}) or {}
        context.log_event(
            dg.AssetObservation(
                asset_key=context.asset_key,
                partition=context.partition_key,
                metadata={
                    **common,
                    "km_stage_state": "FAILED",
                    "error_class": type(exc).__name__,
                    "error_code": getattr(
                        exc,
                        "error_code",
                        error_metadata.get("error_code", "E_BUG"),
                    ),
                    "duration_ms": int((time.monotonic() - started) * 1000),
                },
                description=f"{stage.value} failed",
            )
        )
        raise
```

使用方式：

```python
@dg.asset(
    key=EXTRACT,
    partitions_def=DOC_PARTITIONS,
    group_name=stage_group(PIPELINE_ID, Stage.EXTRACT),
    metadata=terminal_metadata(
        pipeline_id=PIPELINE_ID,
        stage=Stage.EXTRACT,
        pipeline_version=PIPELINE_VERSION,
        owner=OWNER,
    ),
    check_specs=[
        dg.AssetCheckSpec(
            name="extraction_contract",
            asset=EXTRACT,
            blocking=True,
        )
    ],
)
def extracted_knowledge(context: dg.AssetExecutionContext, llm, object_store):
    with stage_execution(context, pipeline_id=PIPELINE_ID, stage=Stage.EXTRACT):
        result = extract(context.partition_key, llm, object_store)
        context.add_asset_metadata({
            "doc_id": result.doc_id,
            "doc_version": result.doc_version,
            "model_version": result.model_version,
            "prompt_version": result.prompt_version,
        })
        yield dg.MaterializeResult(...)
        yield dg.AssetCheckResult(
            check_name="extraction_contract",
            passed=result.contract_valid,
        )
```

注意事項：

- `AssetObservation` 只用於控制面進度，不用來代表資料已完成。
- 每次 step retry 都應帶新的 `km_stage_attempt`。
- 中間 retry 的 `FAILED` signal 不應立刻讓業務 UI 進入最終 `FAILED`；BFF 要結合 run 是否仍在重試。
- Helper 本身不能吞掉異常，原始 `dg.Failure` 與 stack trace 必須保留。
- 發生 start/observation 寫入失敗時，不得掩蓋原本的業務失敗。

### 28.6.3 進度更新

長 stage 可選擇發送 milestone progress，而不是每個 item 都發：

```python
context.log_event(
    dg.AssetObservation(
        asset_key=context.asset_key,
        partition=context.partition_key,
        metadata={
            "km_stage_state": "PROGRESS",
            "km_stage_id": Stage.EXTRACT.value,
            "items_total": total,
            "items_done": done,
        },
    )
)
```

建議節流為每 10 秒或每 5% 一次，避免 event log 膨脹。MVP 可以只做 `STARTED`、`FAILED` 與 materialization。

## 28.7 狀態機與映射

### 28.7.1 前端狀態枚舉

Stage state：

| State | 含義 |
| --- | --- |
| `NOT_APPLICABLE` | 該 Pipeline 沒有啟用此 stage |
| `PENDING` | 尚未排隊 |
| `QUEUED` | 已建立 run/backfill，等待執行 |
| `RUNNING` | 正在執行或重試 |
| `SUCCEEDED` | 本次 run 已完成 terminal materialization，必要 check 全綠 |
| `FAILED` | 本次 run 最終失敗，或 blocking check 阻斷 |
| `BLOCKED` | 上游 stage 失敗，本 stage 未執行 |
| `STALE` | 已有結果，但因 code/data/upstream 變更已過期 |
| `SKIPPED` | 明確的業務條件跳過 |
| `CANCELED` | run 或任務被取消 |

Task state：

| State | 含義 |
| --- | --- |
| `CREATED` | 已登記，尚未排隊 |
| `QUEUED` | 已提交 Dagster |
| `RUNNING` | 至少一個 stage 在執行 |
| `SUCCEEDED` | `PUBLISH` succeeded 且發布門禁通過 |
| `PARTIAL` | 批次部分成功，單文件任務通常不用 |
| `FAILED` | 無後續重試且未發布 |
| `CANCELED` | 使用者或平台取消 |

### 28.7.2 投影優先級

BFF 對每個 stage 套用以下優先級，後面的規則不能覆蓋前面的終態：

1. 任務已取消：`CANCELED`
2. 本次 run 最終失敗且失敗 signal/step 屬於該 stage：`FAILED`
3. 上游已有 `FAILED/BLOCKED/CANCELED` 且本 stage 尚無執行：`BLOCKED`
4. 本次 run 中有 start signal 且尚無 terminal materialization：`RUNNING`
5. 本次 run 已有 terminal materialization 且 blocking checks 通過：`SUCCEEDED`
6. 本次 run 已有 terminal materialization 但有 blocking check 失敗：`FAILED`
7. 已有歷史結果但 stale：`STALE`
8. run 已排隊、asset materialization planned 或 backfill created：`QUEUED`
9. 其餘：`PENDING`

核心規則：

- **只認本次 business task 對應的 run**。不能用 asset 的最新全域 materialization 推斷當前任務成功，否則同一 partition 重跑時會把舊成功誤認成新成功。
- **Run success 不等於 PUBLISHED**。`PUBLISH` stage 還必須完成 terminal materialization 與 blocking checks。
- **WARN check 只進品質摘要**，不把 stage 改成 `FAILED`。
- **重試中仍是 `RUNNING`**，可在 UI 顯示「第 N 次嘗試」。

### 28.7.3 BFF 投影資料

建議保留可重建的兩張 projection table：

```text
km_task
  task_id
  pipeline_id
  partition_key
  tenant
  status
  current_stage
  dagster_run_id
  created_at
  updated_at

km_task_stage
  task_id
  stage_id
  status
  attempt_no
  asset_key
  run_id
  started_at
  finished_at
  error_code
  message
  progress_json
  last_event_cursor
```

唯一鍵建議為 `(task_id, stage_id, attempt_no, run_id)`。所有更新用 idempotent upsert，確保 sensor 回推與對賬 job 重放時結果一致。

Projection 是緩存，不是第二份真相：

- Dagster 的 run、asset event、check evaluation 是真相源。
- 業務 API 回傳 projection，避免每次請求壓 Dagster GraphQL。
- 定時對賬可從 Dagster 重建 task/stage 狀態。

## 28.8 前端 API 契約

前端只需要以下固定結構：

```json
{
  "task_id": "task_01J...",
  "pipeline_id": "manual_extraction",
  "status": "RUNNING",
  "current_stage": "EXTRACT",
  "stages": [
    {
      "stage_id": "INGEST",
      "state": "SUCCEEDED",
      "attempt": 1,
      "started_at": "2026-11-03T08:10:01Z",
      "finished_at": "2026-11-03T08:10:05Z",
      "progress": null,
      "message": null
    },
    {
      "stage_id": "PARSE",
      "state": "SUCCEEDED",
      "attempt": 1,
      "started_at": "2026-11-03T08:10:06Z",
      "finished_at": "2026-11-03T08:11:22Z",
      "progress": {"items_done": 38, "items_total": 38},
      "message": null
    },
    {
      "stage_id": "EXTRACT",
      "state": "RUNNING",
      "attempt": 2,
      "started_at": "2026-11-03T08:11:31Z",
      "finished_at": null,
      "progress": {"items_done": 17, "items_total": 38},
      "message": null
    },
    {
      "stage_id": "CURATE",
      "state": "PENDING",
      "attempt": null,
      "started_at": null,
      "finished_at": null,
      "progress": null,
      "message": null
    }
  ],
  "quality": {
    "blocking_failures": 0,
    "warnings": 1
  }
}
```

API 規則：

- `stages` 永遠按固定順序返回，即使某些 stage `NOT_APPLICABLE`。
- `stage_id`、`state` 是穩定 machine-readable enum。
- 一般業務 UI 不需要看到 asset key、run id、step key、partition key。
- 技術詳情另提供 `/internal` 或開發者頁面，不要把 Dagster 名詞塞進主流程。
- 錯誤回傳使用穩定 `error_code`、`retryable`、`user_message`、`runbook_url`，不要讓前端解析 stack trace。

## 28.9 BFF 狀態來源

建議由 `platform_core` 與 BFF 配合：

```text
Backfill / Run
      |
      +--> run_status_sensor --> callback --> BFF upsert task/stage
      |
      +--> BFF incremental poll --> run.eventConnection
                                   |- AssetObservation: STARTED/FAILED/PROGRESS
                                   |- Materialization: stage completed
                                   |- AssetCheckEvaluation: gate result
                                   +- RunFailure/RunSuccess: final attempt state

BFF reconciliation job
      |
      +--> query recent non-terminal tasks
      +--> query Dagster GraphQL
      +--> rebuild missing or mismatched stage rows
```

建議的查詢對象：

| 目的 | GraphQL 面 |
| --- | --- |
| Catalog | `assetNodes` 的 `assetKey`、`metadataEntries`、`assetChecksOrError` |
| 本次 run | `runOrError` 的 `status`、`tags`、`assetMaterializations`、`assetChecks`、`eventConnection` |
| Stage 開始/失敗/進度 | `eventConnection` 的 `ObservationEvent` |
| Stage 完成 | `MaterializationEvent` 的 `assetKey`、`partition`、`runId`、metadata |
| Stage 過期 | `assetNode.staleStatus(partition:)` |
| 任務查找 | run tags `task_id`、`pipeline_id`、`doc_id`、`doc_version` |

不建議 BFF 直接讀 Dagster 的 Postgres 內部表。表結構不是公開 API，升級時會造成事故。

## 28.10 Template 專案骨架

建議資料夾：

```text
pipelines/_template/
├── pyproject.toml
├── Dockerfile
├── README.md
├── copier.yaml
├── src/
│   └── pipeline_name/
│       ├── __init__.py
│       ├── defs.py                 # Definitions wiring only
│       ├── contract.py             # pipeline id、StageSpec、terminal keys
│       ├── settings.py             # 非敏感設定
│       ├── assets/
│       │   ├── ingest.py
│       │   ├── parse.py
│       │   ├── extract.py
│       │   ├── curate.py
│       │   ├── assemble.py
│       │   └── publish.py
│       └── checks/
│           ├── contracts.py
│           └── quality.py
├── tests/
│   ├── test_contract.py
│   ├── test_fake_pipeline.py
│   └── fixtures/
├── deployment/
│   └── values.yaml
└── scripts/
    ├── dev.ps1
    ├── test.ps1
    └── validate.ps1
```

### 28.10.1 `defs.py` 只做 wiring

```python
import dagster as dg

from pipeline_name.assets import ALL_ASSETS
from pipeline_name.checks import ALL_CHECKS
from pipeline_name.contract import PIPELINE_ID, STAGE_GROUPS
from pipeline_name.resources import RESOURCES

pipeline_job = dg.define_asset_job(
    name=f"{PIPELINE_ID}__full",
    selection=dg.AssetSelection.groups(*STAGE_GROUPS),
)

defs = dg.Definitions(
    assets=ALL_ASSETS,
    asset_checks=ALL_CHECKS,
    jobs=[pipeline_job],
    resources=RESOURCES,
    metadata={
        "km_pipeline_id": PIPELINE_ID,
        "km_contract_version": 1,
    },
)
```

`defs.py` 不放：

- 業務演算法。
- 動態組裝大量 asset 的魔法邏輯。
- 基礎設施連線與憑證。
- 每個團隊各自的例外處理框架。

### 28.10.2 單 stage 重跑

Template 可自動生成每個 stage 的 repair job：

```python
stage_jobs = [
    dg.define_asset_job(
        name=f"{PIPELINE_ID}__repair_{spec.stage.value.lower()}",
        selection=dg.AssetSelection.groups(
            stage_group(PIPELINE_ID, spec.stage)
        ),
    )
    for spec in STAGES
]
```

用 canonical group 選取的好處是：即使 `EXTRACT` 內部有 `page_extract`、`chunk_extract` 等多個 asset，一次修復仍會重算整個 `EXTRACT`，但不會誤跑上游的 `PARSE` 或其他 stage。前提是所有 stage 邊界都傳遞持久化 handle/URI 與 metadata，而不是只在同一 run 記憶體有效的 Python 物件。這是 Template 必須幫開發者守住的設計約束。

### 28.10.3 標準入口命令

```powershell
./scripts/dev.ps1        # fake resources + dagster dev
./scripts/test.ps1       # unit + smoke tests
./scripts/validate.ps1   # definitions validate + contract checks
```

CI 至少執行：

```powershell
python -m dagster definitions validate -m pipeline_name.defs
python -m pytest -q
python -m pipeline_template.validate --module pipeline_name.defs
```

## 28.11 契約檢查清單

### 28.11.1 Definition 載入

- `Definitions` 能被 Dagster 載入。
- Asset key 不重複、不非法。
- Job selection 能解析。
- 所有 stage terminal asset 可執行。

### 28.11.2 Stage 契約

- 只使用平台允許的 `Stage` 枚舉。
- 每個 enabled stage 恰好一個 terminal asset。
- stage 沒有自創或跳過必要順序。
- stage 內所有 asset 都在 canonical group。
- 所有 stage asset 使用相同 partition definition。
- 跨 stage dependency 只接到 terminal asset。
- optional stage 的 `NOT_APPLICABLE` 與 manifest 一致。

### 28.11.3 Metadata 契約

- definition metadata 有 `km_pipeline_id`、`km_stage_id`、`km_stage_role`。
- materialization metadata 有 `doc_id`、`doc_version`、`pipeline_version`。
- 版本、model、prompt、parser metadata 由對應 stage 負責。
- 不得包含憑證、完整文件、向量或超大 payload。

### 28.11.4 品質門禁

- 每個 terminal asset 至少一條 check 或核准 waiver。
- `PUBLISH` 至少有一條 blocking check。
- Check 名稱在同一 asset 內唯一。
- Check result 固定帶 `rule_id`、`threshold`、`observed`。
- Blocking check 失敗時，下游 asset step 不得執行。

### 28.11.5 安全與基礎設施

- 禁止 `boto3.client(...)`、`psycopg2.connect(...)` 等繞過 Resource 的連線。
- 禁止硬編碼 bucket、endpoint、token。
- `os.environ[...]` 只能出現在平台 Resource implementation。
- Docker tag 不可為 `latest`。
- Dagster 版本與平台 base image 一致。

### 28.11.6 Runtime smoke test

- 最小 fake fixture 能跑完六個 stage。
- 每個 stage 都能產生 start observation。
- 故意注入 blocking check failure，後續 stage 被阻擋。
- 單 stage repair 能從已存在的上游產物恢復。
- 同一 partition 重跑不會把舊結果誤判成新成功。

## 28.12 錯誤、重試與冪等

Template 應直接接上 `docs/20` 的錯誤規範：

- 已知不可恢復錯誤用 `dg.Failure(..., allow_retries=False)`。
- 暫時性錯誤才用 `RetryPolicy`。
- Stage start/failure observation 不取代 `dg.Failure`。
- 重試中的失敗只顯示為 running/retrying，最終失敗才進入 `FAILED`。
- 重跑必須覆寫同一分區，或使用明確的新版本分區。
- Stage 寫入必須原子；不要先刪舊結果再計算。
- LLM 抽取要有 chunk-level checkpoint 或 sentinel，避免整份文件重跑重複燒 token。

建議 Pipeline 開發者只處理兩類錯誤入口：

```python
raise_pipeline_failure(DATA_CONTRACT, "抽取結果 schema 不符")
raise_pipeline_failure(TRANSIENT_IO, "物件儲存暫時不可用")
```

不要把 `except Exception` 散落在每個 asset 裡。未知錯誤保留原始 stack trace，平台統一映射為 `E_BUG`。

## 28.13 舊 Pipeline 的接入方式

| 既有形態 | 接入方式 | 注意 |
| --- | --- | --- |
| Python function / class | 包成 stage asset，內部拆分 helper | 不重寫演算法 |
| CLI / shell script | `PipesSubprocessClient` 或 `open_pipes_session` | 回傳 materialization、check、log |
| Docker image / 外部 job | Pipes + 平台 launcher；stage asset 等待結果 | 設 timeout、輸出 handle、錯誤碼 |
| Airflow / 既有 scheduler | 可先保留上游調度，Dagster 只編排下游；或逐步遷移 | 不雙重觸發同一工作 |
| 已有大型 Dagster project | 先加 stage metadata 與 terminal asset，不一次重構全部 asset | 逐步收斂契約 |

核心原則是：

> 保留既有領域程式，新增的是 Dagster 執行邊界、資源接線、品質門禁與狀態契約。

## 28.14 Template 版本與升級

### 28.14.1 三種版本分開管理

| 版本 | 範例 | 變更含意 |
| --- | --- | --- |
| Contract schema | `km_contract_version=1` | Stage/API 欄位破壞性變更 |
| Template | `template==2.3.0` | 骨架、測試、CI、Helm 改進 |
| Platform SDK | `platform-sdk==1.4.0` | Resource、helper、錯誤映射 |

### 28.14.2 升級方式

- 優先使用 Copier，支援 `copier update` 與模板 migration。
- 不建議只用一次性 cookiecutter，否則 Template 一改就產生大量分叉。
- Contract schema major 變更要提供雙寫期與 BFF 兼容期。
- 每次 Template release 要有 changelog、相容版本矩陣與一個 migration test。

## 28.15 分階段落地

### 階段 0：凍結契約（2-3 天）

交付：

- `Stage` 枚舉與狀態枚舉。
- `km_*` metadata key 清單。
- 前端 task/stage response schema。
- 一頁式 terminal asset 規則。

驗收：

- 平台、Pipeline、前端三方對 stage 語義沒有歧義。
- 固定六個 stage，optional stage 的顯示方式已定。

### 階段 1：最小 Template（1 週）

交付：

- 可跑的六 stage fake pipeline。
- `Definitions`、job、fake resources、unit test。
- Contract validator 與 CI example。
- 一份 deployment values。

驗收：

- 新開發者只改業務 asset 與設定即可在本機跑完整鏈。
- `definitions validate`、pytest、contract check 全綠。

### 階段 2：狀態投影（1-2 週）

交付：

- Stage start/fail `AssetObservation` helper。
- BFF task/stage projection。
- blocking check 與 PUBLISH gate。
- run status 回推與對賬 job。

驗收：

- 前端可穩定顯示六個 stage。
- 重試、blocking failure、取消、stale 都有正確狀態。
- 同一任務重跑不會被前一版結果誤導。

### 階段 3：產品化（持續）

交付：

- Copier 升級流程。
- 自助 scaffold、範例庫、開發者文件。
- 接入耗時、失敗率、重跑成本與採納率指標。

驗收：

- 第二個 Pipeline 團隊不需要平台修改 BFF 或前端。
- Template 升級能在 1 天內套用到試點 Pipeline。

## 28.16 驗收指標

| 指標 | 定義 | 建議初期目標 |
| --- | --- | --- |
| 新 Pipeline 接入時間 | scaffold 到 staging 跑通 | 從週降到 1 個工作日內 |
| 契約通過率 | Pipeline PR 首次 contract check 通過比例 | 先量 baseline，再逐步提高 |
| Stage 狀態覆蓋率 | 任務中可被明確投影的 stage 比例 | 100% |
| 發布門禁覆蓋率 | PUBLISH 有 blocking gate 的 Pipeline 比例 | 100% |
| 首次故障定位時間 | 從開啟任務到看到 stage/error_code | 5 分鐘內 |
| 不必要全量重跑比例 | 可單 stage 修復但仍全鏈重跑的比例 | 持續下降 |
| Template 採用率 | 新 Pipeline 從 Template 建立的比例 | 第一階段 100% |

## 28.17 反對意見與回應

### 「固定 stage 會不會限制 Pipeline 彈性？」

限制的是跨團隊可見的介面，不是內部實作。Stage 內仍可任意拆 asset、fan-out、fan-in、並行與 Pipes 外部程式。

### 「為什麼不讓前端直接讀 Asset Graph？」

因為 Asset Graph 是最佳工程診斷視圖，不是穩定產品 API。Asset 名稱、內部節點與 GraphQL 欄位都會隨實作演進，會把前端鎖死在 Pipeline 細節上。

### 「為什麼不每個 stage 起一個 run？」

六個 run 會放大事件量與跨 run 補償難度。一個 document partition 一條 run 已提供天然隔離；Stage 狀態由 run event 投影即可。只有在外部系統要求獨立審批或獨立執行單元時，才把該 stage 拆成另一個 job。

### 「為什麼還要 BFF 狀態表？Dagster 不是已經有 run 狀態？」

因為業務需要的是「文件是否可閱讀、哪個階段失敗、是否可重試」，不是 `RUN_SUCCESS`、`stepKey`、`partition`。Projection table 讓 UI 查詢更快，也能在 Dagster 事件短暫漏接時對賬修復。

## 28.18 最終建議

先做一個 **只包含 fake resource 的六 stage endpoint-to-end 樣板**，同時實作：

1. `StageSpec` 契約與 `km_*` asset metadata。
2. `stage_execution()` 的 start/failure observation。
3. `Definitions` + full job + 每 stage repair job。
4. contract validator 與 CI。
5. BFF 的固定 stage response schema。

不要在第一步就做：

- 複雜的通用 workflow DSL。
- 任意 stage 排列。
- 每個 stage 一個 run。
- 全量 LLM-as-judge gate。
- 直接讀 Dagster Postgres。
- 讓前端直連 Dagster GraphQL。

只要「StageSpec + terminal asset + blocking check + observation + BFF projection」這五件事成立，後面無論 Pipeline 內部是 VLM、LLM、Pipes container 或既有 CLI，都能接進同一套前端狀態模型。
