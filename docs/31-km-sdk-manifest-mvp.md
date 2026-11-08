# 31 · KM Pipeline SDK Manifest MVP 開發說明

> 文件狀態：MVP 設計，尚未實作  
> 目標版本：`km-sdk 0.1.x`、manifest `v1`  
> 適用基線：Dagster 1.13.x、Python 3.11  
> 前置閱讀：`docs/30-knowledge-pipeline-template-v1.md`

## 31.0 這一步要解決什麼

目前 `pipelines/_template` 已經證明六階段 pipeline 可以運作，但使用者仍需要：

- 編輯 `assets.py`。
- 理解 `@dg.asset`、`AssetIn`、`AssetCheckSpec`。
- 理解 `Definitions`、job、partition、observation。
- 知道 metadata、group、check 與 repair job 的關係。
- 看懂 Dagster run、step、event log 才能排查錯誤。

這不符合「容易進駐平台」的目標。

第一步 MVP 要建立一個 **Manifest + Python Handler SDK**：

```text
使用者提供：
  pipeline.yaml
  handlers.py
  fixtures/

SDK 自動生成：
  Dagster assets
  dependencies
  asset checks
  stage observations
  jobs
  metadata
  retry / timeout
  Definitions
```

使用者不需要 import Dagster，也不需要建立 asset 依賴。

## 31.1 MVP 的核心決策

### 31.1.1 固定六階段

Runtime 固定生成：

```text
ingest -> parse -> extraction -> assemble -> validation -> publish
```

使用者只能為這六個 stage 提供 handler，不能自創 stage。

### 31.1.2 每個 stage 一個 handler

MVP 只支援每個 stage 一個 Python callable：

```yaml
stages:
  parse:
    handler: handlers:parse
```

不支援使用者宣告任意 sub-DAG、任意 asset 或任意 step order。

### 31.1.3 使用者不需要建立 Dagster asset

Runtime 根據 manifest 生成固定六個 terminal assets：

| Stage | Runtime asset |
| --- | --- |
| `ingest` | `ingested_document` |
| `parse` | `parsed_document` |
| `extraction` | `extracted_knowledge` |
| `assemble` | `assembled_document` |
| `validation` | `validation_report` |
| `publish` | `published_document` |

依賴關係由 Runtime 固定：

```text
parsed_document
  <- ingested_document

extracted_knowledge
  <- parsed_document

assembled_document
  <- parsed_document
  <- extracted_knowledge

validation_report
  <- parsed_document
  <- assembled_document

published_document
  <- assembled_document
  <- validation_report
```

使用者不可能接錯跨 stage 依賴。

### 31.1.4 使用者程式使用平台型別

使用者 handler 的輸入輸出使用 SDK 定義的型別，例如：

```python
def extract(
    ctx: StageContext,
    parsed: ParsedDocument,
    llm: LlmGateway,
) -> ExtractionResult:
    ...
```

Handler 不接收 Dagster `AssetExecutionContext`，也不直接接收 asset key、partition、run id 等 Dagster 概念。

### 31.1.5 失敗要能轉成使用者語言

使用者看到的應該是：

```text
extraction failed: E_MODEL_SCHEMA
page 17, chunk 4
LLM returned a relation with an unknown entity id
action: check prompt/schema and retry extraction
```

而不是：

```text
RUN_FAILURE on step km__pipeline__extracted_knowledge
```

## 31.2 MVP 範圍

### 31.2.1 In scope

- `pipeline.yaml` manifest schema。
- 六個固定 stage handler interface。
- JSON Schema 與 Python 型別驗證。
- 動態生成 Dagster `Definitions`。
- 固定 asset 依賴與 canonical group。
- Stage observation。
- Validation blocking gate。
- Publish gate 與 atomic visibility contract。
- 每個 stage 的 timeout 與 retry policy。
- 本地 fake object store 與 sample runner。
- `km init`、`km validate`、`km test`、`km run`、`km inspect`。
- CI 契約檢查。
- Code Location 通用 entrypoint。
- 從現有 `_template` 遷移的參考範例。

### 31.2.2 Out of scope

- 任意 DAG 或任意 asset graph 配置。
- 視覺化拖拉式 pipeline editor。
- 任意 stage ordering。
- 任意使用者自訂 Dagster resource wiring。
- 任意使用者自訂 blocking check 程式。
- Java / Go 原生 SDK。
- Container / Pipes handler。
- 完整 builtin step catalog。
- 多語言 prompt evaluator。
- 全量 LLM-as-judge。
- 多租戶強隔離與不信任程式碼 sandbox。

這些能力留到後續版本，不應污染 MVP 介面。

## 31.3 使用者看到的專案

`km init manual-extraction` 生成：

```text
manual_extraction/
├── pipeline.yaml
├── handlers.py
├── fixtures/
│   ├── request.json
│   └── sample.pdf
├── tests/
│   └── test_handlers.py
└── README.md
```

使用者不需要的檔案：

- `defs.py`
- `assets.py`
- `contract.py`
- `schedules.py`
- `sensors.py`
- `workspace.yaml`

如果使用者需要進入 advanced debug，SDK 仍可以另外生成或顯示底層 Definitions，但不作為主要入口。

## 31.4 Manifest 規格

### 31.4.1 完整範例

```yaml
apiVersion: knowledge.pipeline/v1
kind: KnowledgeExtractionPipeline

metadata:
  id: manual_extraction
  version: 0.1.0
  owner: team:knowledge
  description: 機台手冊知識抽取

spec:
  runtime:
    contractVersion: 1
    artifactStore: object_store

  partition:
    keyTemplate: "{doc_id}__{doc_version}"
    requiredTags:
      - tenant

  stages:
    ingest:
      handler: handlers:ingest
      timeoutSeconds: 300
      retry:
        maxRetries: 2
        delaySeconds: 2
        backoff: exponential

    parse:
      handler: handlers:parse
      timeoutSeconds: 3600

    extraction:
      handler: handlers:extraction
      timeoutSeconds: 7200
      retry:
        maxRetries: 1
        delaySeconds: 10
        backoff: exponential

    assemble:
      handler: handlers:assemble
      timeoutSeconds: 600

    validation:
      handler: handlers:validation
      timeoutSeconds: 600

    publish:
      handler: handlers:publish
      timeoutSeconds: 600

  resources:
    llm:
      profile: llm-default
    embedding:
      profile: embedding-default
    index:
      profile: knowledge-index-default

  options:
    language: zh-TW
    maxEntitiesPerChunk: 40
    maxRelationsPerChunk: 40

  fixtures:
    request: fixtures/request.json
    source: fixtures/sample.pdf
```

### 31.4.2 `metadata`

| 欄位 | 必填 | 規則 |
| --- | --- | --- |
| `id` | 是 | `[a-z][a-z0-9_]{2,63}`，建立後不可改 |
| `version` | 是 | SemVer |
| `owner` | 是 | `team:<name>` 或 email |
| `description` | 否 | 使用者可讀說明 |

`metadata.id` 是 BFF、前端、asset namespace 與 deployment 的共同識別。

### 31.4.3 Partition

MVP 只支援單維 document partition：

```text
{doc_id}__{doc_version}
```

`keyTemplate` 必須是平台支援的模板，不允許使用者自行組合任意符號。

禁止使用：

```text
{doc_id}::{doc_version}
```

原因是預設 `FilesystemIOManager` 會把冒號解讀成 URI protocol。

### 31.4.4 Stage handler

格式：

```text
<module>:<function>
```

範例：

```yaml
handler: handlers:extraction
```

Runtime 規則：

- `module` 相對於 pipeline root。
- Module 必須可 import。
- Function 必須存在。
- Signature 必須符合 stage contract。
- Return annotation 必須是對應型別。
- 不允許 async handler；MVP 由 handler 自行管理同步/非同步 client。

### 31.4.5 Resource profile

MVP 的 manifest 只能引用平台核准的 profile：

```yaml
resources:
  llm:
    profile: llm-default
```

不得在 manifest 寫入：

- API token。
- DB password。
- bucket credential。
- endpoint 中的私有 query。
- 任意 Python object import。

Handler 透過參數名稱取得平台注入的 resource：

```python
def extraction(
    ctx: StageContext,
    parsed: ParsedDocument,
    llm: LlmGateway,
) -> ExtractionResult:
    ...
```

Runtime 使用參數名稱 `llm` 找到 manifest 的 `resources.llm`。

### 31.4.6 Options

`spec.options` 是給六個 handler 共用的業務配置：

```python
ctx.options["maxEntitiesPerChunk"]
```

Options 必須：

- JSON serializable。
- 不得存 credentials。
- 由 Runtime 寫進 run tags / materialization metadata 的 hash。
- 變更後能追溯是哪個 option 組合產生結果。

Stage-specific options 可在 stage 下增加 `options`，Runtime 以 stage options 覆蓋 global options。

## 31.5 Stage 資料契約

MVP 建議使用 Pydantic model，並由 `km-sdk` 提供。使用者不需要自訂 wire format。

### 31.5.1 `IngestRequest`

```python
class IngestRequest(BaseModel):
    doc_id: str
    doc_version: str
    tenant: str
    source_uri: str
    mime_type: str | None = None
```

### 31.5.2 `IngestedDocument`

```python
class IngestedDocument(BaseModel):
    doc_id: str
    doc_version: str
    tenant: str
    source_uri: str
    mime_type: str
    content_hash: str
    byte_size: int
```

### 31.5.3 `ParsedDocument`

```python
class ParsedChunk(BaseModel):
    chunk_id: str
    text: str
    page: int | None
    metadata: dict[str, object] = {}


class ParsedDocument(BaseModel):
    doc_id: str
    doc_version: str
    source_hash: str
    parser_version: str
    chunks: list[ParsedChunk]
```

### 31.5.4 `ExtractionResult`

```python
class ExtractedEntity(BaseModel):
    id: str
    name: str
    type: str
    descriptions: list[str] = []
    citations: list[str] = []


class ExtractedRelation(BaseModel):
    source_id: str
    relation: str
    target_id: str
    citations: list[str] = []


class ExtractionResult(BaseModel):
    doc_id: str
    doc_version: str
    schema_version: str
    model_version: str
    prompt_version: str
    entities: list[ExtractedEntity]
    relations: list[ExtractedRelation]
```

### 31.5.5 `AssembledDocument`

```python
class AssembledDocument(BaseModel):
    doc_id: str
    doc_version: str
    title: str
    sections: list[dict[str, object]]
    entities: list[ExtractedEntity]
    relations: list[ExtractedRelation]
    citations: list[str]
    assembly_version: str
```

### 31.5.6 `ValidationReport`

```python
class ValidationRuleResult(BaseModel):
    rule_id: str
    passed: bool
    severity: Literal["ERROR", "WARN"]
    threshold: object
    observed: object
    message: str | None = None


class ValidationReport(BaseModel):
    doc_id: str
    doc_version: str
    policy_version: str
    status: Literal["passed", "failed"]
    blocking_failures: int
    warnings: int
    rules: list[ValidationRuleResult]
```

Runtime 必須檢查：

- `status == "passed"`。
- `blocking_failures == 0`。
- 所有 `severity=ERROR` 的 rule 都 passed。

任何一項不符，`publish` 不得執行。

### 31.5.7 `PublishedDocument`

```python
class PublishedDocument(BaseModel):
    doc_id: str
    doc_version: str
    publication_id: str
    validation_report_id: str
    index_namespace: str
    published_at: datetime
    is_visible: bool
```

Runtime 驗證 `is_visible=True` 後才可將 business task 標記為 completed。

## 31.6 Handler Interface

六個 handler 的 signature 固定如下。

### 31.6.1 Ingest

```python
def ingest(
    ctx: StageContext,
    request: IngestRequest,
) -> IngestedDocument:
    ...
```

### 31.6.2 Parse

```python
def parse(
    ctx: StageContext,
    source: IngestedDocument,
) -> ParsedDocument:
    ...
```

### 31.6.3 Extraction

```python
def extraction(
    ctx: StageContext,
    parsed: ParsedDocument,
    llm: LlmGateway,
) -> ExtractionResult:
    ...
```

### 31.6.4 Assemble

```python
def assemble(
    ctx: StageContext,
    parsed: ParsedDocument,
    extracted: ExtractionResult,
) -> AssembledDocument:
    ...
```

### 31.6.5 Validation

```python
def validation(
    ctx: StageContext,
    parsed: ParsedDocument,
    assembled: AssembledDocument,
) -> ValidationReport:
    ...
```

### 31.6.6 Publish

```python
def publish(
    ctx: StageContext,
    assembled: AssembledDocument,
    report: ValidationReport,
    index: KnowledgeIndex,
) -> PublishedDocument:
    ...
```

## 31.7 StageContext

`StageContext` 是 SDK 提供的穩定介面，不是 Dagster context。

```python
@dataclass(frozen=True)
class StageContext:
    pipeline_id: str
    pipeline_version: str
    task_id: str
    partition_key: str
    doc_id: str
    doc_version: str
    tenant: str
    stage: Stage
    attempt: int
    options: Mapping[str, object]
    artifacts: ArtifactClient
    logger: StructuredLogger
```

可使用能力：

```python
ctx.artifacts.put_json("parse/output.json", payload)
ctx.artifacts.get_json(uri)
ctx.logger.info("parsed pages", page_count=38)
ctx.options["language"]
```

禁止暴露：

- Dagster instance。
- Dagster run id。
- 任意環境變數集合。
- 平台 DB connection。
- 共享 object-store credential。

## 31.8 Runtime 架構

```text
pipeline.yaml
    |
    v
Manifest parser + JSON Schema
    |
    v
Handler loader + signature validator
    |
    v
PipelineRuntimeBuilder
    |
    +--> six AssetDefinitions
    +--> checks
    +--> full job
    +--> six repair jobs
    +--> stage observations
    +--> metadata contract
    |
    v
Dagster Definitions
```

### 31.8.1 Runtime asset 包裝

使用者 handler 被 Runtime 包裝，而不是直接成為 Dagster asset。

概念流程：

```python
with stage_execution(context, stage):
    input_model = artifact_store.load_stage_inputs(stage, partition)
    result = invoke_user_handler(
        handler,
        stage_context,
        input_model,
        resources,
    )
    artifact_store.persist_stage_output(stage, partition, result)
    yield stage_metadata(stage, result)
    yield validation_check_when_needed(stage, result)
```

使用者 handler 不直接處理：

- Asset key。
- Partition。
- Metadata entry。
- Check result。
- Observation。

### 31.8.2 固定依賴

Runtime 自動建立：

```text
parse <- ingest
extraction <- parse
assemble <- parse + extraction
validation <- parse + assemble
publish <- assemble + validation
```

若使用者需要新增內部 asset，MVP 不支援在 manifest 宣告；後續 advanced mode 才處理。

建議 Runtime 的 generated asset 使用固定 `deps` 表達血緣，不使用動態生成的
`AssetIn` 參數注入大型資料：

- 每個 generated asset 的函式 signature 保持一樣。
- Stage output 由 ArtifactClient 持久化。
- 下一個 stage 從 ArtifactClient 載入上一個 terminal output。
- Dagster 仍保留完整 asset dependency graph。
- Stage repair 可以從已存在的 artifact 恢復，不依賴同一 run 的記憶體。
- 使用者 handler 只看到 SDK typed model，不看到 Dagster output object。

這也是 Runtime 能夠穩定支援「一個 stage 一個 handler」與「單 stage 修復」的關鍵。

### 31.8.3 自動 generated Definitions

使用者 image 的通用 entrypoint 建議固定：

```text
km_sdk.dagster_defs
```

它讀取：

```text
KM_PIPELINE_MANIFEST=/app/pipeline/pipeline.yaml
KM_PIPELINE_ROOT=/app/pipeline
```

然後回傳 `defs`。

這表示平台註冊所有 Pipeline 時都可以使用同一個 Definition module，差異只在 image 與 manifest。

## 31.9 本地執行流程

### 31.9.1 `km init`

```powershell
km init manual_extraction
```

生成：

- manifest。
- 六個 handler stub。
- 型別範例。
- fake resources。
- sample request。
- sample source。
- 一個 handler unit test。

### 31.9.2 `km validate`

```powershell
km validate
```

需要檢查：

- YAML schema。
- API version。
- stage 完整且無多餘項目。
- handler import。
- handler signature。
- return annotation。
- resource profile 存在。
- partition template 合法。
- 沒有 secrets 或非法環境變數引用。
- options 可 JSON serialize。

### 31.9.3 `km test`

```powershell
km test
```

使用 fake resources 和 fixture 執行六個 stage 的邏輯，但不啟動 Dagster、不用 K8s、不連 MinIO 或 LLM。

測試至少包含：

- handler unit test。
- 全鏈 fixture smoke test。
- validation failure 阻斷 publish。
- handler signature 錯誤。
- manifest schema 錯誤。

### 31.9.4 `km run`

```powershell
km run --input fixtures/request.json
```

本地 runner 可以使用 ephemeral Dagster instance，但使用者不需要知道。

輸出格式：

```text
ingest       succeeded  0.4s
parse        succeeded  8.2s
extraction   running    17/38 chunks
assemble     pending
validation   pending
publish      pending
```

### 31.9.5 `km inspect`

```powershell
km inspect --last-run
km inspect --fixes
```

顯示：

- 每個 stage 的狀態與時間。
- Handler return summary。
- Validation warnings。
- Publish visibility。
- User-facing error code。
- 可重跑的 stage。

### 31.9.6 `km diagnose`

```powershell
km diagnose
```

檢查：

- manifest 與 SDK 版本相容。
- resources 可用。
- fixture 是否存在。
- Python package 是否可 import。
- image 中的 manifest 路徑。
- 常見錯誤與解法。

## 31.10 錯誤模型

### 31.10.1 使用者可拋出的錯誤

```python
from km_sdk import PipelineError


raise PipelineError(
    code="E_MODEL_SCHEMA",
    message="LLM returned a relation with an unknown entity id",
    retryable=False,
    stage=Stage.EXTRACTION,
    details={"chunk_id": "chunk-004", "endpoint": "target_id"},
)
```

### 31.10.2 Runtime 錯誤碼

| Code | 含義 | 預設重試 |
| --- | --- | --- |
| `E_MANIFEST_INVALID` | manifest 不符合 schema | 否 |
| `E_HANDLER_IMPORT` | handler 無法 import | 否 |
| `E_HANDLER_SIGNATURE` | signature 或 return type 不合法 | 否 |
| `E_RESOURCE_UNAVAILABLE` | resource profile 無法連線 | 是 |
| `E_STAGE_TIMEOUT` | stage 超過 timeout | 視 stage 設定 |
| `E_HANDLER_BUG` | 未處理 exception | 非預設 |
| `E_VALIDATION_FAILED` | validation blocking failure | 否 |
| `E_PUBLISH_GATE` | validation 不允許發布 | 否 |
| `E_PUBLISH_ROLLBACK` | 發布提交或 rollback 失敗 | 否，立即告警 |

### 31.10.3 使用者訊息

API 回傳：

```json
{
  "stage": "extraction",
  "status": "failed",
  "error": {
    "code": "E_MODEL_SCHEMA",
    "user_message": "模型輸出缺少 relation 的目標實體",
    "action": "請檢查 prompt 與 schema，修正後重試 extraction",
    "retryable": false
  }
}
```

完整 stack trace 只給開發者 debug endpoint。

## 31.11 Retry 與 Timeout

### 31.11.1 Stage retry

Manifest：

```yaml
retry:
  maxRetries: 2
  delaySeconds: 2
  backoff: exponential
```

Runtime 轉成 Dagster `RetryPolicy`，但只對可重試錯誤使用。

不可重試錯誤：

- `E_MANIFEST_INVALID`
- `E_HANDLER_SIGNATURE`
- `E_DATA_CONTRACT`
- `E_VALIDATION_FAILED`
- `E_PERMISSION`

可重試錯誤：

- 429 / rate limit。
- 暫時性 network timeout。
- object-store 5xx。
- model gateway 5xx。

### 31.11.2 Timeout

Timeout 是 stage-level 預算，不是 handler 自己 sleep。

Runtime 應：

- 寫入 timeout metadata。
- 產生 stage failed signal。
- 由失敗類型決定是否重試。
- 不把 timeout 一律包成 unknown exception。

## 31.12 Validation 與 Publish 契約

### 31.12.1 Validation

Runtime 從 handler 拿到 `ValidationReport` 後：

1. 驗證 model。
2. 驗證 doc id/version 與 task 一致。
3. 驗證 `blocking_failures` 與 rules 一致。
4. 產生 blocking asset check。
5. 將 validation report URI 放入 metadata。

若 report status 為 `failed`：

- validation stage 顯示 failed。
- publish stage 顯示 blocked。
- task 顯示 failed，但不顯示「系統故障」。

### 31.12.2 Publish

Runtime 必須在呼叫使用者 publish handler 前再檢查：

```text
validation_report.status == passed
validation_report.blocking_failures == 0
validation_report.doc_id == task.doc_id
validation_report.doc_version == task.doc_version
```

呼叫 handler 後，Runtime 再檢查：

```text
published_document.is_visible == true
published_document.validation_report_id 存在
publication_id 可追溯
```

使用者 handler 不得繞過這兩個 gate。

## 31.13 Artifact 與 Idempotency

Runtime 統一寫入 stage outputs。

建議路徑：

```text
km/
  pipeline_id={pipeline_id}/
  tenant={tenant}/
  doc_id={doc_id}/
  doc_version={doc_version}/
  stage={stage}/
  attempt={attempt}/
  output.json
```

規則：

- 同一 stage 重跑使用新的 attempt。
- 發布只依賴最後通過 validation 的 attempt。
- 舊 attempt 保留供 debug。
- Runtime 用 content hash 作 data version。
- Runtime 對 output 做 JSON schema 驗證。
- 大文件由 `ctx.artifacts` 存放，不經 event log。

Stage output 寫入要盡量原子：

```text
write temp -> fsync/flush -> rename/commit
```

正式平台由 `ArtifactClient` 實作，不由使用者自行處理。

## 31.14 CI 與契約檢查

### 31.14.1 必跑命令

```powershell
km validate
km test
km definitions-check
dagster definitions validate -m km_sdk.dagster_defs
```

### 31.14.2 CI 規則

- 使用者程式不得 import `dagster`。
- Manifest 不得包含 secrets。
- Handler module 不得在 import time 呼叫外部服務。
- 六個 stage 必須完整。
- Handler signature 必須符合固定接口。
- `publish` 前必須有 runtime validation gate。
- `publication_id` 必須穩定且可重算。
- Sample fixture 必須能跑完 validation。
- Validation failure fixture 必須證明 publish 不執行。

### 31.14.3 Generated artifact

CI 可生成 `compiled-manifest.json`：

```json
{
  "pipeline_id": "manual_extraction",
  "pipeline_version": "0.1.0",
  "contract_version": 1,
  "stages": [
    {"stage": "ingest", "asset": "km/manual_extraction/ingested_document"},
    {"stage": "parse", "asset": "km/manual_extraction/parsed_document"}
  ],
  "schema_hash": "sha256:...",
  "handler_api_version": 1
}
```

這份檔案供平台註冊與 BFF catalog 使用，不作為使用者手動維護的 source of truth。

## 31.15 Deployment

### 31.15.1 Image

使用者 image 包含：

```text
/app/pipeline/pipeline.yaml
/app/pipeline/handlers.py
/app/pipeline/fixtures/*
km-sdk==0.1.x
```

固定 entrypoint：

```text
python -m km_sdk.grpc_server
```

或平台指定的 Dagster code server module：

```text
km_sdk.dagster_defs
```

### 31.15.2 Registration

平台從 compiled manifest 取得：

- pipeline id。
- image。
- manifest path。
- resource profiles。
- owner。
- contract hash。

平台可自動生成 workspace registration。使用者不需要編輯 `workspace.yaml`。

### 31.15.3 Trigger

BFF 觸發流程不變：

1. 建立 business task。
2. 加入 document partition。
3. 呼叫 `launchBackfill`。
4. 帶上 task/pipeline/doc/tenant tags。
5. 訂閱或輪詢 stage events。
6. 更新 task/stage projection。

使用者只看到 task API 與六個 stage 狀態。

## 31.16 在地化與使用者文件

Template README 不應從 Dagster 教學開始，而應從以下順序開始：

1. 我的輸入長什麼樣。
2. 六個 stage 各自要回傳什麼。
3. 最小 handler 怎麼寫。
4. 如何用 sample 跑一次。
5. 如何看錯誤與重跑。
6. 如何上傳與上線。

文件不能把 Dagster 當成使用者必讀知識。

建議三份文件：

| 文件 | 對象 |
| --- | --- |
| `GETTING_STARTED.md` | 第一次建立 Pipeline 的使用者 |
| `HANDLER_API.md` | 需要寫 Python handler 的使用者 |
| `TROUBLESHOOTING.md` | 跑不起來或 validation 失敗的使用者 |

## 31.17 安全邊界

MVP 的 Python handler 仍在平台 Code Location image 中執行，因此不是不信任程式碼 sandbox。

規則：

- 平台 resource profile 控制 credentials。
- Handler 不得自行讀取 Secrets。
- Handler 不得建立任意 DB/object-store client。
- CI 阻擋 `os.environ` 與未核准 client imports。
- Production registration 仍由平台控制。
- 若未來允許外部團隊上傳任意程式碼，需改為獨立 container / sandbox。

## 31.18 版本相容

需要分開管理：

| 版本 | 範例 | 用途 |
| --- | --- | --- |
| Manifest schema | `knowledge.pipeline/v1` | YAML 結構 |
| SDK API | `km-sdk 0.1.x` | Handler 介面 |
| Runtime contract | `contractVersion: 1` | Stage/asset/status 契約 |
| Pipeline version | `0.1.0` | 業務與 handler 版本 |
| Handler API | `handlerVersion: 1` | 函式 signature |

規則：

- Manifest major 變更提供 migration。
- SDK 0.x 期間允許快速迭代，但每個 release 要有 changelog。
- Runtime contract 破壞性變更要雙讀期。
- compiled manifest 必須記錄 schema hash 與 SDK version。

## 31.19 驗收標準

### 31.19.1 使用體驗

- `km init` 後 5 分鐘內可跑 sample。
- 使用者不需要 import Dagster。
- 使用者不需要修改任何 `defs.py`。
- 第一次 manifest/signature 錯誤在 10 秒內指出。
- 本地測試不依賴外部 MinIO/LLM/vector DB。
- API 回應只包含固定六個 stage。

### 31.19.2 契約行為

- Runtime 自動生成六個 assets。
- Async stage dependency 固定且不可被使用者破壞。
- Validation failure 100% 阻止 publish。
- Publish 失敗時舊版本仍可見。
- 同一 partition 的同版本重跑冪等。
- Stage repair 只重跑指定 stage，並從 artifact 載入上游。
- Handler exception 能映射成 stage error code。

### 31.19.3 測試

必須有：

- Manifest schema 測試。
- Handler signature 測試。
- Handler return type 測試。
- Full pipeline fixture smoke test。
- Validation failure blocks publish。
- Publish idempotency。
- Repair stage from persisted artifacts。
- Unknown handler exception。
- Timeout mapping。
- Secret scanning。

## 31.20 實作里程碑

### Milestone 1：Type contracts

交付：

- `IngestRequest` 到 `PublishedDocument` 的 models。
- `StageContext`。
- `PipelineError`。
- JSON schema。
- 單元測試。

驗收：

- 六個 stage 的 input/output 都有強型別。
- Model 可 JSON serialize。
- Doc id/version 與 report 內容可以驗證。

### Milestone 2：Manifest loader

交付：

- YAML parser。
- JSON Schema validation。
- handler entrypoint resolver。
- signature validator。
- compiled manifest。

驗收：

- 錯誤 manifest 有明確錯誤路徑。
- Handler import 與 signature 可在 CI 提前發現。

### Milestone 3：Runtime builder

交付：

- 六個 generated assets。
- 固定 dependencies。
- checks。
- jobs。
- `km_sdk.dagster_defs`。

驗收：

- `dagster definitions validate` 通過。
- 全鏈 sample 成功。
- validation failure 阻止 publish。

### Milestone 4：CLI

交付：

- `km init`。
- `km validate`。
- `km test`。
- `km run`。
- `km inspect`。

驗收：

- 新使用者不查 Dagster 文件即可跑 sample。
- 錯誤輸出包含 stage、error code、action。

### Milestone 5：Pilot pipeline

選擇一條真實 Pipeline：

- 保留人工維護的 handler。
- 用 manifest 載入。
- 在 staging 跑單一 document partition。
- 驗證 BFF 六階段狀態。
- 對比現有 asset template 的輸出。

## 31.21 從現有 Template 遷移

現有 `pipelines/_template` 保留，作為 Runtime 的 reference implementation。

遷移步驟：

1. 將 `assets.py` 的資料轉換拆成六個 handler。
2. 將 fake storage 換成平台 `ArtifactClient`。
3. 將 inline checks 換成 `ValidationReport`。
4. 將 publish fake 換成平台 KnowledgeIndex。
5. 將 `contract.py` 保留在 SDK 內部，不給使用者修改。
6. 用 `pipeline.yaml` 取代 user-facing `defs.py`。

## 31.22 不建議的設計

### 31.22.1 任意 YAML DAG

不要在 MVP 支援：

```yaml
steps:
  - id: a
    needs: [b, c]
```

這會把 Dagster graph 問題重新包裝成另一門配置語言。

### 31.22.2 讓使用者自行寫 blocking check

Validation 應回傳結構化 `ValidationReport`，由 Runtime 轉成 check。否則使用者仍需要理解 Asset Check semantics。

### 31.22.3 把 resource wiring 暴露給使用者

Manifest 只引用 profile 名稱，不允許使用者指定 Dagster resource class、環境變數名稱或 secret 路徑。

### 31.22.4 以 Dagster UI 作為主要開發者入口

Dagster UI 是平台工程師的 debug 工具。使用者入口應該是 `km` CLI、自研 UI 與結構化 API。

### 31.22.5 為每個 stage 建一個 Dagster run

MVP 維持「一個 document partition 一個 run」，stage 狀態由同一 run 的事件投影。

## 31.23 最終目標

使用者看到的世界：

```text
填 manifest
  -> 寫六個普通 Python handler
  -> km test
  -> 上傳
  -> 看六階段進度與錯誤
```

平台內部的世界：

```text
Dagster Assets + Checks + Jobs + Partitions + Sensors + Observations
```

兩者只透過穩定的 manifest、handler contract、stage status API 與 artifact contract 耦合。

這才是「Pipeline 很容易進駐平台」的正確分層。
