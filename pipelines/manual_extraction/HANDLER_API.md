# Handler API

固定 stage 順序是：

`ingest -> parse -> extraction -> assemble -> validation -> publish`

每個 stage 只有一個同步 Python callable。使用者不得 import Dagster，也不得
自行建立 asset 依賴。

## Signatures

```python
def ingest(ctx: StageContext, request: IngestRequest) -> IngestedDocument: ...
def parse(ctx: StageContext, source: IngestedDocument) -> ParsedDocument: ...
def extraction(ctx: StageContext, parsed: ParsedDocument, llm: LlmGateway) -> ExtractionResult: ...
def assemble(ctx: StageContext, parsed: ParsedDocument, extracted: ExtractionResult) -> AssembledDocument: ...
def validation(ctx: StageContext, parsed: ParsedDocument, assembled: AssembledDocument) -> ValidationReport: ...
def publish(ctx: StageContext, assembled: AssembledDocument, report: ValidationReport, index: KnowledgeIndex) -> PublishedDocument: ...
```

參數名稱、annotation 與 return annotation 都是契約的一部分。`km validate`
會檢查 async handler、參數順序、預設值、resource annotation 與 return type。

## StageContext

`ctx.options` 是 global 與 stage options 合併後的值。`ctx.artifacts` 可原子寫入
JSON 或 bytes；`ctx.logger` 提供 structured logging。Context 不含 Dagster run
id、instance、環境變數集合或 object-store credential。

## ValidationReport

`blocking_failures` 必須等於所有 `severity=ERROR` 且未通過的 rule 數量；
`warnings` 必須等於所有未通過的 `WARN` rule 數量。若有 blocking failure，
status 必須是 `failed`，runtime 不會執行 publish。

## Publish

`publication_id` 需以 pipeline id、doc id、doc version 與 pipeline version
穩定計算。`validation_report_id` 需指向本次 report。Index 必須先完成候選寫入
與讀回檢查，最後才原子切換 visible pointer。

## 錯誤

```python
raise PipelineError(
    code="E_MODEL_SCHEMA",
    message="relation target does not exist",
    retryable=False,
    stage=Stage.EXTRACTION,
    details={"chunk_id": "chunk-004"},
)
```

未處理 exception 會映射成 `E_HANDLER_BUG`。Timeout 映射成
`E_STAGE_TIMEOUT`；validation gate 映射成 `E_VALIDATION_FAILED`。
