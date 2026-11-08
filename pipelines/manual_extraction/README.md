# manual_extraction

這是一條由 `km-sdk` 建立的六階段知識抽取 Pipeline：

`ingest -> parse -> extraction -> assemble -> validation -> publish`

使用者文件：
[GETTING_STARTED.md](GETTING_STARTED.md)、
[HANDLER_API.md](HANDLER_API.md)、
[TROUBLESHOOTING.md](TROUBLESHOOTING.md)。

## 我的輸入

本地範例輸入位於 `fixtures/request.json`，文件內容位於 `fixtures/sample.pdf`。
修改 `doc_id`、`doc_version` 與 `tenant` 時，partition 會自動成為
`{doc_id}__{doc_version}`。

## 六個 handler

| Stage | 輸入 | 輸出 |
| --- | --- | --- |
| `ingest` | `IngestRequest` | `IngestedDocument` |
| `parse` | `IngestedDocument` | `ParsedDocument` |
| `extraction` | `ParsedDocument`, `LlmGateway` | `ExtractionResult` |
| `assemble` | `ParsedDocument`, `ExtractionResult` | `AssembledDocument` |
| `validation` | `ParsedDocument`, `AssembledDocument` | `ValidationReport` |
| `publish` | `AssembledDocument`, `ValidationReport`, `KnowledgeIndex` | `PublishedDocument` |

Handler 不需要 import Dagster，也不需要建立 asset 依賴。

## 跑一次

```powershell
km validate
km test
km run --input fixtures/request.json
km inspect --last-run --fixes
```

`km test` 同時驗證成功路徑與 validation failure 會阻斷 publish。

## 錯誤與重跑

失敗輸出會顯示 stage、error code、使用者訊息與 action。修正資料或 handler 後：

```powershell
km run --input fixtures/request.json --repair-from extraction
```

Repair 會從已持久化的上游 artifact 載入輸入，不需要重跑 ingest/parse。

## 上線

將整個目錄放入 image 的 `/app/pipeline`，安裝 `km-sdk==0.1.x`，設定：

```text
KM_PIPELINE_MANIFEST=/app/pipeline/pipeline.yaml
KM_PIPELINE_ROOT=/app/pipeline
```

Dagster Code Location 使用通用模組 `km_sdk.dagster_defs`。
