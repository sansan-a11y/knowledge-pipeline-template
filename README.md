# Knowledge Pipeline Template

Manifest + Python Handler SDK，用固定六階段建立知識抽取 Pipeline：

```text
ingest -> parse -> extraction -> assemble -> validation -> publish
```

使用者只需提供：

- `pipeline.yaml`
- 六個 Python handlers
- 本地 fixtures

SDK 會負責 Dagster assets、固定依賴、asset checks、stage observations、
repair jobs、metadata、artifact 契約與 publish gate。

## 內容

| Path | Purpose |
| --- | --- |
| `km_sdk/` | Manifest、型別契約、runner、Dagster runtime 與 `km` CLI |
| `pipelines/_template/` | 人工維護的六階段 reference implementation |
| `pipelines/manual_extraction/` | 從 Manifest SDK 啟動的遷移範例 |
| `docs/` | Template、Manifest、checks 與錯誤處理設計 |
| `deploy/pipelines/` | Code Location Helm values 範例 |
| `tests/` | Schema、signature、全鏈、gate、repair 與 Dagster 契約測試 |

## 安裝

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
```

## 建立新 Pipeline

```powershell
.\.venv\Scripts\km.exe init my-pipeline
cd my_pipeline
..\.venv\Scripts\km.exe validate
..\.venv\Scripts\km.exe test
```

## 執行範例

```powershell
.\.venv\Scripts\km.exe validate `
  --manifest pipelines/manual_extraction/pipeline.yaml `
  --project-root pipelines/manual_extraction

.\.venv\Scripts\km.exe test `
  --manifest pipelines/manual_extraction/pipeline.yaml `
  --project-root pipelines/manual_extraction

.\.venv\Scripts\km.exe run `
  --input pipelines/manual_extraction/fixtures/request.json `
  --manifest pipelines/manual_extraction/pipeline.yaml `
  --project-root pipelines/manual_extraction
```

## Dagster Code Location

```powershell
$env:KM_PIPELINE_MANIFEST = "$PWD\pipelines\manual_extraction\pipeline.yaml"
$env:KM_PIPELINE_ROOT = "$PWD\pipelines\manual_extraction"

python -m dagster definitions validate -m km_sdk.dagster_defs
```

正式 run 需帶 `tenant`、`pipeline_id`、`doc_id`、`doc_version` tags。

## 文件

- [Getting Started](pipelines/manual_extraction/GETTING_STARTED.md)
- [Handler API](pipelines/manual_extraction/HANDLER_API.md)
- [Troubleshooting](pipelines/manual_extraction/TROUBLESHOOTING.md)
- [Template v1](docs/30-knowledge-pipeline-template-v1.md)
- [KM SDK Manifest MVP](docs/31-km-sdk-manifest-mvp.md)

## 驗證

```powershell
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\km.exe definitions-check `
  --manifest pipelines/manual_extraction/pipeline.yaml `
  --project-root pipelines/manual_extraction
```

這個 repo 不包含平台共用 Dagster instance、BFF、多租戶控制平面或正式
object store/LLM/index credentials。這些由平台 Resource profile 提供。
