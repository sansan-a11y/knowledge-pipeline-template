# Knowledge Pipeline Template v1

這是一個可直接執行的六階段 Dagster Pipeline Template 初版。

```text
ingest -> parse -> extraction -> assemble -> validation -> publish
```

Template 固定的是 stage 邊界與 terminal asset，不限制 stage 內部的 asset 拆分方式。

## 已包含

- `contract.py`：固定 Stage 枚舉、terminal asset、metadata、group 與契約驗證。
- `assets.py`：六個可端到端執行的 terminal assets。
- `resources.py`：本地 fake storage，以及 idempotent publish/visible pointer 範例。
- `defs.py`：Definitions、full job 與每個 stage 的 repair job。
- `demo.py`：不依賴外部服務的 smoke run。
- `validation` blocking gate：validation 失敗時 publish 不會執行。

## 本地執行

```powershell
$env:DAGSTER_HOME = "$PWD\.dagster_home"

& .\.venv\Scripts\python.exe -m dagster definitions validate -m pipelines._template.defs
& .\.venv\Scripts\python.exe -m pipelines._template.contract
& .\.venv\Scripts\python.exe -m pipelines._template.demo
```

啟動 Dagster UI：

```powershell
& .\.venv\Scripts\python.exe -m dagster dev -m pipelines._template.defs
```

本地產物預設寫到 `.template_data/`，只供 demo 使用。

## 建立自己的 Pipeline

### 1. 複製模板

```powershell
Copy-Item -Recurse pipelines\_template pipelines\<pipeline_module>
```

`<pipeline_module>` 必須是合法的 Python package 名稱，使用 snake_case，例如
`manual_extraction`。Template 內部使用 relative imports，因此目錄改名後不需要逐檔修改 import。

### 2. 修改 identity

在 `contract.py` 修改：

```python
PIPELINE_ID = "<全局唯一的 pipeline id>"
PIPELINE_VERSION = "<release version>"
OWNER = "<team:owner>"
```

`PIPELINE_ID` 會決定：

- Asset key prefix。
- Stage canonical group。
- Full job 與 repair job 名稱。
- `km_pipeline_id` metadata。

不要把 Git repo 名稱或 Kubernetes release 名稱直接當成 `PIPELINE_ID`，這些名稱可以改變，但 Pipeline id 是前端與 BFF 的長期契約。

此後的 Definition module 會是：

```text
pipelines.<pipeline_module>.defs
```

### 3. 替換 partition 與 resources

初版使用：

```python
DOC_PARTITIONS = dg.StaticPartitionsDefinition(["demo__v1"])
```

正式上線要換成平台提供的 document partition，建議使用：

```text
{doc_id}__{doc_version}
```

不要直接使用 `{doc_id}::{doc_version}`，除非平台自訂 IO Manager 已處理路徑編碼。
Dagster 預設 `FilesystemIOManager` 會把 `::` 誤判成 URI protocol。

同時替換 `TemplateStorage`：

- MinIO/S3 ObjectStore resource。
- LLM/VLM gateway resource。
- 向量庫與圖庫 resource。
- 平台錯誤處理與 fake resource。

不要在自己的 asset 中直接建立 `boto3`、`psycopg2` 或模型 client；憑證與 endpoint 只能由平台 Resource 注入。

### 4. 保留六個 terminal asset

每個 stage 必須保留一個 terminal asset：

| Stage | Terminal asset |
| --- | --- |
| `ingest` | `ingested_document` |
| `parse` | `parsed_document` |
| `extraction` | `extracted_knowledge` |
| `assemble` | `assembled_document` |
| `validation` | `validation_report` |
| `publish` | `published_document` |

可以在 stage 內新增任意內部 asset，例如：

```text
parse/
  page_image
  page_classification
  document_chunks
  parsed_document
```

但跨 stage dependency 只能接上一個 stage 的 terminal asset。

### 新增內部 asset 與依賴

最簡單的客製方式是保留六個 terminal asset 的 key、partition、group 與 metadata，
只替換函式內部的演算法。例如把 `extracted_knowledge` 裡的 fake entity/relation
換成呼叫平台 LLM gateway：

```python
@dg.asset(
    key=TERMINAL_ASSETS[Stage.EXTRACTION],
    partitions_def=DOC_PARTITIONS,
    group_name=stage_group(Stage.EXTRACTION),
    owners=[OWNER],
    kinds={"python", "llm"},
    metadata=terminal_metadata(Stage.EXTRACTION),
    ins={"parsed_uri": dg.AssetIn(key=TERMINAL_ASSETS[Stage.PARSE])},
    check_specs=[
        dg.AssetCheckSpec(
            name="extraction_contract",
            asset=TERMINAL_ASSETS[Stage.EXTRACTION],
            blocking=True,
        )
    ],
)
def extracted_knowledge(context, storage, llm, parsed_uri: str):
    with stage_execution(context, stage=Stage.EXTRACTION):
        parsed = storage.get_json(parsed_uri)
        extraction = llm.extract(
            chunks=parsed["chunks"],
            schema_version=1,
        )
        uri = storage.put_json(
            f"extract/{context.partition_key}/knowledge.json",
            extraction,
        )
        yield dg.Output(
            value=uri,
            metadata=_runtime_metadata(
                Stage.EXTRACTION,
                context.partition_key,
                extra={
                    "model_version": extraction["model_version"],
                    "prompt_version": extraction["prompt_version"],
                },
            ),
        )
        yield dg.AssetCheckResult(
            check_name="extraction_contract",
            passed=bool(extraction["entities"]),
        )
```

`llm` 是平台 Resource。Resource key 必須等於函式參數名稱，因此在
`defs.py` 的 `resources` 裡要提供 `"llm"`。

如果 stage 內部需要拆成多個 asset，可以建立只在該 stage 使用的內部 asset：

```python
PAGE_CLASSIFICATION = dg.AssetKey(["km", PIPELINE_ID, "page_classification"])


@dg.asset(
    key=PAGE_CLASSIFICATION,
    partitions_def=DOC_PARTITIONS,
    group_name=stage_group(Stage.PARSE),
    owners=[OWNER],
    kinds={"python", "vlm"},
    metadata=internal_metadata(Stage.PARSE),
    ins={"ingested_uri": dg.AssetIn(key=TERMINAL_ASSETS[Stage.INGEST])},
)
def page_classification(
    context: dg.AssetExecutionContext,
    storage,
    vlm,
    ingested_uri: str,
) -> str:
    with stage_execution(context, stage=Stage.PARSE):
        source = storage.get_json(ingested_uri)
        classified = vlm.classify(source)
        return storage.put_json(
            f"parse/{context.partition_key}/classification.json",
            classified,
        )
```

接著讓 `parsed_document` 依賴內部 asset，而不是直接依賴 ingest：

```python
@dg.asset(
    key=TERMINAL_ASSETS[Stage.PARSE],
    partitions_def=DOC_PARTITIONS,
    group_name=stage_group(Stage.PARSE),
    metadata=terminal_metadata(Stage.PARSE),
    ins={"classification_uri": dg.AssetIn(key=PAGE_CLASSIFICATION)},
    check_specs=[
        dg.AssetCheckSpec(
            name="chunk_contract",
            asset=TERMINAL_ASSETS[Stage.PARSE],
            blocking=True,
        )
    ],
)
def parsed_document(context, storage, classification_uri: str):
    with stage_execution(context, stage=Stage.PARSE):
        classified = storage.get_json(classification_uri)
        # parse + chunk
        ...
```

最後把新增 asset 加入 `ALL_ASSETS`：

```python
ALL_ASSETS = [
    ingested_document,
    page_classification,
    parsed_document,
    extracted_knowledge,
    assembled_document,
    validation_report,
    published_document,
]
```

依賴規則：

- 同一 stage 內：terminal 可以依賴任意內部 asset；內部 asset 之間也可以 fan-out / fan-in。
- 跨 stage：consumer 只能依賴上游 stage 的 terminal asset。
- 例如 `extracted_knowledge` 不能依賴 `page_classification`，必須依賴 `parsed_document`。
- 用 `ins={"x": dg.AssetIn(key=...)}` 取得 Dagster 注入的上游值。
- 只用 `deps=[dg.AssetDep(key=...)]` 表示「先執行，但不注入值」；此時 consumer 要自己用 Resource 讀取 handle。
- 新 asset 必須設定 `group_name=stage_group(stage)` 與 `metadata=internal_metadata(stage)`。

### 5. 實作 validation

Validation 必須產出可審計的 report，不能只寫 log。

建議至少包含：

- schema 與 enum 合法性。
- relation 兩端存在。
- citation/chunk id 可回溯。
- coverage 與空抽取率。
- policy version、threshold、observed、rule id。

Validation 失敗時：

- `validation_report` 可以 materialize，供診斷。
- blocking check 必須失敗。
- `publish` 不得執行。
- BFF 回傳 validation 的 `failed` 或 `blocked` 狀態。

### 6. 實作 publish

Publish 必須具備：

- `publication_id` 的固定計算方式。
- shadow index / staging namespace。
- smoke query 或 write-after-read check。
- 原子可見版本切換。
- 失敗時保留舊版本。
- 重跑同一版本不產生第二份可見資料。

### 7. 跑 CI

```powershell
& .\.venv\Scripts\python.exe -m dagster definitions validate -m pipelines.<pipeline_module>.defs
& .\.venv\Scripts\python.exe -m pytest -q
& .\.venv\Scripts\python.exe -m pipelines.<pipeline_module>.contract
```

CI 應額外檢查：

- 六個 terminal asset 齊全。
- `km_stage_id` 只能是固定六個值。
- canonical group 正確。
- 同一 pipeline 的 partition definition 一致。
- validation 與 publish 都有 blocking check。
- 不存在繞過平台 Resource 的憑證或 client 建立。

### 8. 部署與上線

1. 建置不可變 image：`<pipeline-name>:<date>-<git-sha>`。
2. 使用 `deploy/pipelines/_template/values.yaml` 建立該 Pipeline 的 Helm values。
3. 平台團隊在 `workspace.yaml` 註冊 Code Location。
4. 在 staging 用單一 document partition 跑 full job。
5. 驗證 stage events、validation gate、visible pointer 與 BFF 狀態。
6. 由平台提升 image tag；Pipeline 團隊不直接修改共享 `dagster.yaml`。

## BFF 觸發方式

正式環境由 BFF：

1. 登記文件與 task id。
2. 加入 document dynamic partition。
3. 用 `launchBackfill` 選擇 `published_document` 或 full job 的 terminal asset。
4. 寫入 run tags：`task_id`、`pipeline_id`、`doc_id`、`doc_version`、`tenant`。
5. 查詢 stage observation、materialization、asset checks 與 run 狀態。
6. 回推 BFF task/stage projection；定時對賬兜底。

前端只收到固定的 `ingest/parse/extraction/assemble/validation/publish` 六個 stage，不直接依賴 AssetKey 或 GraphQL。

## Source of truth

- Dagster run、event、materialization、asset check：執行真相源。
- Business task/stage projection：BFF 的查詢緩存，可重建。
- `validation_report`：是否允許發布的可審計依據。
- `published_document` / visible pointer：目前使用者可讀取的版本。

## 上線驗收

- [ ] `pipelines._template.contract` 等價的契約驗證通過。
- [ ] 六個 terminal asset 在單一 partition 端到端成功。
- [ ] validation failure 阻擋 publish。
- [ ] publish 重跑具備 idempotency。
- [ ] publish 失敗時舊版本仍可見。
- [ ] stage STARTED/FAILED observation 可見。
- [ ] BFF 可重建六個固定 stage 狀態。
- [ ] image tag 不可變，Dagster 版本與平台 base image 一致。
