# Troubleshooting

## `km validate` 說 handler import 失敗

確認 `pipeline.yaml` 的 `handler` 是 `<module>:<function>`，module 相對於
pipeline root，且目錄有可 import 的 Python package。

## Signature 不符

依 `HANDLER_API.md` 對齊參數名稱與 annotation。Handler 必須同步，不能使用
default arguments。不要為 `ctx` 加上 Dagster context annotation。

## Validation failed

執行：

```powershell
km inspect --last-run --fixes
```

查看 `validation_report` 的 rule id、threshold 與 observed。修正資料後從
`validation` 或更早的 stage 重跑。

## Publish blocked

確認 report 是 `passed`、`blocking_failures=0`、doc identity 與 task 相同。
Publish handler 回傳的 `PublishedDocument.is_visible` 必須是 `true`，且 index
必須確認同一 `publication_id` 已可見。

## Timeout

`E_STAGE_TIMEOUT` 表示 stage 超過 `timeoutSeconds`。先縮小輸入或拆解耗時步驟；
只有 manifest 有 retry policy 時 timeout 才會被視為可重試。

## Definition 載入失敗

確認 image 中設定：

```text
KM_PIPELINE_MANIFEST=/app/pipeline/pipeline.yaml
KM_PIPELINE_ROOT=/app/pipeline
```

並執行 `km definitions-check` 與
`dagster definitions validate -m km_sdk.dagster_defs`。
