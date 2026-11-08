# Pipeline Code Location Examples

`deploy/pipelines/_template/values.yaml` 是部門 Pipeline 的 Helm values 起點。
`deploy/pipelines/knowledge-extraction/values.yaml` 是 manual extraction 範例。

這些檔案只描述 Code Location，不建立平台共用 Dagster instance、PostgreSQL、
webserver 或 daemon。正式上線時由平台提供：

- image registry 與 pull secret
- object store、LLM、embedding、index profiles
- workspace registration
- 共用 storage 與 credentials

Manifest SDK pipeline 的 gRPC module 固定使用 `km_sdk.dagster_defs`：

```yaml
dagsterApiGrpcArgs:
  - "-m"
  - "km_sdk.dagster_defs"
```

image 內需設定：

```text
KM_PIPELINE_MANIFEST=/app/pipeline/pipeline.yaml
KM_PIPELINE_ROOT=/app/pipeline
```
