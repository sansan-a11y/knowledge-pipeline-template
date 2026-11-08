# 開始使用

## 1. 安裝

```powershell
python -m pip install "km-sdk==0.1.*"
```

## 2. 看輸入

`fixtures/request.json` 描述文件 identity、tenant 與來源 URI。Handler 只會收到
SDK 的 `IngestRequest`，不需要處理 Dagster partition 或 run id。

## 3. 跑 sample

```powershell
km validate
km test
km run --input fixtures/request.json
```

成功時六個 stage 都應顯示 `succeeded`，且 `publish` 的 `is_visible` 為 `true`。

## 4. 修復單一 stage

```powershell
km run --input fixtures/request.json --repair-from extraction
```

SDK 會從已持久化的 `parse` artifact 載入 `ParsedDocument`，再執行 extraction
與後續 stages。

## 5. 查看結果

```powershell
km inspect --last-run --fixes
```

本地 run state 位於 `.km/`。正式平台使用 ArtifactClient 與 BFF projection，
不依賴本地目錄。
