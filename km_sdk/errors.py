"""User-facing errors for knowledge pipeline handlers and the runtime."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .models import Stage


ERROR_ACTIONS: dict[str, str] = {
    "E_MANIFEST_INVALID": "修正 pipeline.yaml 中標示的欄位後重新執行 km validate",
    "E_HANDLER_IMPORT": "確認 handler module、Python package 與檔案路徑都存在",
    "E_HANDLER_SIGNATURE": "依照 HANDLER_API.md 修正 handler 參數與 return annotation",
    "E_RESOURCE_UNAVAILABLE": "檢查 manifest resource profile 與平台 resource 狀態後重試",
    "E_STAGE_TIMEOUT": "縮小輸入、提高 timeoutSeconds，或修正耗時步驟後重試",
    "E_HANDLER_BUG": "查看開發者 log 與 stack trace，修正 handler 後重試",
    "E_DATA_CONTRACT": "確認 handler 回傳 SDK model，且 doc_id/doc_version 與 partition 一致",
    "E_VALIDATION_FAILED": "查看 validation report 的 blocking rule，修正資料後重跑 validation",
    "E_PUBLISH_GATE": "確認 validation 已通過，且 publish 回傳可見的 PublishedDocument",
    "E_PUBLISH_ROLLBACK": "檢查 index 寫入與可見版本切換，保留舊版本並聯絡平台值班",
    "E_PERMISSION": "確認 pipeline owner 與 resource profile 權限後重試",
}


@dataclass
class PipelineError(RuntimeError):
    """Stable error type that handlers may raise and the runtime may map."""

    code: str
    message: str
    retryable: bool = False
    stage: Stage | None = None
    details: dict[str, Any] = field(default_factory=dict)
    user_message: str | None = None
    action: str | None = None

    def __post_init__(self) -> None:
        RuntimeError.__init__(self, self.message)

    @property
    def error_code(self) -> str:
        return self.code

    @property
    def metadata(self) -> dict[str, Any]:
        return {
            "error_code": self.code,
            "stage": self.stage.value if self.stage else None,
            "retryable": self.retryable,
            "details": self.details,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "stage": self.stage.value if self.stage else None,
            "status": "failed",
            "error": {
                "code": self.code,
                "user_message": self.user_message or self.message,
                "action": self.action or ERROR_ACTIONS.get(self.code, "查看 run 診斷資訊"),
                "retryable": self.retryable,
                "details": self.details,
            },
        }


def map_exception(exc: BaseException, *, stage: Stage) -> PipelineError:
    """Convert arbitrary exceptions into the stable user-facing error model."""

    if isinstance(exc, PipelineError):
        if exc.stage is None:
            exc.stage = stage
        return exc
    return PipelineError(
        code="E_HANDLER_BUG",
        message=str(exc) or type(exc).__name__,
        retryable=False,
        stage=stage,
        details={"exception_class": type(exc).__name__},
    )
