"""Manifest parsing, schema validation, and handler contract checks."""

from __future__ import annotations

import ast
import hashlib
import importlib.util
import inspect
import json
import re
import sys
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Literal

import jsonschema
import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

from .context import StageContext
from .errors import PipelineError
from .models import (
    STAGE_ORDER,
    TERMINAL_ASSETS,
    AssembledDocument,
    ExtractionResult,
    IngestedDocument,
    IngestRequest,
    ParsedDocument,
    PublishedDocument,
    Stage,
    ValidationReport,
)
from .resources import KnowledgeIndex, LlmGateway

API_VERSION = "knowledge.pipeline/v1"
KIND = "KnowledgeExtractionPipeline"
CONTRACT_VERSION = 1
HANDLER_API_VERSION = 1
SDK_VERSION = "0.1.0"
PACKAGE_ROOT = Path(__file__).resolve().parent
SCHEMA_PATH = PACKAGE_ROOT / "schema" / "manifest.schema.json"
APPROVED_RESOURCE_PROFILES = frozenset(
    {"llm-default", "embedding-default", "knowledge-index-default"}
)


class RetrySpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    maxRetries: int = Field(default=1, ge=0, le=10)
    delaySeconds: float = Field(default=0, ge=0)
    backoff: Literal["fixed", "exponential"] = "fixed"


class StageConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    handler: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_.]*:[A-Za-z_][A-Za-z0-9_]*$")
    timeoutSeconds: float = Field(gt=0, le=86400)
    retry: RetrySpec | None = None
    options: dict[str, object] = Field(default_factory=dict)


class StagesConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ingest: StageConfig
    parse: StageConfig
    extraction: StageConfig
    assemble: StageConfig
    validation: StageConfig
    publish: StageConfig

    def get(self, stage: Stage) -> StageConfig:
        return getattr(self, stage.value)


class RuntimeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    contractVersion: Literal[1]
    artifactStore: Literal["object_store"]


class PartitionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    keyTemplate: str
    requiredTags: list[str]

    @field_validator("keyTemplate")
    @classmethod
    def supported_template(cls, value: str) -> str:
        if value != "{doc_id}__{doc_version}":
            raise ValueError("keyTemplate must be {doc_id}__{doc_version}")
        return value

    @field_validator("requiredTags")
    @classmethod
    def tenant_required(cls, value: list[str]) -> list[str]:
        if "tenant" not in value:
            raise ValueError("requiredTags must include tenant")
        return value


class ResourceProfile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    profile: str = Field(pattern=r"^[a-z][a-z0-9-]{1,63}$")


class ResourcesConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    llm: ResourceProfile | None = None
    embedding: ResourceProfile | None = None
    index: ResourceProfile | None = None


class FixturesConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request: str | None = None
    source: str | None = None


class ManifestMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(pattern=r"^[a-z][a-z0-9_]{2,63}$")
    version: str = Field(
        pattern=(
            r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
            r"(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$"
        )
    )
    owner: str = Field(
        pattern=r"^(team:[A-Za-z0-9._-]+|[^@\s]+@[^@\s]+\.[^@\s]+)$"
    )
    description: str | None = None


class SpecConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    runtime: RuntimeConfig
    partition: PartitionConfig
    stages: StagesConfig
    resources: ResourcesConfig = Field(default_factory=ResourcesConfig)
    options: dict[str, object] = Field(default_factory=dict)
    fixtures: FixturesConfig = Field(default_factory=FixturesConfig)


class PipelineManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    apiVersion: Literal["knowledge.pipeline/v1"]
    kind: Literal["KnowledgeExtractionPipeline"]
    metadata: ManifestMetadata
    spec: SpecConfig

    @property
    def pipeline_id(self) -> str:
        return self.metadata.id

    @property
    def pipeline_version(self) -> str:
        return self.metadata.version

    def merged_options(self, stage: Stage) -> dict[str, object]:
        return {**self.spec.options, **self.spec.stages.get(stage).options}


@dataclass(frozen=True)
class StageContract:
    stage: Stage
    inputs: tuple[type[BaseModel], ...]
    resources: tuple[type[object], ...]
    output: type[BaseModel]

    @property
    def names(self) -> tuple[str, ...]:
        parameter_names = {
            Stage.INGEST: ("ctx", "request"),
            Stage.PARSE: ("ctx", "source"),
            Stage.EXTRACTION: ("ctx", "parsed", "llm"),
            Stage.ASSEMBLE: ("ctx", "parsed", "extracted"),
            Stage.VALIDATION: ("ctx", "parsed", "assembled"),
            Stage.PUBLISH: ("ctx", "assembled", "report", "index"),
        }
        return parameter_names[self.stage]


STAGE_CONTRACTS: dict[Stage, StageContract] = {
    Stage.INGEST: StageContract(Stage.INGEST, (IngestRequest,), (), IngestedDocument),
    Stage.PARSE: StageContract(Stage.PARSE, (IngestedDocument,), (), ParsedDocument),
    Stage.EXTRACTION: StageContract(
        Stage.EXTRACTION, (ParsedDocument,), (LlmGateway,), ExtractionResult
    ),
    Stage.ASSEMBLE: StageContract(
        Stage.ASSEMBLE,
        (ParsedDocument, ExtractionResult),
        (),
        AssembledDocument,
    ),
    Stage.VALIDATION: StageContract(
        Stage.VALIDATION,
        (ParsedDocument, AssembledDocument),
        (),
        ValidationReport,
    ),
    Stage.PUBLISH: StageContract(
        Stage.PUBLISH,
        (AssembledDocument, ValidationReport),
        (KnowledgeIndex,),
        PublishedDocument,
    ),
}


@dataclass
class LoadedHandler:
    stage: Stage
    entrypoint: str
    module: ModuleType
    function: Callable[..., object]


@dataclass
class LoadedPipeline:
    manifest: PipelineManifest
    manifest_path: Path
    project_root: Path
    handlers: dict[Stage, LoadedHandler]


def manifest_schema() -> dict[str, Any]:
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


@lru_cache(maxsize=1)
def schema_hash() -> str:
    canonical = json.dumps(manifest_schema(), sort_keys=True, separators=(",", ":"))
    return f"sha256:{hashlib.sha256(canonical.encode('utf-8')).hexdigest()}"


def _manifest_error(errors: list[str]) -> PipelineError:
    return PipelineError(
        code="E_MANIFEST_INVALID",
        message="pipeline manifest is invalid",
        stage=None,
        details={"errors": errors},
    )


def _format_schema_error(error: jsonschema.ValidationError) -> str:
    path = "$" + "".join(
        f"[{part}]" if isinstance(part, int) else f".{part}" for part in error.absolute_path
    )
    return f"{path}: {error.message}"


def _scan_secrets(value: object, *, path: str = "$") -> list[str]:
    violations: list[str] = []
    secret_key = re.compile(
        r"(?:^|_)(?:token|password|passwd|secret|credential|api_key|private_key|access_key)(?:$|_)",
        re.IGNORECASE,
    )
    env_patterns = (
        re.compile(r"\$\{[^}]+\}"),
        re.compile(r"\$ENV\{[^}]+\}", re.IGNORECASE),
        re.compile(r"\bos\.environ\b"),
        re.compile(r"://[^/\s:@]+:[^/\s@]+@"),
    )
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}"
            if secret_key.search(str(key)):
                violations.append(f"{child_path}: secret-like field is forbidden")
            violations.extend(_scan_secrets(child, path=child_path))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            violations.extend(_scan_secrets(child, path=f"{path}[{index}]"))
    elif isinstance(value, str):
        if any(pattern.search(value) for pattern in env_patterns):
            violations.append(f"{path}: environment or credential reference is forbidden")
    return violations


def _assert_json_serializable(value: object, *, path: str) -> list[str]:
    try:
        json.dumps(value, allow_nan=False)
    except (TypeError, ValueError) as exc:
        return [f"{path}: options must be JSON serializable ({exc})"]
    return []


def validate_manifest_data(data: object) -> PipelineManifest:
    validator = jsonschema.Draft202012Validator(manifest_schema())
    schema_errors = sorted(validator.iter_errors(data), key=lambda item: list(item.path))
    if schema_errors:
        raise _manifest_error([_format_schema_error(error) for error in schema_errors])
    try:
        manifest = PipelineManifest.model_validate(data)
    except Exception as exc:
        errors = [str(exc)]
        if hasattr(exc, "errors"):
            errors = [
                f"$.{'.'.join(str(part) for part in error['loc'])}: {error['msg']}"
                for error in exc.errors()
            ]
        raise _manifest_error(errors) from exc

    secret_errors = _scan_secrets(data)
    serialization_errors = _assert_json_serializable(manifest.spec.options, path="$.spec.options")
    for stage in STAGE_ORDER:
        serialization_errors.extend(
            _assert_json_serializable(
                manifest.spec.stages.get(stage).options,
                path=f"$.spec.stages.{stage.value}.options",
            )
        )
    errors = [*secret_errors, *serialization_errors]
    if errors:
        raise _manifest_error(errors)
    return manifest


def load_manifest(path: str | Path) -> PipelineManifest:
    manifest_path = Path(path).resolve()
    if not manifest_path.is_file():
        raise _manifest_error([f"$.manifest: file not found: {manifest_path}"])
    try:
        data = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise _manifest_error([f"$.manifest: unable to read YAML ({exc})"]) from exc
    return validate_manifest_data(data)


def _module_path(project_root: Path, module_name: str) -> Path:
    parts = module_name.split(".")
    file_path = project_root.joinpath(*parts).with_suffix(".py")
    if file_path.is_file():
        return file_path
    package_path = project_root.joinpath(*parts, "__init__.py")
    if package_path.is_file():
        return package_path
    raise PipelineError(
        code="E_HANDLER_IMPORT",
        message=f"handler module '{module_name}' was not found under {project_root}",
    )


def _import_handler_module(project_root: Path, module_name: str) -> ModuleType:
    module_path = _module_path(project_root, module_name)
    existing = sys.modules.get(module_name)
    if existing is not None:
        existing_file = Path(getattr(existing, "__file__", "")).resolve()
        if existing_file == module_path:
            return existing
        for name in [
            name
            for name in sys.modules
            if name == module_name or name.startswith(f"{module_name}.")
        ]:
            sys.modules.pop(name, None)
    old_path = list(sys.path)
    try:
        sys.path.insert(0, str(project_root))
        importlib.invalidate_caches()
        module = importlib.import_module(module_name)
    except Exception as exc:
        sys.modules.pop(module_name, None)
        raise PipelineError(
            code="E_HANDLER_IMPORT",
            message=f"unable to import handler module '{module_name}': {exc}",
            details={"exception_class": type(exc).__name__, "module": module_name},
        ) from exc
    finally:
        sys.path[:] = old_path
    return module


def validate_handler_signature(
    stage: Stage,
    function: Callable[..., object],
) -> None:
    contract = STAGE_CONTRACTS[stage]
    problems: list[str] = []
    if inspect.iscoroutinefunction(function):
        problems.append("async handlers are not supported")
    signature = inspect.signature(function)
    parameters = list(signature.parameters.values())
    expected_names = contract.names
    actual_names = tuple(parameter.name for parameter in parameters)
    if actual_names != expected_names:
        problems.append(
            f"expected parameters {expected_names}, found {actual_names}"
        )
    for parameter in parameters:
        if parameter.kind not in {
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.POSITIONAL_ONLY,
        }:
            problems.append(f"parameter '{parameter.name}' must be positional")
        if parameter.default is not inspect.Parameter.empty:
            problems.append(f"parameter '{parameter.name}' cannot have a default")
    try:
        hints = inspect.get_annotations(function, eval_str=True)
    except Exception as exc:
        raise PipelineError(
            code="E_HANDLER_SIGNATURE",
            message=f"{stage.value}: unable to resolve annotations: {exc}",
            stage=stage,
            details={"handler": getattr(function, "__qualname__", repr(function))},
        ) from exc
    expected_annotations = (StageContext, *contract.inputs, *contract.resources)
    for name, expected in zip(actual_names, expected_annotations, strict=False):
        actual = hints.get(name)
        if actual != expected:
            problems.append(
                f"parameter '{name}' annotation must be {expected.__name__}, "
                f"found {getattr(actual, '__name__', actual)!s}"
            )
    return_annotation = hints.get("return")
    if return_annotation != contract.output:
        problems.append(
            f"return annotation must be {contract.output.__name__}, "
            f"found {getattr(return_annotation, '__name__', return_annotation)!s}"
        )
    if problems:
        raise PipelineError(
            code="E_HANDLER_SIGNATURE",
            message=f"{stage.value}: " + "; ".join(problems),
            stage=stage,
            details={
                "handler": getattr(function, "__qualname__", repr(function)),
                "problems": problems,
            },
        )


def scan_project_source(project_root: Path) -> None:
    violations: list[str] = []
    banned_modules = {
        "boto3",
        "botocore",
        "psycopg",
        "psycopg2",
        "pymongo",
        "redis",
        "sqlalchemy",
    }
    for path in project_root.rglob("*.py"):
        if any(part.startswith(".") for part in path.relative_to(project_root).parts):
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError) as exc:
            violations.append(f"{path.name}: unable to parse source ({exc})")
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    root_name = alias.name.split(".", maxsplit=1)[0]
                    if root_name == "dagster":
                        violations.append(
                            f"{path.name}:{node.lineno}: user code must not import dagster"
                        )
                    if root_name in banned_modules:
                        violations.append(
                            f"{path.name}:{node.lineno}: direct client import '{root_name}' is forbidden"
                        )
            elif isinstance(node, ast.ImportFrom) and node.module:
                root_name = node.module.split(".", maxsplit=1)[0]
                if root_name == "dagster":
                    violations.append(
                        f"{path.name}:{node.lineno}: user code must not import dagster"
                    )
                if root_name in banned_modules:
                    violations.append(
                        f"{path.name}:{node.lineno}: direct client import '{root_name}' is forbidden"
                    )
                if node.module == "os" and any(
                    alias.name in {"environ", "getenv"} for alias in node.names
                ):
                    violations.append(
                        f"{path.name}:{node.lineno}: handlers must not read environment variables"
                    )
            elif (
                isinstance(node, ast.Attribute)
                and isinstance(node.value, ast.Name)
                and node.value.id == "os"
                and node.attr in {"environ", "getenv"}
            ):
                violations.append(
                    f"{path.name}:{node.lineno}: handlers must not read os.environ"
                )
    if violations:
        raise PipelineError(
            code="E_MANIFEST_INVALID",
            message="user pipeline source violates the SDK security contract",
            details={"errors": violations},
        )


def load_pipeline(
    manifest_path: str | Path,
    *,
    project_root: str | Path | None = None,
    check_source: bool = True,
    approved_profiles: set[str] | frozenset[str] | None = None,
) -> LoadedPipeline:
    path = Path(manifest_path).resolve()
    manifest = load_manifest(path)
    root = Path(project_root).resolve() if project_root else path.parent
    if not root.is_dir():
        raise _manifest_error([f"$.projectRoot: directory not found: {root}"])
    if check_source:
        scan_project_source(root)

    profiles = (
        approved_profiles
        if approved_profiles is not None
        else APPROVED_RESOURCE_PROFILES
    )
    unknown_profiles = [
        f"$.spec.resources.{name}.profile: unapproved profile "
        f"{profile.profile!r}"
        for name in ("llm", "embedding", "index")
        if (profile := getattr(manifest.spec.resources, name)) is not None
        and profile.profile not in profiles
    ]
    if unknown_profiles:
        raise _manifest_error(unknown_profiles)

    handlers: dict[Stage, LoadedHandler] = {}
    for stage in STAGE_ORDER:
        entrypoint = manifest.spec.stages.get(stage).handler
        module_name, function_name = entrypoint.split(":", maxsplit=1)
        module = _import_handler_module(root, module_name)
        try:
            function = getattr(module, function_name)
        except AttributeError as exc:
            raise PipelineError(
                code="E_HANDLER_IMPORT",
                message=f"{stage.value}: handler '{entrypoint}' does not exist",
                stage=stage,
                details={"entrypoint": entrypoint},
            ) from exc
        if not callable(function):
            raise PipelineError(
                code="E_HANDLER_IMPORT",
                message=f"{stage.value}: handler '{entrypoint}' is not callable",
                stage=stage,
            )
        validate_handler_signature(stage, function)
        handlers[stage] = LoadedHandler(stage, entrypoint, module, function)

    for stage in (Stage.EXTRACTION, Stage.PUBLISH):
        resource_name = "llm" if stage == Stage.EXTRACTION else "index"
        if getattr(manifest.spec.resources, resource_name) is None:
            raise _manifest_error(
                [f"$.spec.resources.{resource_name}: resource profile is required"]
            )

    for fixture_name in ("request", "source"):
        fixture = getattr(manifest.spec.fixtures, fixture_name)
        if fixture and not (root / fixture).is_file():
            raise _manifest_error(
                [f"$.spec.fixtures.{fixture_name}: file not found: {root / fixture}"]
            )
    return LoadedPipeline(manifest, path, root, handlers)


def compiled_manifest(manifest: PipelineManifest) -> dict[str, object]:
    return {
        "pipeline_id": manifest.pipeline_id,
        "pipeline_version": manifest.pipeline_version,
        "contract_version": manifest.spec.runtime.contractVersion,
        "stages": [
            {
                "stage": stage.value,
                "asset": f"km/{manifest.pipeline_id}/{TERMINAL_ASSETS[stage]}",
            }
            for stage in STAGE_ORDER
        ],
        "schema_hash": schema_hash(),
        "handler_api_version": HANDLER_API_VERSION,
        "sdk_version": SDK_VERSION,
    }
