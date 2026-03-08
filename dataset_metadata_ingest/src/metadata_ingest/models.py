from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(slots=True)
class NormalizedDatasetRecord:
    """`dataset` 테이블 1행에 대응하는 정규화 모델.

    source_code / dataset_source_id / collection run id 는 DB 계층에서 붙인다.
    여기서는 사이트별 원본 메타데이터를 공통 포맷으로 맞추는 데 집중한다.
    """

    source_dataset_key: str
    record_hash: Optional[str] = None
    canonical_url: Optional[str] = None
    landing_url: Optional[str] = None
    title: Optional[str] = None
    subtitle: Optional[str] = None
    description_short: Optional[str] = None
    description_long: Optional[str] = None
    search_text: Optional[str] = None
    publisher_name: Optional[str] = None

    domains: List[str] = field(default_factory=list)
    tasks: List[str] = field(default_factory=list)
    modalities: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    languages: List[str] = field(default_factory=list)

    license_name: Optional[str] = None
    license_url: Optional[str] = None
    commercial_use_allowed: Optional[bool] = None

    access_type: Optional[str] = None
    login_required: Optional[bool] = None
    approval_required: Optional[bool] = None
    payment_required: Optional[bool] = None
    is_restricted: Optional[bool] = None

    source_created_at: Optional[str] = None
    source_updated_at: Optional[str] = None
    source_version: Optional[str] = None

    row_count: Optional[int] = None
    dataset_size_bytes: Optional[int] = None

    field_presence_json: Dict[str, Any] = field(default_factory=dict)
    creators_json: List[Dict[str, Any]] = field(default_factory=list)
    resources_json: List[Dict[str, Any]] = field(default_factory=list)
    schema_json: Dict[str, Any] = field(default_factory=dict)
    metrics_json: Dict[str, Any] = field(default_factory=dict)
    extra_json: Dict[str, Any] = field(default_factory=dict)
    raw_json: Dict[str, Any] = field(default_factory=dict)

    status: str = "ACTIVE"


@dataclass(slots=True)
class SourceDefinition:
    source_code: str
    source_name: str
    base_url: str
    collection_type: str


@dataclass(slots=True)
class CollectionRunInfo:
    run_id: int
    source_id: int
    checkpoint_json: Dict[str, Any]


@dataclass(slots=True)
class HarvestStats:
    collected_count: int = 0
    upserted_count: int = 0
    failed_count: int = 0
    last_saved_source_dataset_key: Optional[str] = None
    errors: List[str] = field(default_factory=list)

    def to_error_summary(self, limit: int = 20) -> Optional[str]:
        if not self.errors:
            return None
        head = self.errors[:limit]
        more = ""
        if len(self.errors) > limit:
            more = f"\n... and {len(self.errors) - limit} more"
        return "\n".join(head) + more
