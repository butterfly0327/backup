from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv


load_dotenv()


@dataclass(slots=True)
class Settings:
    database_url: str = os.getenv("DATABASE_URL", "")
    parser_version: str = os.getenv("PARSER_VERSION", "v1.0.0")
    request_timeout_seconds: float = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "45"))
    connect_timeout_seconds: float = float(os.getenv("CONNECT_TIMEOUT_SECONDS", "20"))
    save_every: int = int(os.getenv("SAVE_EVERY", "25"))
    max_per_source: Optional[int] = int(os.getenv("MAX_PER_SOURCE", "0")) or None
    safe_mode_default: bool = os.getenv("SAFE_MODE_DEFAULT", "true").lower() == "true"
    runtime_safe_mode: bool = True
    default_safe_limit: int = int(os.getenv("DEFAULT_SAFE_LIMIT", "20"))
    max_safe_limit: int = int(os.getenv("MAX_SAFE_LIMIT", "100"))
    per_source_cooldown_seconds: float = float(os.getenv("PER_SOURCE_COOLDOWN_SECONDS", "20"))
    batch_pause_every: int = int(os.getenv("BATCH_PAUSE_EVERY", "10"))
    batch_pause_seconds: float = float(os.getenv("BATCH_PAUSE_SECONDS", "8"))
    min_request_interval_seconds: float = float(os.getenv("MIN_REQUEST_INTERVAL_SECONDS", "1.2"))
    request_interval_jitter_seconds: float = float(os.getenv("REQUEST_INTERVAL_JITTER_SECONDS", "0.5"))
    retry_max_sleep_seconds: float = float(os.getenv("RETRY_MAX_SLEEP_SECONDS", "30"))
    retry_status_codes_raw: str = os.getenv("RETRY_STATUS_CODES", "408,409,425,429,500,502,503,504")
    deep_enrichment: bool = os.getenv("DEEP_ENRICHMENT", "false").lower() == "true"
    verify_ssl: bool = os.getenv("VERIFY_SSL", "true").lower() == "true"
    user_agent: str = os.getenv(
        "USER_AGENT",
        "dataset-metadata-ingest/1.0 (+https://example.local; contact=admin@example.local)",
    )

    # 선택적 API 키/인증 정보
    huggingface_token: Optional[str] = os.getenv("HUGGINGFACE_TOKEN")
    kaggle_username: Optional[str] = os.getenv("KAGGLE_USERNAME")
    kaggle_key: Optional[str] = os.getenv("KAGGLE_KEY")
    kaggle_config_dir: Optional[str] = os.getenv("KAGGLE_CONFIG_DIR")
    data_gov_api_key: Optional[str] = os.getenv("DATA_GOV_API_KEY")
    aws_odr_api_key: Optional[str] = os.getenv("AWS_ODR_API_KEY")
    github_token: Optional[str] = os.getenv("GITHUB_TOKEN")

    # URL template override
    public_data_portal_list_url_template: str = os.getenv(
        "PUBLIC_DATA_PORTAL_LIST_URL_TEMPLATE",
        "https://www.data.go.kr/en/tcs/dss/selectDataSetList.do?pageIndex={page}&dType=FILE",
    )
    aihub_list_url_template: str = os.getenv(
        "AIHUB_LIST_URL_TEMPLATE",
        "https://www.aihub.or.kr/aihubdata/data/list.do?currMenu=115&pageIndex={page}",
    )

    def __post_init__(self) -> None:
        self.runtime_safe_mode = self.safe_mode_default

    def validate(self) -> None:
        if not self.database_url:
            raise ValueError("DATABASE_URL 환경변수가 비어 있습니다.")
        if self.default_safe_limit <= 0:
            raise ValueError("DEFAULT_SAFE_LIMIT 은 1 이상이어야 합니다.")
        if self.max_safe_limit <= 0:
            raise ValueError("MAX_SAFE_LIMIT 은 1 이상이어야 합니다.")
        if self.max_safe_limit < self.default_safe_limit:
            raise ValueError("MAX_SAFE_LIMIT 은 DEFAULT_SAFE_LIMIT 이상이어야 합니다.")
        if self.per_source_cooldown_seconds < 0:
            raise ValueError("PER_SOURCE_COOLDOWN_SECONDS 는 0 이상이어야 합니다.")
        if self.batch_pause_every < 0:
            raise ValueError("BATCH_PAUSE_EVERY 는 0 이상이어야 합니다.")
        if self.batch_pause_seconds < 0:
            raise ValueError("BATCH_PAUSE_SECONDS 는 0 이상이어야 합니다.")
        if self.min_request_interval_seconds < 0:
            raise ValueError("MIN_REQUEST_INTERVAL_SECONDS 는 0 이상이어야 합니다.")
        if self.request_interval_jitter_seconds < 0:
            raise ValueError("REQUEST_INTERVAL_JITTER_SECONDS 는 0 이상이어야 합니다.")
        if self.retry_max_sleep_seconds <= 0:
            raise ValueError("RETRY_MAX_SLEEP_SECONDS 는 0보다 커야 합니다.")

    @property
    def retry_status_codes(self) -> set[int]:
        result: set[int] = set()
        for token in self.retry_status_codes_raw.split(","):
            text = token.strip()
            if not text:
                continue
            try:
                value = int(text)
            except ValueError:
                continue
            if 100 <= value <= 599:
                result.add(value)
        if not result:
            return {408, 409, 425, 429, 500, 502, 503, 504}
        return result
