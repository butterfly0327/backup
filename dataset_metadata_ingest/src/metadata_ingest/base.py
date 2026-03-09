from __future__ import annotations

import json
import random
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, Optional, Tuple
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from .config import Settings
from .db import Database
from .models import HarvestStats, NormalizedDatasetRecord, SourceDefinition
from .utils import clean_text, compact_dict, safe_get


_SOURCE_THROTTLE_PROFILES: Dict[str, Dict[str, float]] = {
    # 크롤링 계열: 가장 보수적
    "AI_HUB": {
        "min_request_interval_seconds": 4.0,
        "request_interval_jitter_seconds": 1.1,
        "batch_pause_every": 4.0,
        "batch_pause_seconds": 24.0,
        "per_source_cooldown_seconds": 75.0,
    },
    "PUBLIC_DATA_PORTAL": {
        "min_request_interval_seconds": 3.2,
        "request_interval_jitter_seconds": 0.9,
        "batch_pause_every": 6.0,
        "batch_pause_seconds": 18.0,
        "per_source_cooldown_seconds": 60.0,
    },
    # 인증/대형 플랫폼: 보수적
    "KAGGLE": {
        "min_request_interval_seconds": 2.7,
        "request_interval_jitter_seconds": 0.8,
        "batch_pause_every": 6.0,
        "batch_pause_seconds": 14.0,
        "per_source_cooldown_seconds": 50.0,
    },
    "HUGGINGFACE": {
        "min_request_interval_seconds": 2.4,
        "request_interval_jitter_seconds": 0.8,
        "batch_pause_every": 6.0,
        "batch_pause_seconds": 12.0,
        "per_source_cooldown_seconds": 50.0,
    },
    "DATA_GOV": {
        "min_request_interval_seconds": 1.7,
        "request_interval_jitter_seconds": 0.5,
        "batch_pause_every": 8.0,
        "batch_pause_seconds": 10.0,
        "per_source_cooldown_seconds": 35.0,
    },
    "DATA_EUROPA": {
        "min_request_interval_seconds": 1.8,
        "request_interval_jitter_seconds": 0.5,
        "batch_pause_every": 8.0,
        "batch_pause_seconds": 8.0,
        "per_source_cooldown_seconds": 30.0,
    },
    # 비교적 안정 API: 기본보다 보수적
    "HARVARD_DATAVERSE": {
        "min_request_interval_seconds": 1.6,
        "request_interval_jitter_seconds": 0.4,
        "batch_pause_every": 10.0,
        "batch_pause_seconds": 7.0,
        "per_source_cooldown_seconds": 24.0,
    },
    "FIGSHARE": {
        "min_request_interval_seconds": 1.5,
        "request_interval_jitter_seconds": 0.4,
        "batch_pause_every": 10.0,
        "batch_pause_seconds": 7.0,
        "per_source_cooldown_seconds": 22.0,
    },
    "ZENODO": {
        "min_request_interval_seconds": 1.7,
        "request_interval_jitter_seconds": 0.6,
        "batch_pause_every": 10.0,
        "batch_pause_seconds": 8.0,
        "per_source_cooldown_seconds": 26.0,
    },
    "AWS_ODR": {
        "min_request_interval_seconds": 1.6,
        "request_interval_jitter_seconds": 0.4,
        "batch_pause_every": 9.0,
        "batch_pause_seconds": 8.0,
        "per_source_cooldown_seconds": 26.0,
    },
}


class ResumeGate:
    """같은 페이지/오프셋을 다시 읽을 때 마지막 저장 키 이전 항목을 건너뛴다."""

    def __init__(self, last_saved_source_dataset_key: Optional[str]):
        self.last_key = last_saved_source_dataset_key
        self.unlocked = last_saved_source_dataset_key is None

    def allow(self, source_dataset_key: Optional[str]) -> bool:
        if self.unlocked:
            return True
        if source_dataset_key and source_dataset_key == self.last_key:
            self.unlocked = True
            return False
        return False


class BaseDatasetCollector(ABC):
    source: SourceDefinition

    def __init__(self, db: Database, settings: Settings):
        self.db = db
        self.settings = settings
        self.stats = HarvestStats()
        self._client = httpx.Client(
            headers={"User-Agent": settings.user_agent},
            timeout=httpx.Timeout(settings.request_timeout_seconds, connect=settings.connect_timeout_seconds),
            follow_redirects=True,
            verify=settings.verify_ssl,
        )
        self._last_request_monotonic: Optional[float] = None
        self._rng = random.Random()
        self.min_request_interval_seconds = settings.min_request_interval_seconds
        self.request_interval_jitter_seconds = settings.request_interval_jitter_seconds
        self.batch_pause_every = settings.batch_pause_every
        self.batch_pause_seconds = settings.batch_pause_seconds
        self.per_source_cooldown_seconds = settings.per_source_cooldown_seconds
        self._apply_source_throttle_profile()

    def _apply_source_throttle_profile(self) -> None:
        if not self.settings.runtime_safe_mode:
            return
        profile = _SOURCE_THROTTLE_PROFILES.get(self.source.source_code)
        if not profile:
            return

        self.min_request_interval_seconds = max(
            self.min_request_interval_seconds,
            float(profile.get("min_request_interval_seconds", self.min_request_interval_seconds)),
        )
        self.request_interval_jitter_seconds = max(
            self.request_interval_jitter_seconds,
            float(profile.get("request_interval_jitter_seconds", self.request_interval_jitter_seconds)),
        )

        profile_batch_every = int(profile.get("batch_pause_every", float(self.batch_pause_every)))
        if self.batch_pause_every <= 0:
            self.batch_pause_every = profile_batch_every
        else:
            self.batch_pause_every = min(self.batch_pause_every, profile_batch_every)

        self.batch_pause_seconds = max(
            self.batch_pause_seconds,
            float(profile.get("batch_pause_seconds", self.batch_pause_seconds)),
        )
        self.per_source_cooldown_seconds = max(
            self.per_source_cooldown_seconds,
            float(profile.get("per_source_cooldown_seconds", self.per_source_cooldown_seconds)),
        )

    @property
    def client(self) -> httpx.Client:
        return self._client

    def close(self) -> None:
        self._client.close()

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)

    def request(
        self,
        method: str,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        json_payload: Any = None,
        retries: int = 3,
        retry_sleep_seconds: float = 1.2,
    ) -> httpx.Response:
        last_error: Optional[Exception] = None
        for attempt in range(1, retries + 1):
            try:
                self._pace_before_request()
                response = self.client.request(method, url, params=params, headers=headers, json=json_payload)
                if response.status_code in self.settings.retry_status_codes and attempt < retries:
                    self.sleep(self._retry_sleep_seconds(response, attempt, retry_sleep_seconds))
                    continue
                if response.status_code >= 400:
                    response.raise_for_status()
                return response
            except httpx.RequestError as exc:  # pragma: no cover - 네트워크 예외 대응
                last_error = exc
                if attempt >= retries:
                    break
                self.sleep(min(self.settings.retry_max_sleep_seconds, retry_sleep_seconds * attempt))
        assert last_error is not None
        raise last_error

    def _pace_before_request(self) -> None:
        base_wait = self.min_request_interval_seconds
        if base_wait <= 0:
            self._last_request_monotonic = time.monotonic()
            return

        jitter = 0.0
        if self.request_interval_jitter_seconds > 0:
            jitter = self._rng.uniform(0.0, self.request_interval_jitter_seconds)

        target_interval = base_wait + jitter
        now = time.monotonic()
        if self._last_request_monotonic is not None:
            elapsed = now - self._last_request_monotonic
            remaining = target_interval - elapsed
            if remaining > 0:
                self.sleep(remaining)
        self._last_request_monotonic = time.monotonic()

    def _retry_sleep_seconds(self, response: httpx.Response, attempt: int, fallback_base: float) -> float:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                retry_after_seconds = float(retry_after)
                if retry_after_seconds > 0:
                    return min(self.settings.retry_max_sleep_seconds, retry_after_seconds)
            except ValueError:
                pass
        return min(self.settings.retry_max_sleep_seconds, fallback_base * attempt)

    def get_json(
        self,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        retries: int = 3,
    ) -> Any:
        response = self.request("GET", url, params=params, headers=headers, retries=retries)
        response.raise_for_status()
        return response.json()

    def get_text(
        self,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        retries: int = 3,
    ) -> str:
        response = self.request("GET", url, params=params, headers=headers, retries=retries)
        response.raise_for_status()
        return response.text

    def get_soup(
        self,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        parser: str = "lxml",
        retries: int = 3,
    ) -> BeautifulSoup:
        text = self.get_text(url, params=params, headers=headers, retries=retries)
        return BeautifulSoup(text, parser)

    def absolute_url(self, base_url: str, path_or_url: Optional[str]) -> Optional[str]:
        if not path_or_url:
            return None
        return urljoin(base_url, path_or_url)

    def note_failure(self, key: str, exc: Exception) -> None:
        self.stats.failed_count += 1
        self.stats.errors.append(f"[{self.source.source_code}] {key}: {exc}")

    def checkpoint_with_last_key(self, checkpoint: Dict[str, Any], source_dataset_key: str) -> Dict[str, Any]:
        merged = dict(checkpoint)
        merged["last_saved_source_dataset_key"] = source_dataset_key
        return compact_dict(merged)

    def run(self, *, resume: bool = True, limit: Optional[int] = None) -> HarvestStats:
        run_info = self.db.start_run(self.source, self.settings.parser_version, resume=resume)
        checkpoint: Dict[str, Any] = dict(run_info.checkpoint_json or {})
        current_checkpoint: Dict[str, Any] = dict(checkpoint)
        success = False

        try:
            for record, next_checkpoint in self.iter_records(checkpoint):
                if limit is not None and self.stats.upserted_count >= limit:
                    break

                self.stats.collected_count += 1
                try:
                    self.db.upsert_dataset(run_info.source_id, run_info.run_id, record)
                    self.stats.upserted_count += 1
                    self.stats.last_saved_source_dataset_key = record.source_dataset_key
                    current_checkpoint = self.checkpoint_with_last_key(next_checkpoint, record.source_dataset_key)
                except Exception as exc:  # pragma: no cover - DB/형변환 실환경 예외 대응
                    self.db.rollback()
                    self.note_failure(record.source_dataset_key, exc)
                    continue

                if self.stats.collected_count % self.settings.save_every == 0:
                    self.db.update_run_progress(
                        run_info.run_id,
                        self.stats,
                        checkpoint_json=current_checkpoint,
                        status="RUNNING",
                        error_summary=self.stats.to_error_summary(),
                    )

                if self.batch_pause_every > 0 and self.stats.upserted_count % self.batch_pause_every == 0:
                    if self.batch_pause_seconds > 0:
                        self.sleep(self.batch_pause_seconds)

            success = True
            final_status = "PARTIAL_SUCCESS" if self.stats.failed_count else "SUCCESS"
            self.db.finalize_run(
                run_info.run_id,
                final_status,
                self.stats,
                checkpoint_json=current_checkpoint,
                error_summary=self.stats.to_error_summary(),
            )
            return self.stats
        except KeyboardInterrupt as exc:  # pragma: no cover - 인터럽트 대응
            self.stats.errors.append(f"[{self.source.source_code}] stopped by user: {exc}")
            self.db.finalize_run(
                run_info.run_id,
                "STOPPED",
                self.stats,
                checkpoint_json=current_checkpoint,
                error_summary=self.stats.to_error_summary(),
            )
            raise
        except Exception as exc:
            self.stats.errors.append(f"[{self.source.source_code}] fatal: {exc}")
            self.db.finalize_run(
                run_info.run_id,
                "FAILED",
                self.stats,
                checkpoint_json=current_checkpoint,
                error_summary=self.stats.to_error_summary(),
            )
            raise
        finally:
            if not success and self.db.conn is not None:
                self.db.commit()
            self.close()

    @abstractmethod
    def iter_records(self, checkpoint: Dict[str, Any]) -> Iterator[Tuple[NormalizedDatasetRecord, Dict[str, Any]]]:
        raise NotImplementedError

    def first_text(self, data: Any, *paths: Any) -> Optional[str]:
        for path in paths:
            if not isinstance(path, tuple):
                path = (path,)
            value = safe_get(data, *path)
            text = clean_text(value) if value is not None else None
            if text:
                return text
        return None

    def as_pretty_json(self, value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, indent=2, default=str)
