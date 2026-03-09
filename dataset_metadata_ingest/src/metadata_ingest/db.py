from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional, Sequence

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .models import CollectionRunInfo, HarvestStats, NormalizedDatasetRecord, SourceDefinition
from .utils import build_field_presence, build_search_text, compact_dict, infer_commercial_use_from_license, sha256_json, unique_strings


def _coerce_datetime(value: Any) -> Any:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return value
    return value


_DATASET_COLUMNS: Sequence[str] = (
    "dataset_source_id",
    "last_ingest_run_id",
    "source_dataset_key",
    "record_hash",
    "canonical_url",
    "landing_url",
    "title",
    "subtitle",
    "description_short",
    "description_long",
    "search_text",
    "publisher_name",
    "domains",
    "tasks",
    "modalities",
    "tags",
    "languages",
    "license_name",
    "license_url",
    "commercial_use_allowed",
    "access_type",
    "login_required",
    "approval_required",
    "payment_required",
    "is_restricted",
    "source_created_at",
    "source_updated_at",
    "source_version",
    "row_count",
    "dataset_size_bytes",
    "field_presence_json",
    "creators_json",
    "resources_json",
    "schema_json",
    "metrics_json",
    "extra_json",
    "raw_json",
    "status",
    "last_ingested_at",
)


def _normalize_term_list(values: Iterable[Any], *, max_items: int = 64, max_len: int = 200) -> list[str]:
    normalized: list[str] = []
    for item in unique_strings(values):
        value = item.strip()
        if not value:
            continue
        if len(value) > max_len:
            value = value[:max_len]
        normalized.append(value)
        if len(normalized) >= max_items:
            break
    return normalized


class Database:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.conn: Optional[psycopg.Connection[Any]] = None

    def __enter__(self) -> "Database":
        self.conn = psycopg.connect(self.dsn, row_factory=dict_row)
        self.conn.autocommit = False
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.conn is None:
            return
        try:
            if exc is None:
                self.conn.commit()
            else:
                self.conn.rollback()
        finally:
            self.conn.close()
            self.conn = None

    def _cursor(self):
        if self.conn is None:
            raise RuntimeError("Database connection is not initialized")
        return self.conn.cursor()

    def commit(self) -> None:
        if self.conn is not None:
            self.conn.commit()

    def rollback(self) -> None:
        if self.conn is not None:
            self.conn.rollback()

    def ensure_dataset_source(self, source: SourceDefinition) -> int:
        sql = """
        INSERT INTO dataset_source (
            source_code,
            source_name,
            base_url,
            collection_type,
            is_active
        )
        VALUES (%s, %s, %s, %s, TRUE)
        ON CONFLICT (source_code)
        DO UPDATE SET
            source_name = EXCLUDED.source_name,
            base_url = EXCLUDED.base_url,
            collection_type = EXCLUDED.collection_type,
            is_active = TRUE
        RETURNING id
        """
        with self._cursor() as cur:
            cur.execute(
                sql,
                (
                    source.source_code,
                    source.source_name,
                    source.base_url,
                    source.collection_type,
                ),
            )
            row = cur.fetchone()
        self.commit()
        assert row is not None
        return int(row["id"])

    def start_run(self, source: SourceDefinition, parser_version: str, resume: bool = True) -> CollectionRunInfo:
        source_id = self.ensure_dataset_source(source)
        checkpoint: Dict[str, Any] = {}
        with self._cursor() as cur:
            cur.execute(
                """
                UPDATE collection_dataset
                SET status = 'STOPPED',
                    run_finished_at = NOW(),
                    error_summary = CASE
                        WHEN error_summary IS NULL OR error_summary = ''
                            THEN '[SYSTEM] previous run was not finalized (process interrupted)'
                        ELSE error_summary || E'\n[SYSTEM] previous run was not finalized (process interrupted)'
                    END
                WHERE dataset_source_id = %s
                  AND status = 'RUNNING'
                """,
                (source_id,),
            )
        self.commit()

        if resume:
            with self._cursor() as cur:
                cur.execute(
                    """
                    SELECT checkpoint_json
                    FROM collection_dataset
                    WHERE dataset_source_id = %s
                      AND status IN ('RUNNING', 'FAILED', 'STOPPED', 'PARTIAL_SUCCESS', 'SUCCESS')
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (source_id,),
                )
                row = cur.fetchone()
                if row and row.get("checkpoint_json"):
                    checkpoint = row["checkpoint_json"]

        with self._cursor() as cur:
            cur.execute(
                """
                INSERT INTO collection_dataset (
                    dataset_source_id,
                    parser_version,
                    status,
                    checkpoint_json
                )
                VALUES (%s, %s, 'RUNNING', %s)
                RETURNING id
                """,
                (source_id, parser_version, Jsonb(checkpoint)),
            )
            row = cur.fetchone()
        self.commit()
        assert row is not None
        return CollectionRunInfo(run_id=int(row["id"]), source_id=source_id, checkpoint_json=checkpoint)

    def update_run_progress(
        self,
        run_id: int,
        stats: HarvestStats,
        checkpoint_json: Optional[Dict[str, Any]] = None,
        status: Optional[str] = None,
        error_summary: Optional[str] = None,
    ) -> None:
        sets = [
            "collected_count = %s",
            "upserted_count = %s",
            "failed_count = %s",
            "last_saved_source_dataset_key = %s",
        ]
        params: list[Any] = [
            stats.collected_count,
            stats.upserted_count,
            stats.failed_count,
            stats.last_saved_source_dataset_key,
        ]
        if checkpoint_json is not None:
            sets.append("checkpoint_json = %s")
            params.append(Jsonb(checkpoint_json))
        if status is not None:
            sets.append("status = %s")
            params.append(status)
        if error_summary is not None:
            sets.append("error_summary = %s")
            params.append(error_summary)

        params.append(run_id)
        sql = f"UPDATE collection_dataset SET {', '.join(sets)} WHERE id = %s"
        with self._cursor() as cur:
            cur.execute(sql, params)
        self.commit()

    def finalize_run(
        self,
        run_id: int,
        status: str,
        stats: HarvestStats,
        checkpoint_json: Optional[Dict[str, Any]] = None,
        error_summary: Optional[str] = None,
    ) -> None:
        checkpoint_json = checkpoint_json or {}
        with self._cursor() as cur:
            cur.execute(
                """
                UPDATE collection_dataset
                SET status = %s,
                    run_finished_at = NOW(),
                    collected_count = %s,
                    upserted_count = %s,
                    failed_count = %s,
                    error_summary = %s,
                    last_saved_source_dataset_key = %s,
                    checkpoint_json = %s
                WHERE id = %s
                """,
                (
                    status,
                    stats.collected_count,
                    stats.upserted_count,
                    stats.failed_count,
                    error_summary,
                    stats.last_saved_source_dataset_key,
                    Jsonb(checkpoint_json),
                    run_id,
                ),
            )
        self.commit()

    def upsert_dataset(self, source_id: int, run_id: int, record: NormalizedDatasetRecord) -> None:
        payload = self._record_to_db_payload(source_id=source_id, run_id=run_id, record=record)
        placeholders = ", ".join(["%s"] * len(_DATASET_COLUMNS))
        columns = ", ".join(_DATASET_COLUMNS)
        updates = ",\n            ".join(
            f"{column} = EXCLUDED.{column}"
            for column in _DATASET_COLUMNS
            if column not in {"dataset_source_id", "source_dataset_key"}
        )

        sql = f"""
        INSERT INTO dataset (
            {columns}
        )
        VALUES ({placeholders})
        ON CONFLICT (dataset_source_id, source_dataset_key)
        DO UPDATE SET
            {updates},
            updated_at = NOW()
        """

        params = [payload[column] for column in _DATASET_COLUMNS]
        with self._cursor() as cur:
            cur.execute(sql, params)
        self.commit()

    def _record_to_db_payload(self, source_id: int, run_id: int, record: NormalizedDatasetRecord) -> Dict[str, Any]:
        # 배열/검색텍스트/field presence/rhash 등 누락 보정
        record.domains = _normalize_term_list(record.domains, max_items=40, max_len=120)
        record.tasks = _normalize_term_list(record.tasks, max_items=64, max_len=160)
        record.modalities = _normalize_term_list(record.modalities, max_items=20, max_len=80)
        record.tags = _normalize_term_list(record.tags, max_items=80, max_len=200)
        record.languages = _normalize_term_list(record.languages, max_items=16, max_len=40)

        if record.search_text is None:
            record.search_text = build_search_text(
                record.title,
                record.subtitle,
                record.description_short,
                record.description_long,
                record.publisher_name,
                record.domains,
                record.tasks,
                record.modalities,
                record.tags,
                record.languages,
            )

        if record.commercial_use_allowed is None:
            record.commercial_use_allowed = infer_commercial_use_from_license(
                record.license_name,
                record.license_url,
            )

        compact_view = compact_dict(
            {
                **asdict(record),
                "field_presence_json": None,
            }
        )

        if not record.record_hash:
            record.record_hash = sha256_json(compact_view)

        if not record.field_presence_json:
            record.field_presence_json = build_field_presence(compact_view)

        if not record.status:
            record.status = "ACTIVE"

        payload: Dict[str, Any] = {
            "dataset_source_id": source_id,
            "last_ingest_run_id": run_id,
            "source_dataset_key": record.source_dataset_key,
            "record_hash": record.record_hash,
            "canonical_url": record.canonical_url,
            "landing_url": record.landing_url,
            "title": record.title,
            "subtitle": record.subtitle,
            "description_short": record.description_short,
            "description_long": record.description_long,
            "search_text": record.search_text,
            "publisher_name": record.publisher_name,
            "domains": record.domains,
            "tasks": record.tasks,
            "modalities": record.modalities,
            "tags": record.tags,
            "languages": record.languages,
            "license_name": record.license_name,
            "license_url": record.license_url,
            "commercial_use_allowed": record.commercial_use_allowed,
            "access_type": record.access_type,
            "login_required": record.login_required,
            "approval_required": record.approval_required,
            "payment_required": record.payment_required,
            "is_restricted": record.is_restricted,
            "source_created_at": _coerce_datetime(record.source_created_at),
            "source_updated_at": _coerce_datetime(record.source_updated_at),
            "source_version": record.source_version,
            "row_count": record.row_count,
            "dataset_size_bytes": record.dataset_size_bytes,
            "field_presence_json": Jsonb(record.field_presence_json),
            "creators_json": Jsonb(record.creators_json),
            "resources_json": Jsonb(record.resources_json),
            "schema_json": Jsonb(record.schema_json),
            "metrics_json": Jsonb(record.metrics_json),
            "extra_json": Jsonb(record.extra_json),
            "raw_json": Jsonb(record.raw_json),
            "status": record.status,
            "last_ingested_at": datetime.now(timezone.utc),
        }
        return payload
