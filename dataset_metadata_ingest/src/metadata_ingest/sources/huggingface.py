from __future__ import annotations

import importlib
from typing import Any, Dict, Iterator, List, Optional, Tuple

from ..base import BaseDatasetCollector, ResumeGate
from ..models import NormalizedDatasetRecord, SourceDefinition
from ..utils import (
    clean_text,
    domains_from_urls,
    ensure_list,
    guess_modalities_from_text,
    guess_tasks_from_tags,
    parse_bytes,
    parse_datetime,
    parse_int,
    safe_get,
    to_serializable,
    unique_strings,
)


SOURCE = SourceDefinition(
    source_code="HUGGINGFACE",
    source_name="Hugging Face",
    base_url="https://huggingface.co",
    collection_type="API",
)


class HuggingFaceCollector(BaseDatasetCollector):
    source = SOURCE

    def iter_records(
        self, checkpoint: Dict[str, Any]
    ) -> Iterator[Tuple[NormalizedDatasetRecord, Dict[str, Any]]]:
        try:
            hub_module = importlib.import_module("huggingface_hub")
            hf_api_cls = getattr(hub_module, "HfApi")
        except Exception as exc:
            raise ImportError(
                "huggingface_hub 패키지가 설치되어 있지 않습니다. requirements.txt 기준으로 설치하세요."
            ) from exc

        api = hf_api_cls(token=self.settings.huggingface_token or None)
        resume_gate = ResumeGate(checkpoint.get("last_saved_source_dataset_key"))

        iterable = api.list_datasets(full=True, sort="last_modified")
        for info in iterable:
            self._pace_before_request()
            raw = to_serializable(info)
            source_key = clean_text(raw.get("id"))
            if not source_key:
                continue
            if not resume_gate.allow(source_key):
                continue

            try:
                yield (
                    self._normalize(raw),
                    {"sort": "last_modified", "direction": "desc"},
                )
            except Exception as exc:
                self.note_failure(source_key, exc)
                continue

    def _normalize(self, raw: Dict[str, Any]) -> NormalizedDatasetRecord:
        source_key = clean_text(raw.get("id")) or ""
        dataset_url = f"https://huggingface.co/datasets/{source_key}"
        tags = unique_strings(raw.get("tags") or [])
        card = raw.get("cardData") or raw.get("card_data") or {}

        languages = self._extract_prefixed_tags(tags, "language:")
        tasks = unique_strings(
            self._extract_prefixed_tags(tags, "task_categories:")
            + self._extract_prefixed_tags(tags, "task_ids:")
            + ensure_list(card.get("task_categories"))
            + ensure_list(card.get("task_ids"))
        )
        license_name = (
            clean_text(card.get("license"))
            or self._first_prefixed_tag(tags, "license:")
            or clean_text(raw.get("license"))
        )

        dataset_size_bytes = self._extract_dataset_size_bytes(raw, card)
        row_count = self._extract_row_count(raw, card)
        resource_files = self._build_resources(raw)
        domains = unique_strings(
            self._extract_prefixed_tags(tags, "domain:")
            + self._extract_prefixed_tags(tags, "benchmark:")
            + self._extract_prefixed_tags(tags, "library:")
        )

        creators = []
        author = clean_text(raw.get("author"))
        if author:
            creators.append({"name": author, "role": "author"})
        for item in ensure_list(card.get("authors")):
            if isinstance(item, dict):
                name = clean_text(item.get("name") or item.get("author"))
                if name:
                    creators.append(item)
            else:
                name = clean_text(item)
                if name:
                    creators.append({"name": name, "role": "author"})

        access_type = "OPEN"
        login_required = False
        approval_required = False
        is_restricted = False
        if raw.get("gated"):
            access_type = "APPROVAL"
            login_required = True
            approval_required = True
            is_restricted = True
        elif raw.get("private"):
            access_type = "REGISTERED"
            login_required = True
            approval_required = False
            is_restricted = True

        description_short = clean_text(raw.get("description"))
        description_long = (
            clean_text(card.get("pretty_name"))
            if description_short is None and card.get("pretty_name")
            else description_short
        )
        if card.get("dataset_summary"):
            description_long = clean_text(card.get("dataset_summary"))
        elif card.get("description"):
            description_long = clean_text(card.get("description"))
        elif description_short:
            description_long = description_short
        if description_short is None and description_long is not None:
            description_short = description_long

        title = clean_text(card.get("pretty_name")) or source_key.split("/")[-1]
        subtitle = source_key if title != source_key else None

        modalities = unique_strings(
            self._extract_prefixed_tags(tags, "modality:")
            + guess_modalities_from_text(tags, description_short, description_long)
        )

        publisher_name = author or clean_text(safe_get(card, "publisher", "name"))
        raw_domains = domains_from_urls(
            [safe_get(raw, "cardData", "homepage"), safe_get(card, "homepage")]
        )

        return NormalizedDatasetRecord(
            source_dataset_key=source_key,
            canonical_url=dataset_url,
            landing_url=dataset_url,
            title=title,
            subtitle=subtitle,
            description_short=description_short,
            description_long=description_long,
            publisher_name=publisher_name,
            domains=unique_strings(domains + raw_domains),
            tasks=tasks or guess_tasks_from_tags(tags),
            modalities=modalities,
            tags=tags,
            languages=languages,
            license_name=license_name,
            license_url=clean_text(card.get("license_link")),
            access_type=access_type,
            login_required=login_required,
            approval_required=approval_required,
            payment_required=False,
            is_restricted=is_restricted,
            source_created_at=parse_datetime(
                raw.get("createdAt") or raw.get("created_at")
            ),
            source_updated_at=parse_datetime(
                raw.get("lastModified") or raw.get("last_modified")
            ),
            source_version=clean_text(raw.get("sha") or raw.get("lastModified")),
            row_count=row_count,
            dataset_size_bytes=dataset_size_bytes,
            creators_json=creators,
            resources_json=resource_files,
            schema_json={
                "features": card.get("dataset_info", {}).get("features")
                if isinstance(card.get("dataset_info"), dict)
                else safe_get(card, "features"),
                "splits": card.get("dataset_info", {}).get("splits")
                if isinstance(card.get("dataset_info"), dict)
                else None,
            },
            metrics_json={
                "downloads": parse_int(raw.get("downloads")),
                "downloads_all_time": parse_int(
                    raw.get("downloadsAllTime") or raw.get("downloads_all_time")
                ),
                "likes": parse_int(raw.get("likes")),
            },
            extra_json={
                "gated": raw.get("gated"),
                "private": raw.get("private"),
                "trending_score": raw.get("trendingScore"),
                "siblings_count": len(resource_files),
                "card_data": card,
            },
            raw_json=raw,
        )

    def _extract_prefixed_tags(self, tags: List[str], prefix: str) -> List[str]:
        result: List[str] = []
        prefix_cf = prefix.casefold()
        for tag in tags:
            if tag.casefold().startswith(prefix_cf):
                value = clean_text(tag[len(prefix) :])
                if value:
                    result.append(value)
        return unique_strings(result)

    def _first_prefixed_tag(self, tags: List[str], prefix: str) -> Optional[str]:
        values = self._extract_prefixed_tags(tags, prefix)
        return values[0] if values else None

    def _extract_dataset_size_bytes(
        self, raw: Dict[str, Any], card: Dict[str, Any]
    ) -> Optional[int]:
        candidates = [
            safe_get(raw, "usedStorage"),
            safe_get(raw, "used_storage"),
            safe_get(card, "dataset_info", "dataset_size"),
            safe_get(card, "dataset_size"),
            safe_get(card, "size_in_bytes"),
        ]
        for candidate in candidates:
            value = parse_bytes(candidate)
            if value is not None:
                return value

        total = 0
        matched = False
        for sibling in raw.get("siblings") or []:
            if not isinstance(sibling, dict):
                continue
            for key in ("size", "size_in_bytes", "lfs", "blob_size"):
                candidate = sibling.get(key)
                if isinstance(candidate, dict):
                    candidate = candidate.get("size")
                value = parse_bytes(candidate)
                if value is not None:
                    total += value
                    matched = True
                    break
        return total if matched else None

    def _extract_row_count(
        self, raw: Dict[str, Any], card: Dict[str, Any]
    ) -> Optional[int]:
        candidates = [
            safe_get(card, "dataset_info", "dataset_num_rows"),
            safe_get(card, "dataset_num_rows"),
        ]
        for candidate in candidates:
            value = parse_int(candidate)
            if value is not None:
                return value

        dataset_info = safe_get(card, "dataset_info")
        if isinstance(dataset_info, dict):
            total = 0
            found = False
            for split in ensure_list(dataset_info.get("splits")):
                if not isinstance(split, dict):
                    continue
                value = parse_int(
                    split.get("num_examples") or split.get("dataset_num_rows")
                )
                if value is not None:
                    total += value
                    found = True
            if found:
                return total
        return None

    def _build_resources(self, raw: Dict[str, Any]) -> List[Dict[str, Any]]:
        resources: List[Dict[str, Any]] = []
        for sibling in raw.get("siblings") or []:
            if isinstance(sibling, str):
                resources.append({"path": sibling})
                continue
            if not isinstance(sibling, dict):
                continue
            resource = {
                "path": clean_text(
                    sibling.get("rfilename")
                    or sibling.get("path")
                    or sibling.get("name")
                ),
                "size_bytes": parse_bytes(
                    sibling.get("size") or sibling.get("size_in_bytes")
                ),
                "lfs": sibling.get("lfs"),
            }
            if resource["path"]:
                resources.append(resource)
        return resources
