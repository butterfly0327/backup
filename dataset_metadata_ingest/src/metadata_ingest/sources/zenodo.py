from __future__ import annotations

from typing import Any, Dict, Iterator, List, Tuple

from ..base import BaseDatasetCollector, ResumeGate
from ..models import NormalizedDatasetRecord, SourceDefinition
from ..utils import (
    clean_text,
    domains_from_urls,
    ensure_list,
    guess_modalities_from_text,
    parse_bytes,
    parse_datetime,
    parse_int,
    safe_get,
    unique_strings,
)


SOURCE = SourceDefinition(
    source_code="ZENODO",
    source_name="Zenodo",
    base_url="https://zenodo.org/api",
    collection_type="API",
)


class ZenodoCollector(BaseDatasetCollector):
    source = SOURCE
    PAGE_SIZE = 25

    def iter_records(self, checkpoint: Dict[str, Any]) -> Iterator[Tuple[NormalizedDatasetRecord, Dict[str, Any]]]:
        page = max(parse_int(checkpoint.get("page")) or 1, 1)
        start_page = page
        resume_gate = ResumeGate(checkpoint.get("last_saved_source_dataset_key"))

        while True:
            payload = self._fetch_records_page(page)
            hits = safe_get(payload, "hits", "hits") or payload.get("hits") or []
            if not isinstance(hits, list) or not hits:
                break

            for item in hits:
                source_key = str(item.get("id") or item.get("record_id") or "").strip()
                if not source_key:
                    continue
                if page == start_page and not resume_gate.allow(source_key):
                    continue
                try:
                    yield self._normalize(item), {"page": page}
                except Exception as exc:
                    self.note_failure(source_key, exc)
                    continue

            page += 1

    def _fetch_records_page(self, page: int) -> Dict[str, Any]:
        endpoint = f"{self.source.base_url}/records"
        payload = self.get_json(
            endpoint,
            params={
                "page": page,
                "size": self.PAGE_SIZE,
                "sort": "newest",
                "q": "resource_type.type:dataset",
            },
        )
        if isinstance(payload, dict):
            return payload
        return {}

    def _normalize(self, raw: Dict[str, Any]) -> NormalizedDatasetRecord:
        metadata = raw.get("metadata") or {}
        source_key = str(raw.get("id") or raw.get("record_id") or "").strip()
        landing_url = clean_text(safe_get(raw, "links", "html") or raw.get("doi_url") or safe_get(raw, "links", "self_html"))
        title = clean_text(metadata.get("title")) or source_key
        description = clean_text(metadata.get("description"))

        creators = []
        for creator in metadata.get("creators") or []:
            if not isinstance(creator, dict):
                continue
            creators.append(
                {
                    "name": clean_text(creator.get("name")),
                    "affiliation": clean_text(creator.get("affiliation")),
                    "orcid": clean_text(creator.get("orcid")),
                    "gnd": clean_text(creator.get("gnd")),
                    "role": "creator",
                }
            )
        for contributor in metadata.get("contributors") or []:
            if not isinstance(contributor, dict):
                continue
            creators.append(
                {
                    "name": clean_text(contributor.get("name")),
                    "affiliation": clean_text(contributor.get("affiliation")),
                    "orcid": clean_text(contributor.get("orcid")),
                    "type": clean_text(contributor.get("type")),
                    "role": "contributor",
                }
            )

        resources: List[Dict[str, Any]] = []
        total_size = 0
        for file in raw.get("files") or []:
            if not isinstance(file, dict):
                continue
            size_bytes = parse_bytes(file.get("size") or safe_get(file, "checksum"))
            if size_bytes is None:
                size_bytes = parse_int(file.get("size"))
            if size_bytes:
                total_size += size_bytes
            resources.append(
                {
                    "filename": clean_text(file.get("key") or file.get("filename")),
                    "download_url": clean_text(safe_get(file, "links", "self") or safe_get(file, "links", "download")),
                    "mime_type": clean_text(file.get("mimetype") or file.get("type")),
                    "size_bytes": size_bytes,
                    "checksum": clean_text(file.get("checksum")),
                    "created_at": parse_datetime(file.get("created")),
                    "updated_at": parse_datetime(file.get("updated")),
                }
            )

        keywords = unique_strings(metadata.get("keywords") or [])
        communities = unique_strings(
            community.get("identifier") if isinstance(community, dict) else community
            for community in metadata.get("communities") or []
        )
        subjects = unique_strings(
            subject.get("term") if isinstance(subject, dict) else subject
            for subject in metadata.get("subjects") or []
        )
        tags = unique_strings(keywords + communities + subjects)

        access_right = clean_text(metadata.get("access_right") or metadata.get("accessRight") or raw.get("access_right"))
        access_type = "OPEN"
        login_required = False
        approval_required = False
        payment_required = False
        is_restricted = False
        if access_right:
            lowered = access_right.casefold()
            if lowered in {"open"}:
                access_type = "OPEN"
            elif lowered in {"embargoed"}:
                access_type = "REGISTERED"
                login_required = True
                is_restricted = True
            elif lowered in {"restricted", "closed"}:
                access_type = "RESTRICTED"
                login_required = True
                approval_required = True
                is_restricted = True

        publisher_name = clean_text(metadata.get("publisher")) or (creators[0].get("name") if creators else None)

        return NormalizedDatasetRecord(
            source_dataset_key=source_key,
            canonical_url=landing_url,
            landing_url=landing_url,
            title=title,
            description_short=description,
            description_long=description,
            publisher_name=publisher_name,
            domains=domains_from_urls([landing_url] + [r.get("download_url") for r in resources]),
            tasks=keywords,
            modalities=guess_modalities_from_text(title, description, tags, [r.get("filename") for r in resources]),
            tags=tags,
            languages=unique_strings([metadata.get("language")]),
            license_name=clean_text(safe_get(metadata, "license", "id") or (metadata.get("license") if isinstance(metadata.get("license"), str) else None)),
            license_url=clean_text(safe_get(raw, "links", "license") or safe_get(metadata, "license", "url")),
            access_type=access_type,
            login_required=login_required,
            approval_required=approval_required,
            payment_required=payment_required,
            is_restricted=is_restricted,
            source_created_at=parse_datetime(raw.get("created") or metadata.get("publication_date")),
            source_updated_at=parse_datetime(raw.get("updated")),
            source_version=clean_text(metadata.get("version") or raw.get("revision")),
            row_count=parse_int(metadata.get("num_rows") or metadata.get("record_count")),
            dataset_size_bytes=total_size or parse_int(raw.get("size")),
            creators_json=creators,
            resources_json=resources,
            schema_json={
                "resource_type": metadata.get("resource_type"),
                "related_identifiers": metadata.get("related_identifiers"),
            },
            metrics_json={
                "downloads": parse_int(safe_get(raw, "stats", "downloads")),
                "views": parse_int(safe_get(raw, "stats", "views")),
                "versions": parse_int(safe_get(raw, "stats", "versions")),
            },
            extra_json={
                "doi": clean_text(raw.get("doi") or metadata.get("doi")),
                "conceptdoi": clean_text(raw.get("conceptdoi") or metadata.get("conceptdoi")),
                "access_right": access_right,
                "communities": metadata.get("communities"),
                "grants": metadata.get("grants"),
                "subjects": metadata.get("subjects"),
            },
            raw_json=raw,
        )
