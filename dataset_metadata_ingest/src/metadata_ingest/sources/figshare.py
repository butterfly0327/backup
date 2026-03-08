from __future__ import annotations

from typing import Any, Dict, Iterator, List, Tuple

from ..base import BaseDatasetCollector, ResumeGate
from ..models import NormalizedDatasetRecord, SourceDefinition
from ..utils import (
    clean_text,
    domains_from_urls,
    guess_modalities_from_text,
    parse_bytes,
    parse_datetime,
    parse_int,
    safe_get,
    unique_strings,
)


SOURCE = SourceDefinition(
    source_code="FIGSHARE",
    source_name="Figshare",
    base_url="https://api.figshare.com/v2",
    collection_type="API",
)


class FigshareCollector(BaseDatasetCollector):
    source = SOURCE
    PAGE_SIZE = 100

    def iter_records(self, checkpoint: Dict[str, Any]) -> Iterator[Tuple[NormalizedDatasetRecord, Dict[str, Any]]]:
        page = max(parse_int(checkpoint.get("page")) or 1, 1)
        start_page = page
        resume_gate = ResumeGate(checkpoint.get("last_saved_source_dataset_key"))

        while True:
            items = self.get_json(
                f"{self.source.base_url}/articles",
                params={
                    "page": page,
                    "page_size": self.PAGE_SIZE,
                    "item_type": 3,
                    "order": "published_date",
                    "order_direction": "desc",
                },
            )
            if not isinstance(items, list) or not items:
                break

            for item in items:
                source_key = str(item.get("id") or "").strip()
                if not source_key:
                    continue
                if page == start_page and not resume_gate.allow(source_key):
                    continue

                try:
                    detail = self.get_json(f"{self.source.base_url}/articles/{source_key}")
                    yield self._normalize(detail), {"page": page}
                except Exception as exc:
                    self.note_failure(source_key, exc)
                    continue

            page += 1

    def _normalize(self, raw: Dict[str, Any]) -> NormalizedDatasetRecord:
        source_key = str(raw.get("id") or "").strip()
        landing_url = clean_text(raw.get("url_public_html") or raw.get("figshare_url") or raw.get("url"))
        title = clean_text(raw.get("title")) or source_key
        description = clean_text(raw.get("description"))
        license_obj = raw.get("license") if isinstance(raw.get("license"), dict) else {}

        creators: List[Dict[str, Any]] = []
        for author in raw.get("authors") or []:
            if not isinstance(author, dict):
                continue
            creators.append(
                {
                    "name": clean_text(author.get("full_name") or author.get("name")),
                    "orcid_id": clean_text(author.get("orcid_id")),
                    "url": clean_text(author.get("url_name") or author.get("url")),
                    "role": "author",
                }
            )

        resources: List[Dict[str, Any]] = []
        total_size = 0
        for file in raw.get("files") or []:
            if not isinstance(file, dict):
                continue
            size_bytes = parse_bytes(file.get("size"))
            if size_bytes:
                total_size += size_bytes
            resources.append(
                {
                    "id": file.get("id"),
                    "name": clean_text(file.get("name") or file.get("supplied_filename")),
                    "download_url": clean_text(file.get("download_url")),
                    "mime_type": clean_text(file.get("mime_type")),
                    "size_bytes": size_bytes,
                    "is_link_only": False,
                }
            )
        dataset_size_bytes = total_size or parse_bytes(raw.get("size"))

        categories = unique_strings(
            cat.get("title") if isinstance(cat, dict) else cat for cat in raw.get("categories") or []
        )
        keywords = unique_strings(raw.get("tags") or raw.get("keywords") or [])
        tags = unique_strings(keywords + categories)

        publisher_name = (
            clean_text(raw.get("group_title"))
            or clean_text(raw.get("institution_name"))
            or clean_text(safe_get(raw, "owner", "full_name"))
            or (creators[0].get("name") if creators else None)
        )

        is_public = raw.get("is_public")
        access_type = "OPEN" if is_public is not False else "RESTRICTED"
        is_restricted = False if is_public is not False else True

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
            modalities=guess_modalities_from_text(title, description, tags, [r.get("name") for r in resources]),
            tags=tags,
            languages=unique_strings([raw.get("language")]),
            license_name=clean_text(license_obj.get("name") or raw.get("license_name")),
            license_url=clean_text(license_obj.get("url") or raw.get("license_url")),
            access_type=access_type,
            login_required=False,
            approval_required=False,
            payment_required=False,
            is_restricted=is_restricted,
            source_created_at=parse_datetime(raw.get("published_date") or raw.get("posted_date")),
            source_updated_at=parse_datetime(raw.get("modified_date")),
            source_version=clean_text(raw.get("version") or raw.get("version_id") or raw.get("resource_doi")),
            row_count=parse_int(raw.get("row_count")),
            dataset_size_bytes=dataset_size_bytes,
            creators_json=creators,
            resources_json=resources,
            schema_json={
                "custom_fields": raw.get("custom_fields"),
                "defined_type_name": raw.get("defined_type_name"),
            },
            metrics_json={
                "views": parse_int(raw.get("views") or raw.get("views_count")),
                "downloads": parse_int(raw.get("downloads") or raw.get("downloads_count")),
                "shares": parse_int(raw.get("shares") or raw.get("shares_count")),
                "citations": parse_int(raw.get("cites") or raw.get("citation_count")),
            },
            extra_json={
                "doi": clean_text(raw.get("doi") or raw.get("resource_doi")),
                "embargo_date": parse_datetime(raw.get("embargo_date")),
                "funding": raw.get("funding"),
                "references": raw.get("references"),
                "categories": raw.get("categories"),
                "license": raw.get("license"),
            },
            raw_json=raw,
        )
