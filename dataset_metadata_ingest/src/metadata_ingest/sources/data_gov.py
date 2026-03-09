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
    unique_strings,
)


SOURCE = SourceDefinition(
    source_code="DATA_GOV",
    source_name="data.gov catalog",
    base_url="https://api.gsa.gov/technology/datagov/v3",
    collection_type="API",
)


class DataGovCollector(BaseDatasetCollector):
    source = SOURCE
    PAGE_SIZE = 500

    def iter_records(
        self, checkpoint: Dict[str, Any]
    ) -> Iterator[Tuple[NormalizedDatasetRecord, Dict[str, Any]]]:
        if not self.settings.data_gov_api_key:
            raise ValueError("DATA_GOV_API_KEY 환경변수가 필요합니다.")

        start = max(parse_int(checkpoint.get("start")) or 0, 0)
        start_offset = start
        resume_gate = ResumeGate(checkpoint.get("last_saved_source_dataset_key"))
        headers = {"x-api-key": self.settings.data_gov_api_key}

        while True:
            payload = self.get_json(
                f"{self.source.base_url}/action/package_search",
                params={"q": "*:*", "rows": self.PAGE_SIZE, "start": start},
                headers=headers,
            )
            result = payload.get("result") or {}
            items = result.get("results") or []
            if not items:
                break

            for item in items:
                source_key = clean_text(item.get("id") or item.get("name"))
                if not source_key:
                    continue
                if start == start_offset and not resume_gate.allow(source_key):
                    continue
                try:
                    yield self._normalize(item), {"start": start}
                except Exception as exc:
                    self.note_failure(source_key, exc)
                    continue

            start += self.PAGE_SIZE

    def _normalize(self, raw: Dict[str, Any]) -> NormalizedDatasetRecord:
        source_key = clean_text(raw.get("id") or raw.get("name")) or ""
        landing_url = clean_text(
            raw.get("url")
            or raw.get("landingPage")
            or f"https://catalog.data.gov/dataset/{raw.get('name')}"
        )
        title = (
            clean_text(raw.get("title")) or clean_text(raw.get("name")) or source_key
        )
        notes = clean_text(raw.get("notes"))
        extras_map = self._extras_map(raw.get("extras") or [])

        tags = unique_strings(
            [
                tag.get("display_name") if isinstance(tag, dict) else tag
                for tag in raw.get("tags") or []
            ]
            + [
                group.get("display_name") if isinstance(group, dict) else group
                for group in raw.get("groups") or []
            ]
            + self._split_keywords(extras_map.get("keyword"))
            + self._split_keywords(extras_map.get("theme"))
        )
        languages = unique_strings(
            self._split_keywords(extras_map.get("language"))
            + self._split_keywords(raw.get("language"))
        )

        resources: List[Dict[str, Any]] = []
        total_size = 0
        for resource in raw.get("resources") or []:
            if not isinstance(resource, dict):
                continue
            size_bytes = parse_bytes(
                resource.get("size")
                or resource.get("resource_size")
                or resource.get("package_size")
            )
            if size_bytes:
                total_size += size_bytes
            resources.append(
                {
                    "id": resource.get("id"),
                    "name": clean_text(resource.get("name")),
                    "description": clean_text(resource.get("description")),
                    "url": clean_text(resource.get("url")),
                    "format": clean_text(resource.get("format")),
                    "mime_type": clean_text(
                        resource.get("mimetype") or resource.get("mimetype_inner")
                    ),
                    "size_bytes": size_bytes,
                    "state": clean_text(resource.get("state")),
                }
            )

        access_level = clean_text(
            extras_map.get("accessLevel") or raw.get("accessLevel")
        )
        access_type = "OPEN"
        login_required = False
        approval_required = False
        payment_required = False
        is_restricted = False
        access_level_lower = access_level.casefold() if access_level else None
        is_open_flag = raw.get("isopen")
        if access_level_lower in {"public", "open"}:
            access_type = "OPEN"
            is_restricted = False
        elif access_level and access_level.casefold() not in {"public", "open"}:
            access_type = "RESTRICTED"
            is_restricted = True
        elif is_open_flag is False:
            access_type = "RESTRICTED"
            is_restricted = True

        publisher_name = (
            clean_text(self._org_title(raw.get("organization")))
            or clean_text(extras_map.get("publisher"))
            or clean_text(raw.get("author"))
            or clean_text(raw.get("maintainer"))
        )

        license_name = clean_text(
            raw.get("license_title")
            or raw.get("license_id")
            or extras_map.get("license")
        )
        license_url = clean_text(
            raw.get("license_url") or extras_map.get("license_url")
        )

        return NormalizedDatasetRecord(
            source_dataset_key=source_key,
            canonical_url=landing_url,
            landing_url=landing_url,
            title=title,
            description_short=notes,
            description_long=notes,
            publisher_name=publisher_name,
            domains=unique_strings(
                [
                    group.get("display_name") if isinstance(group, dict) else group
                    for group in raw.get("groups") or []
                ]
                + self._split_keywords(extras_map.get("theme"))
            ),
            tasks=self._split_keywords(extras_map.get("keyword")),
            modalities=guess_modalities_from_text(
                title, notes, tags, [r.get("format") for r in resources]
            ),
            tags=tags,
            languages=languages,
            license_name=license_name,
            license_url=license_url,
            access_type=access_type,
            login_required=login_required,
            approval_required=approval_required,
            payment_required=payment_required,
            is_restricted=is_restricted,
            source_created_at=parse_datetime(raw.get("metadata_created")),
            source_updated_at=parse_datetime(
                raw.get("metadata_modified")
                or raw.get("revision_timestamp")
                or extras_map.get("modified")
            ),
            source_version=clean_text(raw.get("revision_id") or raw.get("state")),
            row_count=parse_int(
                extras_map.get("record_count") or extras_map.get("rows")
            ),
            dataset_size_bytes=total_size or parse_bytes(extras_map.get("data_size")),
            creators_json=self._build_creators(raw, extras_map),
            resources_json=resources,
            schema_json={
                "described_by": clean_text(extras_map.get("describedBy")),
                "described_by_type": clean_text(extras_map.get("describedByType")),
                "bureau_code": clean_text(extras_map.get("bureauCode")),
                "program_code": clean_text(extras_map.get("programCode")),
            },
            metrics_json={
                "num_resources": parse_int(raw.get("num_resources") or len(resources)),
            },
            extra_json={
                "extras": extras_map,
                "organization": raw.get("organization"),
                "groups": raw.get("groups"),
                "relationships": raw.get("relationships_as_subject"),
                "access_level": access_level,
            },
            raw_json=raw,
        )

    def _extras_map(self, extras: List[Dict[str, Any]]) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        for item in extras:
            if not isinstance(item, dict):
                continue
            key = item.get("key")
            if not key:
                continue
            result[str(key)] = item.get("value")
        return result

    def _split_keywords(self, value: Any) -> List[str]:
        if value in (None, ""):
            return []
        if isinstance(value, list):
            parts = value
        else:
            text = str(value)
            delimiter = ";" if ";" in text else ","
            parts = [part.strip() for part in text.split(delimiter)]
        return unique_strings(parts)

    def _org_title(self, org: Any) -> Any:
        if isinstance(org, dict):
            return org.get("title") or org.get("display_name") or org.get("name")
        return org

    def _build_creators(
        self, raw: Dict[str, Any], extras_map: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        creators: List[Dict[str, Any]] = []
        author = clean_text(raw.get("author"))
        if author:
            creators.append({"name": author, "role": "author"})
        maintainer = clean_text(raw.get("maintainer"))
        if maintainer:
            creators.append(
                {
                    "name": maintainer,
                    "role": "maintainer",
                    "email": clean_text(raw.get("maintainer_email")),
                }
            )
        publisher = clean_text(extras_map.get("publisher"))
        if publisher:
            creators.append({"name": publisher, "role": "publisher"})
        contact = clean_text(extras_map.get("contactPoint"))
        if contact:
            creators.append({"name": contact, "role": "contact"})
        return creators
