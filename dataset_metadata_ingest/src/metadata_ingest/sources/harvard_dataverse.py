from __future__ import annotations

from typing import Any, Dict, Iterator, List, Optional, Tuple
from urllib.parse import urlencode

from ..base import BaseDatasetCollector, ResumeGate
from ..models import NormalizedDatasetRecord, SourceDefinition
from ..utils import (
    clean_text,
    domains_from_urls,
    ensure_list,
    guess_modalities_from_text,
    parse_datetime,
    parse_int,
    safe_get,
    unique_strings,
)


SOURCE = SourceDefinition(
    source_code="HARVARD_DATAVERSE",
    source_name="Harvard Dataverse",
    base_url="https://dataverse.harvard.edu",
    collection_type="API",
)


class HarvardDataverseCollector(BaseDatasetCollector):
    source = SOURCE
    PAGE_SIZE = 100

    def iter_records(self, checkpoint: Dict[str, Any]) -> Iterator[Tuple[NormalizedDatasetRecord, Dict[str, Any]]]:
        start = max(parse_int(checkpoint.get("start")) or 0, 0)
        start_offset = start
        resume_gate = ResumeGate(checkpoint.get("last_saved_source_dataset_key"))

        while True:
            payload = self.get_json(
                f"{self.source.base_url}/api/search",
                params={
                    "q": "*",
                    "type": "dataset",
                    "per_page": self.PAGE_SIZE,
                    "start": start,
                },
            )
            items = safe_get(payload, "data", "items") or []
            if not items:
                break

            for item in items:
                source_key = clean_text(item.get("global_id") or item.get("persistentId") or item.get("identifier"))
                if not source_key:
                    continue
                if start == start_offset and not resume_gate.allow(source_key):
                    continue

                try:
                    detail = self.get_json(
                        f"{self.source.base_url}/api/datasets/:persistentId/",
                        params={"persistentId": source_key},
                    )
                    yield self._normalize(item, detail), {"start": start}
                except Exception as exc:
                    self.note_failure(source_key, exc)
                    continue

            start += self.PAGE_SIZE

    def _normalize(self, search_item: Dict[str, Any], detail_payload: Dict[str, Any]) -> NormalizedDatasetRecord:
        detail = safe_get(detail_payload, "data") or detail_payload
        latest = detail.get("latestVersion") or {}
        metadata_blocks = latest.get("metadataBlocks") or {}
        citation_fields = self._field_map(safe_get(metadata_blocks, "citation", "fields") or [])

        source_key = clean_text(search_item.get("global_id") or search_item.get("persistentId")) or ""
        landing_url = f"{self.source.base_url}/dataset.xhtml?{urlencode({'persistentId': source_key})}"
        title = self._single_text(citation_fields.get("title")) or clean_text(search_item.get("name")) or source_key
        subtitle = self._single_text(citation_fields.get("subtitle"))
        descriptions = self._compound_texts(citation_fields.get("dsDescription"), "dsDescriptionValue")
        description_short = descriptions[0] if descriptions else clean_text(search_item.get("description"))
        description_long = "\n\n".join(descriptions) if descriptions else description_short

        subjects = unique_strings(ensure_list(search_item.get("subjects")) + self._multi_text(citation_fields.get("subject")))
        keywords = self._compound_texts(citation_fields.get("keyword"), "keywordValue")
        languages = unique_strings(self._multi_text(citation_fields.get("language")))
        kind_of_data = self._multi_text(citation_fields.get("kindOfData"))
        tags = unique_strings(subjects + keywords + kind_of_data)

        authors = self._compound_people(citation_fields.get("author"), name_key="authorName", affiliation_key="authorAffiliation")
        producers = self._compound_people(citation_fields.get("producer"), name_key="producerName", affiliation_key="producerAffiliation")
        contacts = self._compound_people(citation_fields.get("datasetContact"), name_key="datasetContactName", affiliation_key="datasetContactAffiliation", email_key="datasetContactEmail")
        creators = authors + producers + contacts

        files = latest.get("files") or []
        resources: List[Dict[str, Any]] = []
        total_size = 0
        restricted_file_exists = False
        for file_item in files:
            if not isinstance(file_item, dict):
                continue
            data_file = file_item.get("dataFile") or {}
            size_bytes = parse_int(data_file.get("filesize"))
            if size_bytes:
                total_size += size_bytes
            restricted = bool(file_item.get("restricted"))
            restricted_file_exists = restricted_file_exists or restricted
            resources.append(
                {
                    "file_id": data_file.get("id"),
                    "filename": clean_text(data_file.get("filename")),
                    "description": clean_text(data_file.get("description") or file_item.get("description")),
                    "content_type": clean_text(data_file.get("contentType")),
                    "size_bytes": size_bytes,
                    "storage_identifier": clean_text(data_file.get("storageIdentifier")),
                    "restricted": restricted,
                }
            )

        access_type = "RESTRICTED" if restricted_file_exists else "OPEN"
        login_required = True if restricted_file_exists else False
        approval_required = True if restricted_file_exists else False
        is_restricted = restricted_file_exists

        publisher_name = (
            (producers[0].get("name") if producers else None)
            or (authors[0].get("name") if authors else None)
            or clean_text(detail.get("ownerName"))
        )

        version = None
        if latest.get("majorVersion") is not None and latest.get("minorVersion") is not None:
            version = f"{latest.get('majorVersion')}.{latest.get('minorVersion')}"
            if latest.get("versionState"):
                version += f" ({latest.get('versionState')})"

        extra_json = {
            "citation_fields": citation_fields,
            "metadata_blocks": metadata_blocks,
            "terms_of_use": clean_text(detail.get("termsOfUse") or latest.get("termsOfUse")),
            "license": latest.get("license"),
            "version_state": latest.get("versionState"),
            "publication_date": parse_datetime(detail.get("publicationDate") or latest.get("publicationDate")),
        }

        return NormalizedDatasetRecord(
            source_dataset_key=source_key,
            canonical_url=landing_url,
            landing_url=landing_url,
            title=title,
            subtitle=subtitle,
            description_short=description_short,
            description_long=description_long,
            publisher_name=publisher_name,
            domains=unique_strings(subjects),
            tasks=keywords,
            modalities=guess_modalities_from_text(title, description_long, tags, [r.get("filename") for r in resources]),
            tags=tags,
            languages=languages,
            license_name=clean_text(safe_get(latest, "license", "name") or safe_get(extra_json, "license", "name")),
            license_url=clean_text(safe_get(latest, "license", "uri") or safe_get(extra_json, "license", "uri")),
            access_type=access_type,
            login_required=login_required,
            approval_required=approval_required,
            payment_required=False,
            is_restricted=is_restricted,
            source_created_at=parse_datetime(search_item.get("published_at") or detail.get("publicationDate") or latest.get("createTime")),
            source_updated_at=parse_datetime(search_item.get("updatedAt") or latest.get("lastUpdateTime") or detail.get("lastUpdateTime")),
            source_version=version,
            row_count=None,
            dataset_size_bytes=total_size or None,
            creators_json=creators,
            resources_json=resources,
            schema_json={
                "metadata_block_names": list(metadata_blocks.keys()),
                "citation_field_names": list(citation_fields.keys()),
            },
            metrics_json={
                "file_count": parse_int(search_item.get("fileCount")),
                "version_id": parse_int(search_item.get("versionId")),
            },
            extra_json=extra_json,
            raw_json={"search_item": search_item, "detail": detail_payload},
        )

    def _field_map(self, fields: List[Dict[str, Any]]) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        for field in fields:
            if not isinstance(field, dict):
                continue
            type_name = field.get("typeName")
            if not type_name:
                continue
            result[type_name] = field.get("value")
        return result

    def _single_text(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, list):
            for item in value:
                text = self._single_text(item)
                if text:
                    return text
            return None
        if isinstance(value, dict):
            if "value" in value and len(value) == 1:
                return clean_text(value.get("value"))
            return None
        return clean_text(value)

    def _multi_text(self, value: Any) -> List[str]:
        result: List[str] = []
        for item in ensure_list(value):
            if isinstance(item, dict):
                if item.get("typeClass") == "primitive":
                    text = clean_text(item.get("value"))
                else:
                    text = None
            else:
                text = clean_text(item)
            if text:
                result.append(text)
        return unique_strings(result)

    def _compound_texts(self, value: Any, key_name: str) -> List[str]:
        result: List[str] = []
        for item in ensure_list(value):
            if not isinstance(item, dict):
                continue
            for child in ensure_list(item.get("value")):
                if not isinstance(child, dict):
                    continue
                if child.get("typeName") != key_name:
                    continue
                text = clean_text(child.get("value"))
                if text:
                    result.append(text)
        return unique_strings(result)

    def _compound_people(
        self,
        value: Any,
        *,
        name_key: str,
        affiliation_key: Optional[str] = None,
        email_key: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        people: List[Dict[str, Any]] = []
        for item in ensure_list(value):
            if not isinstance(item, dict):
                continue
            values = item.get("value") or []
            person: Dict[str, Any] = {}
            for child in values:
                if not isinstance(child, dict):
                    continue
                type_name = child.get("typeName")
                child_value = clean_text(child.get("value"))
                if not child_value:
                    continue
                if type_name == name_key:
                    person["name"] = child_value
                elif affiliation_key and type_name == affiliation_key:
                    person["affiliation"] = child_value
                elif email_key and type_name == email_key:
                    person["email"] = child_value
                else:
                    person[type_name] = child_value
            if person.get("name"):
                people.append(person)
        return people
