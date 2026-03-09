from __future__ import annotations

import json
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

    def iter_records(
        self, checkpoint: Dict[str, Any]
    ) -> Iterator[Tuple[NormalizedDatasetRecord, Dict[str, Any]]]:
        start = max(parse_int(checkpoint.get("start")) or 0, 0)
        start_offset = start
        resume_gate = ResumeGate(checkpoint.get("last_saved_source_dataset_key"))

        while True:
            payload = self._get_json_with_guard(
                f"{self.source.base_url}/api/search",
                params={
                    "q": "*",
                    "type": "dataset",
                    "per_page": self.PAGE_SIZE,
                    "start": start,
                },
                failure_key=f"search:start:{start}",
            )
            if payload is None:
                break

            items = safe_get(payload, "data", "items") or []
            if not items:
                break

            for item in items:
                source_key = clean_text(
                    item.get("global_id")
                    or item.get("persistentId")
                    or item.get("identifier")
                )
                if not source_key:
                    continue
                if start == start_offset and not resume_gate.allow(source_key):
                    continue

                try:
                    detail = self._get_json_with_guard(
                        f"{self.source.base_url}/api/datasets/:persistentId/",
                        params={"persistentId": source_key},
                        failure_key=source_key,
                    )
                    if detail is None:
                        continue
                    yield self._normalize(item, detail), {"start": start}
                except Exception as exc:
                    self.note_failure(source_key, exc)
                    continue

            start += self.PAGE_SIZE

    def _get_json_with_guard(
        self,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        failure_key: str,
        parse_retries: int = 3,
    ) -> Optional[Dict[str, Any]]:
        headers = {"Accept": "application/json"}
        for attempt in range(1, parse_retries + 1):
            try:
                text = self.get_text(url, params=params, headers=headers, retries=3)
            except Exception as exc:
                if attempt >= parse_retries:
                    self.note_failure(failure_key, exc)
                    return None
                self.sleep(1.0 * attempt)
                continue

            body = text.strip()
            if not body:
                if attempt >= parse_retries:
                    self.note_failure(
                        failure_key, ValueError("empty JSON response body")
                    )
                    return None
                self.sleep(1.0 * attempt)
                continue

            try:
                payload = json.loads(body)
            except json.JSONDecodeError as exc:
                if attempt >= parse_retries:
                    self.note_failure(
                        failure_key, ValueError(f"invalid JSON payload: {exc}")
                    )
                    return None
                self.sleep(1.0 * attempt)
                continue

            if not isinstance(payload, dict):
                if attempt >= parse_retries:
                    self.note_failure(
                        failure_key, ValueError("JSON payload is not an object")
                    )
                    return None
                self.sleep(1.0 * attempt)
                continue

            return payload

        return None

    def _normalize(
        self, search_item: Dict[str, Any], detail_payload: Dict[str, Any]
    ) -> NormalizedDatasetRecord:
        detail = safe_get(detail_payload, "data") or detail_payload
        latest = detail.get("latestVersion") or {}
        metadata_blocks = latest.get("metadataBlocks") or {}
        citation_fields = self._field_map(
            safe_get(metadata_blocks, "citation", "fields") or []
        )

        source_key = (
            clean_text(search_item.get("global_id") or search_item.get("persistentId"))
            or ""
        )
        landing_url = f"{self.source.base_url}/dataset.xhtml?{urlencode({'persistentId': source_key})}"
        title = (
            self._single_text(citation_fields.get("title"))
            or clean_text(search_item.get("name"))
            or source_key
        )
        subtitle = self._single_text(citation_fields.get("subtitle"))
        descriptions = self._compound_texts(
            citation_fields.get("dsDescription"), "dsDescriptionValue"
        )
        fallback_description = clean_text(
            search_item.get("description")
            or detail.get("description")
            or latest.get("description")
        )
        description_short = descriptions[0] if descriptions else fallback_description
        description_long = (
            "\n\n".join(descriptions) if descriptions else description_short
        )

        subjects = unique_strings(
            ensure_list(search_item.get("subjects"))
            + self._multi_text(citation_fields.get("subject"))
        )
        keywords = self._compound_texts(citation_fields.get("keyword"), "keywordValue")
        languages = unique_strings(self._multi_text(citation_fields.get("language")))
        kind_of_data = self._multi_text(citation_fields.get("kindOfData"))
        tags = unique_strings(subjects + keywords + kind_of_data)

        authors = self._compound_people(
            citation_fields.get("author"),
            name_key="authorName",
            affiliation_key="authorAffiliation",
        )
        producers = self._compound_people(
            citation_fields.get("producer"),
            name_key="producerName",
            affiliation_key="producerAffiliation",
        )
        distributors = self._compound_people(
            citation_fields.get("distributor"),
            name_key="distributorName",
            affiliation_key="distributorAffiliation",
        )
        contacts = self._compound_people(
            citation_fields.get("datasetContact"),
            name_key="datasetContactName",
            affiliation_key="datasetContactAffiliation",
            email_key="datasetContactEmail",
        )
        creators = authors + producers + distributors + contacts

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
                    "description": clean_text(
                        data_file.get("description") or file_item.get("description")
                    ),
                    "content_type": clean_text(data_file.get("contentType")),
                    "size_bytes": size_bytes,
                    "storage_identifier": clean_text(
                        data_file.get("storageIdentifier")
                    ),
                    "restricted": restricted,
                }
            )

        access_type = "RESTRICTED" if restricted_file_exists else "OPEN"
        login_required = True if restricted_file_exists else False
        approval_required = True if restricted_file_exists else False
        is_restricted = restricted_file_exists

        publisher_name = (
            (producers[0].get("name") if producers else None)
            or (distributors[0].get("name") if distributors else None)
            or (authors[0].get("name") if authors else None)
            or (contacts[0].get("name") if contacts else None)
            or clean_text(detail.get("ownerName"))
            or clean_text(search_item.get("name"))
        )

        version = None
        if (
            latest.get("majorVersion") is not None
            and latest.get("minorVersion") is not None
        ):
            version = f"{latest.get('majorVersion')}.{latest.get('minorVersion')}"
            if latest.get("versionState"):
                version += f" ({latest.get('versionState')})"

        extra_json = {
            "citation_fields": citation_fields,
            "metadata_blocks": metadata_blocks,
            "terms_of_use": clean_text(
                detail.get("termsOfUse") or latest.get("termsOfUse")
            ),
            "license": latest.get("license"),
            "version_state": latest.get("versionState"),
            "publication_date": parse_datetime(
                detail.get("publicationDate") or latest.get("publicationDate")
            ),
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
            modalities=guess_modalities_from_text(
                title, description_long, tags, [r.get("filename") for r in resources]
            ),
            tags=tags,
            languages=languages,
            license_name=clean_text(
                safe_get(latest, "license", "name")
                or safe_get(extra_json, "license", "name")
            ),
            license_url=clean_text(
                safe_get(latest, "license", "uri")
                or safe_get(extra_json, "license", "uri")
            ),
            access_type=access_type,
            login_required=login_required,
            approval_required=approval_required,
            payment_required=False,
            is_restricted=is_restricted,
            source_created_at=parse_datetime(
                search_item.get("published_at")
                or detail.get("publicationDate")
                or latest.get("createTime")
            ),
            source_updated_at=parse_datetime(
                search_item.get("updatedAt")
                or latest.get("lastUpdateTime")
                or detail.get("lastUpdateTime")
            ),
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
            if isinstance(item, str):
                text = clean_text(item)
                if text:
                    result.append(text)
                continue
            if not isinstance(item, dict):
                continue
            item_value = item.get("value")
            if item_value in (None, []) and key_name in item:
                item_value = [item]
            if isinstance(item_value, str):
                text = clean_text(item_value)
                if text:
                    result.append(text)
                continue
            for child in ensure_list(item_value):
                if not isinstance(child, dict):
                    continue
                if child.get("typeName") == key_name:
                    text = clean_text(child.get("value"))
                elif key_name in child and isinstance(child.get(key_name), dict):
                    text = clean_text(child.get(key_name, {}).get("value"))
                elif key_name in child:
                    text = clean_text(child.get(key_name))
                else:
                    text = None
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
            if isinstance(item, str):
                name = clean_text(item)
                if name:
                    people.append({"name": name})
                continue
            if not isinstance(item, dict):
                continue
            values = item.get("value") or []
            if not values and any(
                alias and alias in item
                for alias in [name_key, affiliation_key, email_key]
            ):
                values = [item]
            if isinstance(values, dict):
                values = [values]
            elif not isinstance(values, list):
                values = [{"typeName": name_key, "value": values}]
            person: Dict[str, Any] = {}
            for child in values:
                if not isinstance(child, dict):
                    continue
                type_name = child.get("typeName")
                child_value = clean_text(child.get("value"))
                if child_value is None:
                    for alias in [name_key, affiliation_key, email_key]:
                        if not alias or alias not in child:
                            continue
                        nested_value = child.get(alias)
                        if isinstance(nested_value, dict):
                            child_value = clean_text(nested_value.get("value"))
                        else:
                            child_value = clean_text(nested_value)
                        if child_value:
                            type_name = alias
                            break
                if not child_value:
                    continue
                if type_name == name_key:
                    person["name"] = child_value
                elif affiliation_key and type_name == affiliation_key:
                    person["affiliation"] = child_value
                elif email_key and type_name == email_key:
                    person["email"] = child_value
                else:
                    if isinstance(type_name, str) and type_name:
                        person[type_name] = child_value
            if person.get("name"):
                people.append(person)
        return people
