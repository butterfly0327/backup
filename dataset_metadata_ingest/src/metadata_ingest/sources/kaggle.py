from __future__ import annotations

import inspect
import os
from typing import Any, Dict, Iterator, List, Tuple

from ..base import BaseDatasetCollector, ResumeGate
from ..models import NormalizedDatasetRecord, SourceDefinition
from ..utils import clean_text, guess_modalities_from_text, parse_datetime, parse_float, parse_int, to_serializable, unique_strings


SOURCE = SourceDefinition(
    source_code="KAGGLE",
    source_name="Kaggle",
    base_url="https://www.kaggle.com",
    collection_type="API",
)


class KaggleCollector(BaseDatasetCollector):
    source = SOURCE

    def iter_records(self, checkpoint: Dict[str, Any]) -> Iterator[Tuple[NormalizedDatasetRecord, Dict[str, Any]]]:
        api = self._authenticate()
        page = max(parse_int(checkpoint.get("page")) or 1, 1)
        start_page = page
        resume_gate = ResumeGate(checkpoint.get("last_saved_source_dataset_key"))

        while True:
            items = self._dataset_list(api, page=page)
            if not items:
                break

            for item in items:
                source_key = clean_text(self._obj_value(item, "ref", "datasetRef", "refId"))
                if not source_key:
                    continue
                if page == start_page and not resume_gate.allow(source_key):
                    continue
                try:
                    detail = self._dataset_view(api, source_key)
                    files = self._dataset_list_files(api, source_key)
                    yield self._normalize(source_key, item, detail, files), {"page": page}
                except Exception as exc:
                    self.note_failure(source_key, exc)
                    continue

            page += 1

    def _authenticate(self):
        try:
            from kaggle.api.kaggle_api_extended import KaggleApi
        except Exception as exc:  # pragma: no cover
            raise ImportError("kaggle 패키지가 설치되어 있지 않습니다. requirements.txt 기준으로 설치하세요.") from exc

        if self.settings.kaggle_username:
            os.environ["KAGGLE_USERNAME"] = self.settings.kaggle_username
        if self.settings.kaggle_key:
            os.environ["KAGGLE_KEY"] = self.settings.kaggle_key
        if self.settings.kaggle_config_dir:
            os.environ["KAGGLE_CONFIG_DIR"] = self.settings.kaggle_config_dir

        api = KaggleApi()
        api.authenticate()
        return api

    def _call_supported(self, func: Any, **kwargs: Any) -> Any:
        signature = inspect.signature(func)
        supported = {key: value for key, value in kwargs.items() if key in signature.parameters}
        return func(**supported)

    def _dataset_list(self, api: Any, *, page: int) -> List[Any]:
        self._pace_before_request()
        items = self._call_supported(
            api.dataset_list,
            page=page,
            search="",
            mine=False,
        )
        return list(items or [])

    def _dataset_view(self, api: Any, ref: str) -> Dict[str, Any]:
        method = getattr(api, "dataset_view", None)
        if method is None:
            return {}
        self._pace_before_request()
        detail = self._call_supported(method, dataset=ref, dataset_ref=ref)
        if detail is None:
            return {}
        return self._to_dict(detail)

    def _dataset_list_files(self, api: Any, ref: str) -> List[Dict[str, Any]]:
        method = getattr(api, "dataset_list_files", None)
        if method is None:
            return []
        self._pace_before_request()
        payload = self._call_supported(method, dataset=ref, dataset_ref=ref)
        items = payload.files if hasattr(payload, "files") else payload
        return [self._to_dict(item) for item in list(items or [])]

    def _normalize(
        self,
        source_key: str,
        item: Any,
        detail: Dict[str, Any],
        files: List[Dict[str, Any]],
    ) -> NormalizedDatasetRecord:
        item_dict = self._to_dict(item)
        title = clean_text(detail.get("title") or item_dict.get("title")) or source_key.split("/")[-1]
        subtitle = clean_text(detail.get("subtitle") or item_dict.get("subtitle"))
        description = clean_text(detail.get("description") or item_dict.get("description") or subtitle)
        landing_url = clean_text(detail.get("url") or item_dict.get("url") or f"https://www.kaggle.com/datasets/{source_key}")

        raw_tags = detail.get("tags") or item_dict.get("tags") or []
        tags = unique_strings(
            tag.get("name") if isinstance(tag, dict) else tag for tag in raw_tags
        )
        creators = []
        owner_name = clean_text(detail.get("ownerName") or item_dict.get("ownerName") or detail.get("creatorName"))
        owner_ref = source_key.split("/")[0] if "/" in source_key else None
        if owner_name or owner_ref:
            creators.append({"name": owner_name or owner_ref, "role": "owner"})
        for contributor in detail.get("collaborators") or []:
            if isinstance(contributor, dict):
                creators.append({"name": clean_text(contributor.get("displayName") or contributor.get("username")), "role": "collaborator"})

        resources: List[Dict[str, Any]] = []
        total_size = parse_int(detail.get("totalBytes") or item_dict.get("totalBytes")) or 0
        if not total_size:
            for file in files:
                size_bytes = parse_int(file.get("totalBytes") or file.get("size") or file.get("sizeBytes"))
                if size_bytes:
                    total_size += size_bytes
                resources.append(
                    {
                        "name": clean_text(file.get("name") or file.get("fileName")),
                        "size_bytes": size_bytes,
                        "creation_date": parse_datetime(file.get("creationDate") or file.get("creation_date")),
                    }
                )
        else:
            for file in files:
                resources.append(
                    {
                        "name": clean_text(file.get("name") or file.get("fileName")),
                        "size_bytes": parse_int(file.get("totalBytes") or file.get("size") or file.get("sizeBytes")),
                        "creation_date": parse_datetime(file.get("creationDate") or file.get("creation_date")),
                    }
                )

        license_name = clean_text(detail.get("licenseName") or item_dict.get("licenseName") or detail.get("license_name"))
        is_private = detail.get("isPrivate") if "isPrivate" in detail else item_dict.get("isPrivate")
        access_type = "REGISTERED"
        login_required = True
        approval_required = False
        is_restricted = True
        if is_private is True:
            access_type = "RESTRICTED"
            approval_required = True
        elif is_private is False:
            access_type = "REGISTERED"
            approval_required = False

        metrics_json = {
            "download_count": parse_int(detail.get("downloadCount") or item_dict.get("downloadCount")),
            "vote_count": parse_int(detail.get("voteCount") or item_dict.get("voteCount")),
            "view_count": parse_int(detail.get("viewCount") or item_dict.get("viewCount")),
            "usability_rating": parse_float(detail.get("usabilityRating") or item_dict.get("usabilityRating")),
        }

        return NormalizedDatasetRecord(
            source_dataset_key=source_key,
            canonical_url=landing_url,
            landing_url=landing_url,
            title=title,
            subtitle=subtitle,
            description_short=description,
            description_long=description,
            publisher_name=owner_name or owner_ref,
            domains=[],
            tasks=tags,
            modalities=guess_modalities_from_text(title, description, tags, [r.get("name") for r in resources]),
            tags=tags,
            languages=[],
            license_name=license_name,
            license_url=None,
            access_type=access_type,
            login_required=login_required,
            approval_required=approval_required,
            payment_required=False,
            is_restricted=is_restricted,
            source_created_at=parse_datetime(detail.get("creationDate") or item_dict.get("creationDate")),
            source_updated_at=parse_datetime(detail.get("lastUpdated") or item_dict.get("lastUpdated") or item_dict.get("last_updated")),
            source_version=clean_text(detail.get("versionNumber") or detail.get("lastUpdated")),
            row_count=parse_int(detail.get("rowCount") or item_dict.get("rowCount")),
            dataset_size_bytes=total_size or None,
            creators_json=creators,
            resources_json=resources,
            schema_json={},
            metrics_json=metrics_json,
            extra_json={
                "owner_ref": owner_ref,
                "subtitle": subtitle,
                "is_private": is_private,
            },
            raw_json={
                "list_item": item_dict,
                "detail": detail,
                "files": files,
            },
        )

    def _obj_value(self, obj: Any, *keys: str) -> Any:
        if isinstance(obj, dict):
            for key in keys:
                if key in obj:
                    return obj.get(key)
            return None
        for key in keys:
            if hasattr(obj, key):
                return getattr(obj, key)
        return None

    def _to_dict(self, obj: Any) -> Dict[str, Any]:
        data = to_serializable(obj)
        if isinstance(data, dict):
            return data
        return {"value": data}
