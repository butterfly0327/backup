from __future__ import annotations

from typing import Any, Dict, Iterator, List, Optional, Tuple

from ..base import BaseDatasetCollector, ResumeGate
from ..models import NormalizedDatasetRecord, SourceDefinition
from ..utils import (
    clean_text,
    domains_from_urls,
    ensure_list,
    extract_uuid,
    guess_modalities_from_text,
    jsonld_collect_text,
    jsonld_first,
    jsonld_value,
    parse_bytes,
    parse_datetime,
    parse_int,
    unique_strings,
)


SOURCE = SourceDefinition(
    source_code="DATA_EUROPA",
    source_name="data.europa.eu",
    base_url="https://data.europa.eu",
    collection_type="API",
)


class DataEuropaCollector(BaseDatasetCollector):
    source = SOURCE
    PAGE_SIZE = 100

    def iter_records(
        self, checkpoint: Dict[str, Any]
    ) -> Iterator[Tuple[NormalizedDatasetRecord, Dict[str, Any]]]:
        page = max(parse_int(checkpoint.get("page")) or 0, 0)
        start_page = page
        resume_gate = ResumeGate(checkpoint.get("last_saved_source_dataset_key"))

        while True:
            payload = self.get_json(
                f"{self.source.base_url}/api/hub/search/search",
                params={"page": page, "limit": self.PAGE_SIZE},
            )
            items = self._extract_items(payload)
            if not items:
                break

            for item in items:
                source_key = self._extract_dataset_id(item)
                if not source_key:
                    continue
                if page == start_page and not resume_gate.allow(source_key):
                    continue

                try:
                    detail = self.get_json(
                        f"{self.source.base_url}/api/hub/repo/datasets/{source_key}"
                    )
                    yield (
                        self._normalize(
                            search_item=item,
                            detail_payload=detail,
                            source_key=source_key,
                        ),
                        {"page": page},
                    )
                except Exception:
                    try:
                        yield (
                            self._normalize(
                                search_item=item,
                                detail_payload=None,
                                source_key=source_key,
                            ),
                            {"page": page},
                        )
                    except Exception as exc:
                        self.note_failure(source_key, exc)
                        continue

            page += 1

    def _normalize(
        self,
        *,
        search_item: Dict[str, Any],
        detail_payload: Optional[Dict[str, Any]],
        source_key: str,
    ) -> NormalizedDatasetRecord:
        dataset_node, graph_index = self._extract_dataset_node(detail_payload)

        landing_url = (
            self._first_text_any(dataset_node, ["landingPage"])
            or self._first_text_any(
                search_item, ["landingPage", "url", "uri", "id", "about"]
            )
            or f"{self.source.base_url}/en/data/datasets/{source_key}"
        )
        title = (
            self._first_text_any(dataset_node, ["title", "name"])
            or self._first_text_any(search_item, ["title", "name"])
            or source_key
        )
        description = self._join_texts(
            dataset_node, ["description"]
        ) or self._join_texts(search_item, ["description", "summary"])
        themes = self._collect_text_any(
            dataset_node, ["theme"]
        ) or self._collect_text_any(search_item, ["theme"])
        keywords = self._collect_text_any(
            dataset_node, ["keyword"]
        ) or self._collect_text_any(search_item, ["keyword", "keywords", "tag"])
        languages = self._collect_text_any(dataset_node, ["language"])
        tags = unique_strings(themes + keywords)

        distributions = self._extract_distributions(dataset_node, graph_index)
        resources: List[Dict[str, Any]] = []
        total_size = 0
        licenses: List[str] = []
        for dist in distributions:
            size_bytes = parse_bytes(
                self._first_text_any(dist, ["byteSize", "size"])
            ) or parse_int(self._first_text_any(dist, ["byteSize", "size"]))
            if size_bytes:
                total_size += size_bytes
            license_value = self._first_text_any(dist, ["license"])
            if license_value:
                licenses.append(license_value)
            resources.append(
                {
                    "title": self._first_text_any(dist, ["title", "name"]),
                    "description": self._join_texts(dist, ["description"]),
                    "download_url": self._first_text_any(
                        dist, ["downloadURL", "accessURL"]
                    ),
                    "format": self._first_text_any(dist, ["format", "mediaType"]),
                    "byte_size": size_bytes,
                    "license": license_value,
                }
            )

        creators = self._extract_agents(
            dataset_node, graph_index, ["creator", "publisher", "contactPoint"]
        )
        publisher_name = (
            self._agent_name(creators, "publisher")
            or self._first_text_any(
                dataset_node, ["publisher", "creator", "rightsHolder"]
            )
            or self._agent_name(creators, None)
        )

        access_value = self._first_text_any(dataset_node, ["accessRights"])
        access_type = "OPEN"
        login_required = False
        approval_required = False
        payment_required = False
        is_restricted = False
        if access_value:
            lowered = access_value.casefold()
            if any(
                token in lowered
                for token in ["restricted", "non-public", "nonpublic", "private"]
            ):
                access_type = "RESTRICTED"
                login_required = True
                approval_required = True
                is_restricted = True
            elif "registered" in lowered:
                access_type = "REGISTERED"
                login_required = True
                is_restricted = True
            elif "paid" in lowered:
                access_type = "PAID"
                payment_required = True
                is_restricted = True

        return NormalizedDatasetRecord(
            source_dataset_key=source_key,
            canonical_url=landing_url,
            landing_url=landing_url,
            title=title,
            description_short=description,
            description_long=description,
            publisher_name=publisher_name,
            domains=themes,
            tasks=keywords,
            modalities=guess_modalities_from_text(
                title, description, tags, [r.get("format") for r in resources]
            ),
            tags=tags,
            languages=languages,
            license_name=self._first_text_any(dataset_node, ["license", "rights"])
            or (licenses[0] if licenses else None),
            license_url=self._first_url_any(dataset_node, ["license"])
            or self._first_url_any(search_item, ["license"]),
            access_type=access_type,
            login_required=login_required,
            approval_required=approval_required,
            payment_required=payment_required,
            is_restricted=is_restricted,
            source_created_at=parse_datetime(
                self._first_text_any(dataset_node, ["issued", "created"])
            ),
            source_updated_at=parse_datetime(
                self._first_text_any(dataset_node, ["modified"])
            ),
            source_version=self._first_text_any(
                dataset_node, ["versionInfo", "version"]
            ),
            row_count=parse_int(
                self._first_text_any(dataset_node, ["recordCount", "rows"])
            ),
            dataset_size_bytes=total_size or None,
            creators_json=creators,
            resources_json=resources,
            schema_json={
                "conforms_to": self._collect_text_any(dataset_node, ["conformsTo"]),
                "temporal_resolution": self._first_text_any(
                    dataset_node, ["temporalResolution"]
                ),
                "spatial_resolution_meters": self._first_text_any(
                    dataset_node, ["spatialResolutionInMeters"]
                ),
            },
            metrics_json={
                "distribution_count": len(resources),
            },
            extra_json={
                "themes": themes,
                "keywords": keywords,
                "languages": languages,
                "search_item": search_item,
                "graph_node_count": len(graph_index),
            },
            raw_json={
                "search_item": search_item,
                "detail_payload": detail_payload,
            },
        )

    def _extract_items(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        candidates = [
            payload.get("results"),
            payload.get("items"),
            (payload.get("result") or {}).get("results")
            if isinstance(payload.get("result"), dict)
            else None,
            (payload.get("data") or {}).get("results")
            if isinstance(payload.get("data"), dict)
            else None,
            (payload.get("hits") or {}).get("hits")
            if isinstance(payload.get("hits"), dict)
            else None,
        ]
        for candidate in candidates:
            if (
                isinstance(candidate, list)
                and candidate
                and all(isinstance(item, dict) for item in candidate)
            ):
                return candidate
        for value in payload.values():
            if (
                isinstance(value, list)
                and value
                and all(isinstance(item, dict) for item in value)
            ):
                return value
        return []

    def _extract_dataset_id(self, item: Dict[str, Any]) -> Optional[str]:
        for key in ["id", "datasetId", "identifier", "uri", "about", "href"]:
            value = item.get(key)
            if value is None:
                continue
            if isinstance(value, list):
                values = value
            else:
                values = [value]
            for candidate in values:
                if isinstance(candidate, dict):
                    candidate = (
                        candidate.get("@id")
                        or candidate.get("id")
                        or candidate.get("identifier")
                    )
                text = clean_text(candidate)
                uuid = extract_uuid(text)
                if uuid:
                    return uuid
        return None

    def _extract_dataset_node(
        self,
        detail_payload: Optional[Dict[str, Any]],
    ) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]]]:
        if not detail_payload:
            return {}, {}
        nodes: List[Dict[str, Any]] = []
        if isinstance(detail_payload, list):
            nodes = [node for node in detail_payload if isinstance(node, dict)]
        elif isinstance(detail_payload, dict):
            graph_nodes = detail_payload.get("@graph")
            if isinstance(graph_nodes, list):
                nodes = [node for node in graph_nodes if isinstance(node, dict)]
            else:
                nodes = [detail_payload]
        index: Dict[str, Dict[str, Any]] = {}
        for node in nodes:
            node_id = clean_text(node.get("@id") or node.get("id"))
            if node_id:
                index[node_id] = node
        for node in nodes:
            types = self._types(node)
            if any(t.casefold().endswith("dataset") for t in types):
                return node, index
        return (nodes[0] if nodes else {}), index

    def _types(self, node: Dict[str, Any]) -> List[str]:
        values = ensure_list(node.get("@type") or node.get("type"))
        return unique_strings(jsonld_value(v) for v in values)

    def _key_local(self, key: str) -> str:
        key = key.rsplit("#", 1)[-1]
        key = key.rsplit("/", 1)[-1]
        key = key.split(":")[-1]
        return key

    def _match_value(self, node: Dict[str, Any], local_names: List[str]) -> Any:
        for key, value in node.items():
            if self._key_local(str(key)) in local_names:
                return value
        return None

    def _collect_text_any(
        self, node: Dict[str, Any], local_names: List[str]
    ) -> List[str]:
        values: List[str] = []
        for key, value in node.items():
            if self._key_local(str(key)) not in local_names:
                continue
            for item in ensure_list(value):
                item = jsonld_value(item)
                if isinstance(item, dict):
                    text = jsonld_first(
                        item, ["prefLabel", "label", "name", "title", "@id"]
                    )
                else:
                    text = item
                text = clean_text(text) if text is not None else None
                if text:
                    values.append(text)
        return unique_strings(values)

    def _join_texts(
        self, node: Dict[str, Any], local_names: List[str]
    ) -> Optional[str]:
        values = self._collect_text_any(node, local_names)
        return "\n\n".join(values) if values else None

    def _first_text_any(
        self, node: Dict[str, Any], local_names: List[str]
    ) -> Optional[str]:
        values = self._collect_text_any(node, local_names)
        return values[0] if values else None

    def _first_url_any(
        self, node: Dict[str, Any], local_names: List[str]
    ) -> Optional[str]:
        for key, value in node.items():
            if self._key_local(str(key)) not in local_names:
                continue
            for item in ensure_list(value):
                item = jsonld_value(item)
                if isinstance(item, dict):
                    text = clean_text(
                        item.get("@id") or item.get("id") or item.get("url")
                    )
                else:
                    text = clean_text(item)
                if text and text.startswith("http"):
                    return text
        return None

    def _resolve_node(
        self, value: Any, index: Dict[str, Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        value = jsonld_value(value)
        if isinstance(value, dict):
            ref_id = clean_text(value.get("@id") or value.get("id"))
            if ref_id and ref_id in index:
                return index[ref_id]
            return value
        if isinstance(value, str) and value in index:
            return index[value]
        return None

    def _extract_distributions(
        self, dataset_node: Dict[str, Any], index: Dict[str, Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        value = self._match_value(dataset_node, ["distribution"])
        results: List[Dict[str, Any]] = []
        for item in ensure_list(value):
            node = self._resolve_node(item, index)
            if node:
                results.append(node)
        return results

    def _extract_agents(
        self,
        dataset_node: Dict[str, Any],
        index: Dict[str, Dict[str, Any]],
        local_names: List[str],
    ) -> List[Dict[str, Any]]:
        agents: List[Dict[str, Any]] = []
        for local_name in local_names:
            value = self._match_value(dataset_node, [local_name])
            for item in ensure_list(value):
                node = self._resolve_node(item, index)
                if not node:
                    text = clean_text(item)
                    if text:
                        agents.append(
                            {
                                "role": local_name,
                                "name": text,
                                "email": None,
                                "url": None,
                            }
                        )
                    continue
                agents.append(
                    {
                        "role": local_name,
                        "name": self._first_text_any(
                            node,
                            [
                                "name",
                                "title",
                                "prefLabel",
                                "label",
                                "legalName",
                                "organizationName",
                                "organization-name",
                                "fn",
                            ],
                        ),
                        "email": self._first_text_any(
                            node, ["email", "mbox", "hasEmail"]
                        ),
                        "url": self._first_url_any(node, ["homepage", "url"]),
                    }
                )
        return [agent for agent in agents if agent.get("name") or agent.get("email")]

    def _agent_name(
        self, creators: List[Dict[str, Any]], role: Optional[str]
    ) -> Optional[str]:
        for creator in creators:
            if role is None or creator.get("role") == role:
                if creator.get("name"):
                    return clean_text(creator.get("name"))
        return None
