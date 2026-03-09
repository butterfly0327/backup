from __future__ import annotations

import json
import importlib
import os
import sys
import unittest
from typing import Any
from types import SimpleNamespace

PROJECT_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if PROJECT_SRC not in sys.path:
    sys.path.insert(0, PROJECT_SRC)

AIHubCollector = importlib.import_module("metadata_ingest.sources.aihub").AIHubCollector
DataEuropaCollector = importlib.import_module(
    "metadata_ingest.sources.data_europa"
).DataEuropaCollector
DataGovCollector = importlib.import_module(
    "metadata_ingest.sources.data_gov"
).DataGovCollector
HarvardDataverseCollector = importlib.import_module(
    "metadata_ingest.sources.harvard_dataverse"
).HarvardDataverseCollector
HuggingFaceCollector = importlib.import_module(
    "metadata_ingest.sources.huggingface"
).HuggingFaceCollector
KaggleCollector = importlib.import_module(
    "metadata_ingest.sources.kaggle"
).KaggleCollector


def _make_settings() -> SimpleNamespace:
    return SimpleNamespace(
        user_agent="test-agent",
        request_timeout_seconds=3,
        connect_timeout_seconds=3,
        verify_ssl=False,
        min_request_interval_seconds=0.0,
        request_interval_jitter_seconds=0.0,
        batch_pause_every=0,
        batch_pause_seconds=0.0,
        per_source_cooldown_seconds=0.0,
        runtime_safe_mode=False,
        retry_status_codes={429, 500, 502, 503, 504},
        retry_max_sleep_seconds=1.0,
        save_every=10,
        parser_version="test",
        kaggle_username=None,
        kaggle_key=None,
        kaggle_config_dir=None,
        data_gov_api_key="dummy",
        huggingface_token=None,
        aihub_list_url_template="https://example.com?page={page}",
    )


class _FakeKaggleApi:
    def dataset_metadata(self, dataset: str, path: str) -> str:
        payload = {
            "info": {
                "title": "Road Traffic Dataset",
                "description": "Traffic records for model training",
                "subtitle": "Urban traffic sample",
                "licenses": [{"name": "CC0-1.0"}],
            }
        }
        file_path = f"{path}/dataset-metadata.json"
        with open(file_path, "w", encoding="utf-8") as fp:
            json.dump(payload, fp)
        return file_path


class KaggleCollectorTests(unittest.TestCase):
    collector: Any = None

    def setUp(self) -> None:
        self.collector = KaggleCollector(
            db=SimpleNamespace(), settings=_make_settings()
        )

    def tearDown(self) -> None:
        self.collector.close()

    def test_dataset_view_falls_back_to_dataset_metadata(self) -> None:
        api = _FakeKaggleApi()
        detail = self.collector._dataset_view(api, "owner/dataset")
        self.assertEqual(
            detail.get("info", {}).get("description"),
            "Traffic records for model training",
        )

    def test_normalize_uses_metadata_description_and_license(self) -> None:
        item = {
            "ref": "owner/dataset",
            "title": "dataset",
            "totalDownloads": 12,
            "totalVotes": 2,
            "totalViews": 120,
        }
        detail = {
            "info": {
                "title": "Pretty Dataset Title",
                "subtitle": "Pretty Subtitle",
                "description": "Detailed metadata description",
                "licenses": [{"name": "CC-BY-4.0"}],
            }
        }
        record = self.collector._normalize("owner/dataset", item, detail, files=[])
        self.assertEqual(record.title, "Pretty Dataset Title")
        self.assertEqual(record.subtitle, "Pretty Subtitle")
        self.assertEqual(record.description_short, "Detailed metadata description")
        self.assertEqual(record.description_long, "Detailed metadata description")
        self.assertEqual(record.license_name, "CC-BY-4.0")
        self.assertEqual(record.metrics_json.get("download_count"), 12)


class AIHubCollectorTests(unittest.TestCase):
    collector: Any = None

    def setUp(self) -> None:
        self.collector = AIHubCollector(db=SimpleNamespace(), settings=_make_settings())

    def tearDown(self) -> None:
        self.collector.close()

    def test_generic_og_title_is_ignored(self) -> None:
        picked = self.collector._pick_title(
            "AI-Hub",
            "AI-Hub | 인공지능 포털",
            "한국어 음성 인식 데이터셋",
        )
        self.assertEqual(picked, "한국어 음성 인식 데이터셋")

    def test_noisy_long_title_is_deprioritized(self) -> None:
        noisy = (
            "AI모델 task AI모델 성능 지표 Data I/O Input data: JPG Output data: Category "
            "어노테이션 포맷 설명서 및 활용가이드"
        )
        picked = self.collector._pick_title(noisy, "NEW 공공 민원 상담 데이터")
        self.assertEqual(picked, "NEW 공공 민원 상담 데이터")

    def test_generic_data_classification_title_not_selected(self) -> None:
        text = "NEW 위암 병리 이미지 및 판독문 합성데이터 분야 헬스케어 유형 이미지 생성 방식 합성데이터 구축년도 : 2024"
        extracted = self.collector._extract_title_from_page_text(text)
        picked = self.collector._pick_title("데이터 분류", extracted)
        self.assertEqual(picked, "위암 병리 이미지 및 판독문 합성데이터")

    def test_noisy_footer_description_is_rejected(self) -> None:
        noisy = (
            "페이지에서 다운로드 버튼 클릭하여 승인이 필요합니다. "
            "개인정보보호 책임자 : 양현수 운영지원단장 CONTACT. 한국지능정보사회진흥원 "
            "이용약관 개인정보처리방침 사이트맵 Family Site"
        )
        self.assertIsNone(self.collector._pick_description(noisy))


class HarvardCollectorTests(unittest.TestCase):
    collector: Any = None

    def setUp(self) -> None:
        self.collector = HarvardDataverseCollector(
            db=SimpleNamespace(), settings=_make_settings()
        )

    def tearDown(self) -> None:
        self.collector.close()

    def test_distributor_used_as_publisher_and_description_fallback(self) -> None:
        search_item = {
            "global_id": "doi:test/1",
            "name": "Fallback Name",
            "description": None,
            "subjects": [],
        }
        detail_payload = {
            "data": {
                "description": "detail-level description",
                "latestVersion": {
                    "metadataBlocks": {
                        "citation": {
                            "fields": [
                                {
                                    "typeName": "title",
                                    "value": "Dataverse Title",
                                },
                                {
                                    "typeName": "distributor",
                                    "value": [
                                        {
                                            "distributorName": {
                                                "value": "Distributor Org",
                                                "typeName": "distributorName",
                                                "typeClass": "primitive",
                                            }
                                        }
                                    ],
                                },
                                {
                                    "typeName": "dsDescription",
                                    "value": [
                                        {
                                            "dsDescriptionValue": {
                                                "value": "Nested description",
                                                "typeName": "dsDescriptionValue",
                                                "typeClass": "primitive",
                                            }
                                        }
                                    ],
                                },
                            ]
                        }
                    },
                    "files": [],
                },
            }
        }
        record = self.collector._normalize(search_item, detail_payload)
        self.assertEqual(record.publisher_name, "Distributor Org")
        self.assertEqual(record.description_short, "Nested description")


class DataEuropaCollectorTests(unittest.TestCase):
    collector: Any = None

    def setUp(self) -> None:
        self.collector = DataEuropaCollector(
            db=SimpleNamespace(), settings=_make_settings()
        )

    def tearDown(self) -> None:
        self.collector.close()

    def test_literal_publisher_value_maps_to_publisher_name(self) -> None:
        search_item = {"id": "x", "title": "title"}
        detail_payload = {
            "@id": "dataset-1",
            "@type": "Dataset",
            "title": "Europa Dataset",
            "description": "desc",
            "publisher": "Office for Open Data",
        }
        record = self.collector._normalize(
            search_item=search_item,
            detail_payload=detail_payload,
            source_key="dataset-1",
        )
        self.assertEqual(record.publisher_name, "Office for Open Data")


class DataGovCollectorTests(unittest.TestCase):
    collector: Any = None

    def setUp(self) -> None:
        self.collector = DataGovCollector(
            db=SimpleNamespace(), settings=_make_settings()
        )

    def tearDown(self) -> None:
        self.collector.close()

    def test_access_level_public_overrides_isopen_false(self) -> None:
        raw = {
            "id": "id-1",
            "name": "name-1",
            "title": "Title",
            "notes": "desc",
            "extras": [{"key": "accessLevel", "value": "public"}],
            "isopen": False,
            "resources": [],
        }
        record = self.collector._normalize(raw)
        self.assertEqual(record.access_type, "OPEN")
        self.assertFalse(record.is_restricted)


class HuggingFaceCollectorTests(unittest.TestCase):
    collector: Any = None

    def setUp(self) -> None:
        self.collector = HuggingFaceCollector(
            db=SimpleNamespace(), settings=_make_settings()
        )

    def tearDown(self) -> None:
        self.collector.close()

    def test_description_short_falls_back_to_card_description(self) -> None:
        raw = {
            "id": "org/ds",
            "description": None,
            "cardData": {"description": "card description text"},
            "tags": [],
            "siblings": [],
        }
        record = self.collector._normalize(raw)
        self.assertEqual(record.description_short, "card description text")


if __name__ == "__main__":
    unittest.main()
