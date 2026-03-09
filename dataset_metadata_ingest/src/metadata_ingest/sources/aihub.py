from __future__ import annotations

import re
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urljoin, urlparse

BeautifulSoup = Any

from ..base import BaseDatasetCollector, ResumeGate
from ..models import NormalizedDatasetRecord, SourceDefinition
from ..utils import (
    clean_text,
    extract_hashtags,
    guess_modalities_from_text,
    parse_datetime,
    parse_int,
    parse_kv_text_block,
    unique_strings,
)


SOURCE = SourceDefinition(
    source_code="AI_HUB",
    source_name="AI Hub",
    base_url="https://www.aihub.or.kr",
    collection_type="CRAWL",
)


_SECTION_NAMES = [
    "데이터 개요",
    "데이터 변경이력",
    "메타데이터 구조표",
    "데이터 통계",
    "데이터 성능 지표",
    "어노테이션 포맷 및 데이터 구조",
    "소개",
    "활용서비스",
]

_GENERIC_TITLE_TOKENS = {
    "aihub",
    "ai hub",
    "ai-hub",
    "인공지능허브",
    "인공지능 허브",
    "데이터 분류",
    "데이터 소개",
    "데이터 정보",
}


class AIHubCollector(BaseDatasetCollector):
    source = SOURCE

    def iter_records(
        self, checkpoint: Dict[str, Any]
    ) -> Iterator[Tuple[NormalizedDatasetRecord, Dict[str, Any]]]:
        page = max(parse_int(checkpoint.get("page")) or 1, 1)
        start_page = page
        resume_gate = ResumeGate(checkpoint.get("last_saved_source_dataset_key"))

        while True:
            list_url = self.settings.aihub_list_url_template.format(page=page)
            soup = self.get_soup(list_url)
            detail_links = self._extract_detail_links(soup)
            if not detail_links:
                break

            for detail_url in detail_links:
                source_key = self._extract_dataset_id(detail_url)
                if not source_key:
                    continue
                if page == start_page and not resume_gate.allow(source_key):
                    continue

                try:
                    detail_soup = self.get_soup(detail_url)
                    yield (
                        self._normalize(source_key, detail_url, detail_soup),
                        {"page": page},
                    )
                except Exception as exc:
                    self.note_failure(source_key, exc)
                    continue

            page += 1

    def _extract_detail_links(self, soup: BeautifulSoup) -> List[str]:
        result: List[str] = []
        seen: Set[str] = set()
        for anchor in soup.select('a[href*="view.do"][href*="dataSetSn="]'):
            href = anchor.get("href")
            if not href:
                continue
            url = urljoin(self.source.base_url, href)
            if url in seen:
                continue
            seen.add(url)
            result.append(url)
        return result

    def _extract_dataset_id(self, detail_url: str) -> Optional[str]:
        parsed = urlparse(detail_url)
        query = parse_qs(parsed.query)
        values = (
            query.get("dataSetSn") or query.get("datasetSn") or query.get("dataSetNo")
        )
        if values:
            return clean_text(values[0])
        match = re.search(r"dataSetSn=(\d+)", detail_url)
        return match.group(1) if match else None

    def _normalize(
        self, source_key: str, detail_url: str, soup: BeautifulSoup
    ) -> NormalizedDatasetRecord:
        text = soup.get_text(" ", strip=True)
        kv = parse_kv_text_block(text)
        sections = self._extract_sections(text)

        og_title = soup.select_one("meta[property='og:title']")
        title = self._pick_title(
            clean_text(og_title.get("content") if og_title else None),
            *(
                clean_text(soup.title.get_text(" ", strip=True))
                if soup.title
                else None,
            ),
            self._extract_title_from_page_text(text),
            *(clean_text(kv.get("데이터셋명")) if "데이터셋명" in kv else None,),
            *(clean_text(kv.get("데이터셋 이름")) if "데이터셋 이름" in kv else None,),
            *(clean_text(kv.get("데이터명")) if "데이터명" in kv else None,),
            *[
                clean_text(tag.get_text(" ", strip=True))
                for selector in ["h1", "h2", "h3", ".title", ".subj"]
                for tag in [soup.select_one(selector)]
                if tag is not None
            ],
        )
        title = title or source_key

        description_candidates = [
            sections.get("데이터 개요"),
            sections.get("소개"),
            sections.get("메타데이터 구조표"),
            sections.get("데이터 통계"),
            sections.get("활용서비스"),
            clean_text(
                soup.select_one("meta[name='description']").get("content")
                if soup.select_one("meta[name='description']")
                else None
            ),
        ]
        description = self._pick_description(*description_candidates)

        hashtags = extract_hashtags(text)
        inline_tags = self._safe_terms(
            hashtags
            + self._split_mixed(kv.get("태그"))
            + self._split_mixed(kv.get("분야"))
            + self._split_mixed(kv.get("유형"))
            + self._split_mixed(kv.get("서비스 분야"))
            + self._split_mixed(kv.get("데이터 영역"))
            + self._split_mixed(kv.get("데이터 형식"))
            + self._split_mixed(kv.get("데이터 유형"))
        )

        domains = self._safe_terms(
            self._split_mixed(kv.get("서비스 분야"))
            + self._split_mixed(kv.get("데이터 영역"))
            + self._split_mixed(kv.get("분야")),
            max_items=24,
            max_len=80,
        )
        tasks = self._safe_terms(
            self._split_mixed(kv.get("라벨링 유형"))
            + self._split_mixed(kv.get("라벨링 포맷")),
            max_items=24,
            max_len=80,
        )

        creators = []
        for key, role in [
            ("주관기관", "주관기관"),
            ("수행기관", "수행기관"),
            ("데이터 출처", "데이터 출처"),
        ]:
            name = clean_text(kv.get(key))
            if name:
                creators.append({"name": name, "role": role})

        download_links = []
        for anchor in soup.select("a[href]"):
            href = clean_text(anchor.get("href"))
            if not href:
                continue
            if any(
                token in href.lower()
                for token in ["download", "down", "file", ".zip", ".csv", ".json"]
            ):
                download_links.append(urljoin(detail_url, href))
        resources = [
            {
                "url": url,
                "title": title,
                "format": kv.get("데이터 포맷")
                or kv.get("데이터 형식")
                or kv.get("라벨링 포맷"),
            }
            for url in unique_strings(download_links)
        ]
        if not resources:
            resources.append(
                {
                    "url": detail_url,
                    "title": title,
                    "format": kv.get("데이터 포맷")
                    or kv.get("데이터 형식")
                    or kv.get("라벨링 포맷"),
                }
            )

        metrics: Dict[str, Any] = {
            "view_count": parse_int(kv.get("조회수")),
            "download_count": parse_int(kv.get("다운로드")),
        }
        performance_text = sections.get("데이터 성능 지표")
        if performance_text:
            metrics["performance_excerpt"] = performance_text[:1000]

        updated_at = parse_datetime(kv.get("갱신년월"))
        source_version = clean_text(kv.get("구축년도"))

        return NormalizedDatasetRecord(
            source_dataset_key=source_key,
            canonical_url=detail_url,
            landing_url=detail_url,
            title=title,
            description_short=description,
            description_long=description,
            publisher_name=clean_text(
                kv.get("주관기관") or kv.get("수행기관") or kv.get("데이터 출처")
            ),
            domains=domains,
            tasks=tasks,
            modalities=guess_modalities_from_text(
                title, description, inline_tags, [r.get("format") for r in resources]
            ),
            tags=inline_tags,
            languages=[],
            license_name=None,
            license_url=None,
            access_type="APPROVAL",
            login_required=True,
            approval_required=True,
            payment_required=False,
            is_restricted=True,
            source_created_at=None,
            source_updated_at=updated_at,
            source_version=source_version,
            row_count=None,
            dataset_size_bytes=None,
            creators_json=creators,
            resources_json=resources,
            schema_json={
                "metadata_structure": sections.get("메타데이터 구조표"),
                "annotation_format": sections.get("어노테이션 포맷 및 데이터 구조"),
            },
            metrics_json=metrics,
            extra_json={
                "detail_kv": kv,
                "sections": sections,
                "data_format": kv.get("데이터 포맷") or kv.get("데이터 형식"),
                "labeling_type": kv.get("라벨링 유형"),
                "labeling_format": kv.get("라벨링 포맷"),
                "build_amount": kv.get("구축량"),
            },
            raw_json={"page_text": text},
        )

    def _extract_sections(self, text: str) -> Dict[str, str]:
        normalized = clean_text(text) or ""
        if not normalized:
            return {}
        positions: List[Tuple[int, str]] = []
        for section_name in _SECTION_NAMES:
            for match in re.finditer(re.escape(section_name), normalized):
                positions.append((match.start(), section_name))
        positions.sort(key=lambda item: item[0])
        if not positions:
            return {}
        result: Dict[str, str] = {}
        for idx, (start, name) in enumerate(positions):
            end = positions[idx + 1][0] if idx + 1 < len(positions) else len(normalized)
            value = normalized[start + len(name) : end].strip(" :\n\t")
            if value:
                result[name] = value
        return result

    def _pick_title(self, *candidates: Optional[str]) -> Optional[str]:
        cleaned_candidates: List[str] = []
        for candidate in candidates:
            text = clean_text(candidate)
            if not text:
                continue
            if text not in cleaned_candidates:
                cleaned_candidates.append(text)

        if not cleaned_candidates:
            return None

        non_generic = [
            text for text in cleaned_candidates if not self._is_generic_site_title(text)
        ]
        if not non_generic:
            return cleaned_candidates[0]

        non_noisy = [text for text in non_generic if not self._is_noisy_title(text)]
        if non_noisy:
            return min(non_noisy, key=len)
        return min(non_generic, key=len)

    def _is_generic_site_title(self, title: str) -> bool:
        text = clean_text(title)
        if not text:
            return True
        lowered = text.casefold()
        normalized = re.sub(r"[^a-z0-9가-힣]+", "", lowered)
        if normalized in {"aihub", "인공지능허브"}:
            return True
        if lowered in _GENERIC_TITLE_TOKENS:
            return True
        separator_tokens = ["|", "·", ":", "/"]
        if any(sep in lowered for sep in separator_tokens):
            first_idx = min(
                idx
                for idx in (lowered.find(sep) for sep in separator_tokens)
                if idx >= 0
            )
            head = lowered[:first_idx].strip()
            if head in _GENERIC_TITLE_TOKENS:
                return True
        return False

    def _is_noisy_title(self, title: str) -> bool:
        text = clean_text(title)
        if not text:
            return True
        if len(text) > 180:
            return True
        lowered = text.casefold()
        noise_tokens = [
            "input data",
            "output data",
            "data i/o",
            "성능 지표",
            "설명서",
            "어노테이션 포맷",
            "top-1 accuracy",
        ]
        hit_count = sum(1 for token in noise_tokens if token in lowered)
        return hit_count >= 2

    def _extract_title_from_page_text(self, page_text: str) -> Optional[str]:
        text = clean_text(page_text) or ""
        if not text:
            return None

        match = re.search(
            r"NEW\s+(.+?)(?:\s+분야\b|\s+유형\b|\s+구축년도\b|\s+갱신년월\b|\s+조회수\b)",
            text,
        )
        if match:
            return clean_text(match.group(1))

        match = re.search(
            r"데이터셋명\s*[:：]?\s*(.+?)(?:\s+분야\b|\s+유형\b|\s+조회수\b)",
            text,
        )
        if match:
            return clean_text(match.group(1))

        return None

    def _pick_description(self, *candidates: Optional[str]) -> Optional[str]:
        for candidate in candidates:
            text = clean_text(candidate)
            if not text:
                continue
            if self._is_noisy_description(text):
                continue
            return text
        return None

    def _is_noisy_description(self, text: str) -> bool:
        lowered = text.casefold()
        noise_tokens = [
            "개인정보보호 책임자",
            "이용약관",
            "개인정보처리방침",
            "family site",
            "webwatch",
            "contact.",
            "한국지능정보사회진흥원",
            "사이트맵",
            "웹접근성",
        ]
        hit_count = sum(1 for token in noise_tokens if token in lowered)
        if hit_count >= 2:
            return True
        if len(text) > 1200 and hit_count >= 1:
            return True
        return False

    def _split_mixed(self, value: Any) -> List[str]:
        text = clean_text(value)
        if not text:
            return []
        parts = re.split(r"[,/|#>]", text)
        return unique_strings(part.strip() for part in parts)

    def _safe_terms(
        self, values: List[str], *, max_items: int = 48, max_len: int = 120
    ) -> List[str]:
        result: List[str] = []
        for term in unique_strings(values):
            if not term:
                continue
            normalized = term if len(term) <= max_len else term[:max_len]
            result.append(normalized)
            if len(result) >= max_items:
                break
        return result
