from __future__ import annotations

import hashlib
import html
import json
import math
import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from dateutil import parser as dtparser


_WHITESPACE_RE = re.compile(r"\s+")
_TAG_RE = re.compile(r"<[^>]+>")
_UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)
_BYTES_RE = re.compile(
    r"(?P<number>\d+(?:\.\d+)?)\s*(?P<unit>B|KB|MB|GB|TB|PB|KiB|MiB|GiB|TiB|PiB)",
    re.IGNORECASE,
)


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()



def ensure_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]



def clean_text(text: Any) -> Optional[str]:
    if text is None:
        return None
    if isinstance(text, (list, tuple, set)):
        text = " ".join(str(item) for item in text if item is not None)
    elif isinstance(text, dict):
        text = json_dumps_stable(text)
    elif not isinstance(text, str):
        text = str(text)
    text = html.unescape(text)
    text = _TAG_RE.sub(" ", text)
    text = _WHITESPACE_RE.sub(" ", text).strip()
    return text or None



def strip_html(raw_html: Optional[str]) -> Optional[str]:
    if raw_html is None:
        return None
    soup = BeautifulSoup(raw_html, "lxml")
    text = soup.get_text(" ", strip=True)
    return clean_text(text)



def text_to_lines(text: Optional[str]) -> List[str]:
    if not text:
        return []
    text = html.unescape(text)
    return [line.strip() for line in text.splitlines() if line.strip()]



def unique_strings(values: Iterable[Any]) -> List[str]:
    seen = set()
    result: List[str] = []
    for value in values:
        if value is None:
            continue
        if isinstance(value, (dict, list, tuple, set)):
            continue
        text = clean_text(str(value))
        if not text:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(text)
    return result



def compact_dict(data: Dict[str, Any]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for key, value in data.items():
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        if isinstance(value, (list, tuple, set)) and len(value) == 0:
            continue
        if isinstance(value, dict):
            nested = compact_dict(value)
            if not nested:
                continue
            result[key] = nested
            continue
        result[key] = value
    return result



def json_dumps_stable(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)



def sha256_json(value: Any) -> str:
    payload = json_dumps_stable(value).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()



def parse_datetime(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    try:
        dt = dtparser.parse(str(value))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()



def parse_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return None
        return int(value)
    text = clean_text(str(value))
    if not text:
        return None
    text = text.replace(",", "")
    match = re.search(r"-?\d+", text)
    if not match:
        return None
    try:
        return int(match.group(0))
    except ValueError:
        return None



def parse_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except Exception:
            return None
    text = clean_text(str(value))
    if not text:
        return None
    text = text.replace(",", "")
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None



def parse_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = clean_text(str(value))
    if not text:
        return None
    lowered = text.casefold()
    truthy = {
        "true",
        "1",
        "yes",
        "y",
        "open",
        "public",
        "free",
        "available",
        "허용",
        "가능",
        "예",
        "있음",
        "무료",
    }
    falsy = {
        "false",
        "0",
        "no",
        "n",
        "none",
        "private",
        "restricted",
        "불가",
        "아니오",
        "없음",
        "비공개",
    }
    if lowered in truthy:
        return True
    if lowered in falsy:
        return False
    return None



def parse_bytes(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float):
        return int(value) if value >= 0 else None
    text = clean_text(str(value))
    if not text:
        return None
    match = _BYTES_RE.search(text)
    if not match:
        # 숫자만 있으면 bytes로 간주
        return parse_int(text)
    number = float(match.group("number"))
    unit = match.group("unit").upper()
    factor_map = {
        "B": 1,
        "KB": 1000,
        "MB": 1000**2,
        "GB": 1000**3,
        "TB": 1000**4,
        "PB": 1000**5,
        "KIB": 1024,
        "MIB": 1024**2,
        "GIB": 1024**3,
        "TIB": 1024**4,
        "PIB": 1024**5,
    }
    return int(number * factor_map[unit])



def first_non_empty(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        if isinstance(value, (list, tuple, set, dict)) and len(value) == 0:
            continue
        return value
    return None



def build_search_text(*parts: Any) -> Optional[str]:
    tokens: List[str] = []
    for part in parts:
        if part is None:
            continue
        if isinstance(part, str):
            cleaned = clean_text(part)
            if cleaned:
                tokens.append(cleaned)
            continue
        if isinstance(part, dict):
            cleaned = clean_text(json_dumps_stable(part))
            if cleaned:
                tokens.append(cleaned)
            continue
        if isinstance(part, (list, tuple, set)):
            for item in part:
                cleaned = clean_text(str(item))
                if cleaned:
                    tokens.append(cleaned)
            continue
        cleaned = clean_text(str(part))
        if cleaned:
            tokens.append(cleaned)
    result = " \n".join(unique_strings(tokens))
    return result or None



def domain_from_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    try:
        parsed = urlparse(url)
    except Exception:
        return None
    domain = parsed.netloc.lower().strip()
    return domain or None



def domains_from_urls(urls: Iterable[Optional[str]]) -> List[str]:
    return unique_strings(domain_from_url(url) for url in urls if url)



def build_field_presence(record: Dict[str, Any]) -> Dict[str, Any]:
    scalar_fields: Dict[str, bool] = {}
    array_lengths: Dict[str, int] = {}
    json_keys: Dict[str, int] = {}

    for key, value in record.items():
        if key in {"field_presence_json", "raw_json"}:
            continue
        if isinstance(value, list):
            array_lengths[key] = len(value)
            scalar_fields[key] = len(value) > 0
        elif isinstance(value, dict):
            json_keys[key] = len(value)
            scalar_fields[key] = len(value) > 0
        else:
            scalar_fields[key] = value is not None and (not isinstance(value, str) or bool(value.strip()))

    return {
        "has": scalar_fields,
        "array_lengths": array_lengths,
        "json_key_counts": json_keys,
    }



def guess_modalities_from_text(*parts: Any) -> List[str]:
    text = build_search_text(*parts) or ""
    lowered = text.casefold()
    mapping = {
        "text": ["text", "문자", "텍스트", "corpus", "qa", "translation", "document", "pdf"],
        "image": ["image", "이미지", "사진", "jpg", "jpeg", "png", "segmentation", "object detection"],
        "audio": ["audio", "음성", "소리", "speech", "wav", "mp3"],
        "video": ["video", "영상", "동영상", "mp4", "avi"],
        "tabular": ["csv", "xlsx", "xls", "parquet", "table", "tabular", "structured", "정형", "표"],
        "multimodal": ["multimodal", "multi-modal", "멀티모달"],
        "geospatial": ["geospatial", "geojson", "shp", "wms", "지도", "공간", "satellite", "raster"],
        "time-series": ["time series", "timeseries", "시계열"],
        "3d": ["3d", "point cloud", "obj", "fbx", "mesh", "lidar"],
    }
    result: List[str] = []
    for modality, keywords in mapping.items():
        if any(keyword in lowered for keyword in keywords):
            result.append(modality)
    return unique_strings(result)



def guess_tasks_from_tags(tags: Iterable[str]) -> List[str]:
    values = [clean_text(tag) for tag in tags]
    values = [v for v in values if v]
    return unique_strings(values)



def infer_commercial_use_from_license(license_name: Optional[str], license_url: Optional[str] = None) -> Optional[bool]:
    text = (license_name or "") + " " + (license_url or "")
    lowered = text.casefold()
    if not lowered.strip():
        return None

    disallow_tokens = ["nc", "non-commercial", "비영리", "not for commercial"]
    allow_tokens = [
        "mit",
        "apache",
        "bsd",
        "cc-by",
        "cc0",
        "odc",
        "pddl",
        "european union public licence",
        "gpl",
        "lgpl",
        "mpl",
    ]

    if any(token in lowered for token in disallow_tokens):
        return False
    if any(token in lowered for token in allow_tokens):
        return True
    return None



def extract_uuid(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    match = _UUID_RE.search(value)
    return match.group(0) if match else None



def extract_hashtags(text: Optional[str]) -> List[str]:
    if not text:
        return []
    return unique_strings(match.group(1) for match in re.finditer(r"#([^#\s]+(?:\s+[^#\s]+)*)", text))



def chunked(seq: Sequence[Any], size: int) -> Iterator[Sequence[Any]]:
    if size <= 0:
        raise ValueError("size must be >= 1")
    for idx in range(0, len(seq), size):
        yield seq[idx : idx + size]



def safe_get(data: Any, *path: Any) -> Any:
    current = data
    for key in path:
        if current is None:
            return None
        if isinstance(current, dict):
            current = current.get(key)
            continue
        if isinstance(current, list):
            try:
                current = current[key]
            except Exception:
                return None
            continue
        return None
    return current



def to_serializable(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime):
        return parse_datetime(value)
    if isinstance(value, dict):
        return {str(k): to_serializable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [to_serializable(v) for v in value]
    if hasattr(value, "__dict__"):
        return to_serializable(vars(value))
    return str(value)



def parse_kv_text_block(text: Optional[str]) -> Dict[str, str]:
    """표가 평문으로 풀려 나온 경우를 최대한 복구한다.

    예:
        File Name foo\nClassified Bar\nUpdate Cycle Monthly
    """
    if not text:
        return {}
    labels = [
        "File Name",
        "Classified",
        "Department",
        "Department NO",
        "Retention",
        "Collected by",
        "Update Cycle",
        "Next Registration Date",
        "Media Type",
        "All Rows",
        "File Extension",
        "Download",
        "Data Limit",
        "Keyword",
        "Registered",
        "Edited",
        "Provided",
        "Description",
        "Payment",
        "Criteria for Additional Costs",
        "Scope of License",
        "Service",
        "Classification System",
        "Provider",
        "Management Agency",
        "Management agency phone number",
        "Basis For Retention",
        "Collection Method",
        "Next Enrollment Date",
        "Whole Row",
        "Extension",
        "Application For Use",
        "Enrollment",
        "Correction",
        "Form Of Provision",
        "Explanation",
        "Other Notes",
        "Charge Standard And Unit",
        "Scope Of Use",
        "Usage Specification",
        # AI Hub
        "구축년도",
        "갱신년월",
        "조회수",
        "다운로드",
        "데이터 유형",
        "데이터 포맷",
        "데이터 출처",
        "라벨링 유형",
        "라벨링 포맷",
        "서비스 분야",
        "데이터 영역",
        "데이터 형식",
        "구축량",
        "주관기관",
        "수행기관",
        "분야",
        "유형",
        "태그",
    ]

    normalized = clean_text(text) or ""
    if not normalized:
        return {}

    positions: List[tuple[int, str]] = []
    for label in labels:
        for match in re.finditer(re.escape(label), normalized):
            positions.append((match.start(), label))
    positions.sort(key=lambda item: item[0])
    if not positions:
        return {}

    result: Dict[str, str] = {}
    for idx, (start, label) in enumerate(positions):
        end = positions[idx + 1][0] if idx + 1 < len(positions) else len(normalized)
        chunk = normalized[start:end].strip()
        value = chunk[len(label) :].strip(" :\n\t")
        if value:
            result[label] = value
    return result



def jsonld_value(value: Any) -> Any:
    if isinstance(value, dict):
        if "@value" in value:
            return value.get("@value")
        if "@id" in value and len(value) == 1:
            return value.get("@id")
    return value



def jsonld_first(data: Dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if key not in data:
            continue
        value = data[key]
        if isinstance(value, list):
            for item in value:
                unwrapped = jsonld_value(item)
                if isinstance(unwrapped, dict):
                    continue
                if unwrapped not in (None, ""):
                    return unwrapped
        else:
            unwrapped = jsonld_value(value)
            if unwrapped not in (None, ""):
                return unwrapped
    return None



def jsonld_collect_text(data: Dict[str, Any], keys: Iterable[str]) -> List[str]:
    result: List[str] = []
    for key in keys:
        if key not in data:
            continue
        value = data[key]
        for item in ensure_list(value):
            item = jsonld_value(item)
            if isinstance(item, dict):
                candidate = jsonld_first(item, ["prefLabel", "label", "name", "@id"])
            else:
                candidate = item
            cleaned = clean_text(str(candidate)) if candidate is not None else None
            if cleaned:
                result.append(cleaned)
    return unique_strings(result)



def merge_dicts(*values: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for value in values:
        if not value:
            continue
        result.update(value)
    return result
