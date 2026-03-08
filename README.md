# dataset_metadata_ingest

네가 만든 `dataset` / `collection_dataset` / `dataset_source` 구조를 기준으로, 아래 10개 소스의 **데이터셋 메타데이터를 수집해서 PostgreSQL `dataset` 테이블에 upsert** 하는 1단계 구현이다.

대상 소스는 다음 10개다.

1. Hugging Face
2. 공공데이터포털
3. Figshare
4. Harvard Dataverse
5. Kaggle
6. AI Hub
7. AWS Open Data Registry
8. Zenodo
9. data.gov catalog
10. data.europa.eu

이 저장소의 목표는 **“사이트별 메타데이터 구조가 제각각이어도, 최대한 많은 필드를 공통 `dataset` 테이블에 정규화해서 넣는 것”** 이다.

또한 2단계(FastAPI + RAG 서버에서 비동기적으로 돌리는 orchestrator)를 고려해서, 수집 로직을 **사이트별 collector 모듈**로 분리했다. 즉:

- 수집 방법을 수정할 때는 `sources/*.py` 만 수정
- API 서버 / 스케줄러를 붙일 때는 collector를 그대로 호출
- 중간 장애/중지 시 `collection_dataset.checkpoint_json` 과 `last_saved_source_dataset_key` 를 사용해 재개

---

## 1. 디렉터리 구조

```text
dataset_metadata_ingest/
├─ .env.example
├─ README.md
├─ requirements.txt
├─ sql/
│  └─ seed_dataset_sources.sql
└─ src/
   └─ metadata_ingest/
      ├─ __init__.py
      ├─ __main__.py
      ├─ base.py
      ├─ cli.py
      ├─ config.py
      ├─ db.py
      ├─ models.py
      ├─ utils.py
      └─ sources/
         ├─ __init__.py
         ├─ aihub.py
         ├─ aws_odr.py
         ├─ data_europa.py
         ├─ data_gov.py
         ├─ figshare.py
         ├─ harvard_dataverse.py
         ├─ huggingface.py
         ├─ kaggle.py
         ├─ public_data_portal.py
         └─ zenodo.py
```

---

## 2. 핵심 설계 원칙

### 2-1. 하나의 거대한 스크립트 대신, 사이트별 collector 분리

이 프로젝트는 일부 사이트는 공식 API가 잘 되어 있고, 일부는 HTML 크롤링이 필요하며, 일부는 JSON-LD / YAML / CKAN / RDF 스타일 메타데이터를 사용한다.

그래서 아래처럼 분리했다.

- `sources/huggingface.py`: Hugging Face 전용
- `sources/public_data_portal.py`: 공공데이터포털 전용
- `sources/figshare.py`: Figshare 전용
- `sources/harvard_dataverse.py`: Harvard Dataverse 전용
- `sources/kaggle.py`: Kaggle 전용
- `sources/aihub.py`: AI Hub 전용
- `sources/aws_odr.py`: AWS ODR 전용
- `sources/zenodo.py`: Zenodo 전용
- `sources/data_gov.py`: data.gov catalog 전용
- `sources/data_europa.py`: data.europa.eu 전용

이렇게 해두면 나중에 특정 사이트의 DOM 구조가 바뀌거나 API 응답이 바뀌어도 **그 collector만 수정**하면 된다.

### 2-2. DB upsert는 공통 계층에서 처리

각 collector는 원본 응답을 `NormalizedDatasetRecord` 로 바꾸는 데 집중한다.

실제 `dataset` 테이블 upsert는 `db.py` 에서 공통으로 처리한다.

장점:

- 모든 소스가 같은 DB 규칙을 사용함
- `record_hash`, `search_text`, `field_presence_json` 자동 생성
- 배열/JSONB/nullable 처리 일관성 확보
- `last_ingest_run_id` 자동 반영

### 2-3. `collection_dataset` 를 진짜로 활용하는 구조

이번 단계는 “단순히 긁어서 INSERT 하는 스크립트”가 아니라, **중간 장애 복구를 염두에 둔 collector** 다.

각 실행 시:

1. `dataset_source` row 보장
2. `collection_dataset` 에 RUNNING row 생성
3. 이전 실패/중지/부분성공 run의 checkpoint 읽기 (`resume=True` 일 때)
4. 주기적으로 `collected_count`, `upserted_count`, `failed_count`, `last_saved_source_dataset_key`, `checkpoint_json` 갱신
5. 정상 종료 시 `SUCCESS` 또는 `PARTIAL_SUCCESS`
6. 예외 시 `FAILED`
7. Ctrl+C 등 중지 시 `STOPPED`

### 2-4. 재개(resume)는 “마지막 성공 저장 지점” 기준

collector는 현재 run 중 **마지막으로 DB에 성공 저장된 dataset key** 기준으로 checkpoint를 남긴다.

즉,

- 일부 레코드 detail fetch 실패 → 실패는 카운트하고 계속 진행
- DB upsert 실패 → 그 레코드는 실패 처리, checkpoint는 이전 성공 지점 유지
- 프로세스 중단 → 다음 실행 시 마지막 성공 저장 지점 이후부터 재개 시도

정확히 말하면 소스마다 “페이지 + 마지막 저장 key” 기준으로 재개한다. 따라서 재시작 시 **같은 페이지를 한 번 더 읽고 마지막 key 전까지 skip** 하는 방식이 많다.

또한 기본 구현은 `SUCCESS` run을 resume 기준으로 삼지 않는다. 즉, **중단/실패/부분성공 복구에 초점을 둔 resume** 이다. 다음 단계에서 주기적 증분 수집 정책(예: 최근 N페이지 재스캔, `source_updated_at` 기반 증분 등)을 별도로 얹는 것을 전제로 했다.

이 방식의 장점은 다음과 같다.

- 구현이 단순함
- 중복 수집이 발생해도 `ON CONFLICT (dataset_source_id, source_dataset_key)` 로 안전
- 페이지 순서가 크게 흔들리지 않는 소스에서 매우 안정적

---

## 3. 설치 방법

### 3-1. 가상환경

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
```

### 3-2. 패키지 설치

```bash
pip install -r requirements.txt
```

### 3-3. 환경변수 파일 준비

```bash
cp .env.example .env
```

그 다음 `.env` 를 수정한다.

### 3-4. `dataset_source` seed (권장)

자동으로 `dataset_source` 를 upsert 하도록 만들어두었기 때문에, 이 SQL은 **필수는 아니다**.

하지만 운영 초기에 명확하게 시드해두는 편이 좋다.

```bash
psql "$DATABASE_URL" -f sql/seed_dataset_sources.sql
```

---

## 4. 환경변수 / API Key 넣는 위치

핵심만 먼저 정리하면 아래와 같다.

### 4-1. 반드시 넣어야 하는 것

```env
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/your_db
```

### 4-2. 네가 미리 발급받은 key를 넣는 곳

#### Kaggle

방법 1:

```env
KAGGLE_USERNAME=네_카글_유저명
KAGGLE_KEY=네_카글_api_key
```

방법 2:

- `~/.kaggle/kaggle.json` 사용
- 또는

```env
KAGGLE_CONFIG_DIR=/path/to/dir
```

를 설정해 `KAGGLE_CONFIG_DIR/kaggle.json` 을 읽게 할 수 있다.

#### data.gov catalog

```env
DATA_GOV_API_KEY=네_key
```

#### AWS ODR

```env
AWS_ODR_API_KEY=네_key
```

다만 **현재 기본 구현은 AWS ODR 메타데이터 수집에 이 키를 사용하지 않는다.**

이 구현은 공식 공개 registry/YAML 메타데이터를 기반으로 수집한다. 대신 GitHub rate limit 완화를 위해 아래를 권장한다.

```env
GITHUB_TOKEN=선택사항_권장
```

### 4-3. 없어도 되는 것

```env
HUGGINGFACE_TOKEN=
```

공개 메타데이터만 가져올 때는 없어도 된다.

### 4-4. IP 차단/거부 방지용 안전 실행 옵션(권장)

```env
SAFE_MODE_DEFAULT=true
DEFAULT_SAFE_LIMIT=20
MAX_SAFE_LIMIT=100
PER_SOURCE_COOLDOWN_SECONDS=20
BATCH_PAUSE_EVERY=10
BATCH_PAUSE_SECONDS=8
MIN_REQUEST_INTERVAL_SECONDS=1.2
REQUEST_INTERVAL_JITTER_SECONDS=0.5
RETRY_STATUS_CODES=408,409,425,429,500,502,503,504
RETRY_MAX_SLEEP_SECONDS=30
```

- `SAFE_MODE_DEFAULT=true` 이면 `--limit` 미지정 시 소스별 최대 `DEFAULT_SAFE_LIMIT` 건만 수집한다.
- 안전 모드에서 너무 큰 `--limit` 값을 넣으면 `MAX_SAFE_LIMIT` 까지만 실행한다.
- 요청 간 최소 간격 + 지터 + 배치 쿨다운을 함께 적용해 차단 가능성을 낮춘다.
- 안전 모드에서는 소스별 차단 강도에 맞춘 보수 프로파일(예: 크롤링 소스는 더 긴 간격/쿨다운)을 자동 적용한다.

---

## 5. 실행 방법

### 5-1. 단일 소스 실행

```bash
PYTHONPATH=src python -m metadata_ingest --source huggingface
PYTHONPATH=src python -m metadata_ingest --source public_data_portal
PYTHONPATH=src python -m metadata_ingest --source kaggle
```

### 5-2. 전체 실행

```bash
PYTHONPATH=src python -m metadata_ingest --source all
```

### 5-3. 처음부터 다시 긁기

```bash
PYTHONPATH=src python -m metadata_ingest --source figshare --from-scratch
```

### 5-4. 소스별 최대 저장 건수 제한

```bash
PYTHONPATH=src python -m metadata_ingest --source zenodo --limit 100
```

### 5-5. 지원 소스 목록 확인

```bash
PYTHONPATH=src python -m metadata_ingest --list-sources
```

### 5-6. 안전 모드 제어

```bash
# 안전 모드 강제(권장)
PYTHONPATH=src python -m metadata_ingest --source huggingface --safe

# 안전 모드 + 제한 건수 지정
PYTHONPATH=src python -m metadata_ingest --source all --safe --limit 30

# 안전 모드 비활성화(요청 간격/지터/배치 pause/소스간 cooldown 비활성)
PYTHONPATH=src python -m metadata_ingest --source all --no-safe
```

---

## 6. `dataset` 테이블 컬럼 매핑 정책

아래는 공통 정책이다.

| `dataset` 컬럼 | 저장 정책 |
|---|---|
| `dataset_source_id` | `dataset_source.source_code` 기준 자동 확보 |
| `last_ingest_run_id` | 현재 `collection_dataset.id` 자동 저장 |
| `source_dataset_key` | 소스 내 고유 키. 가장 중요함 |
| `record_hash` | 정규화된 record 내용 기반 SHA-256 자동 생성 |
| `canonical_url` | 가능한 한 dataset 대표 URL |
| `landing_url` | 사용자가 실제로 볼 수 있는 상세 페이지 |
| `title` | 가장 신뢰할 수 있는 제목 |
| `subtitle` | 있으면 저장, 없으면 NULL |
| `description_short` | 요약 설명 |
| `description_long` | 긴 설명 또는 상세 설명 |
| `search_text` | 제목/설명/태그/배열 필드들을 합쳐 자동 생성 |
| `publisher_name` | 기관/작성자/소유자/배포자 중 가장 적절한 값 |
| `domains` | 주제/분야/카테고리 |
| `tasks` | 태그/라벨 유형/키워드/작업 카테고리 |
| `modalities` | 텍스트/이미지/오디오/비디오/표/멀티모달 등을 휴리스틱 추론 |
| `tags` | 원문 태그 최대한 보존 |
| `languages` | 명시된 언어만 저장 |
| `license_name` / `license_url` | 가능한 경우 채움 |
| `commercial_use_allowed` | 라이선스 문자열 기반 휴리스틱 추정 |
| `access_type` | OPEN / REGISTERED / APPROVAL / PAID / RESTRICTED 중 하나 |
| `login_required` | 로그인이 필요한지 |
| `approval_required` | 승인/신청이 필요한지 |
| `payment_required` | 유료 여부 |
| `is_restricted` | 접근 제한이 있는지 |
| `source_created_at` | 원천 서비스 생성일 |
| `source_updated_at` | 원천 서비스 수정일 |
| `source_version` | 버전/리비전/SHA/버전 문자열 |
| `row_count` | 실제 row 수가 명시된 경우만 저장 |
| `dataset_size_bytes` | 실제 byte 크기를 구할 수 있을 때 저장 |
| `field_presence_json` | 자동 생성 |
| `creators_json` | 저자/기관/연락처/소유자 등 구조화 저장 |
| `resources_json` | 파일/배포 리소스/링크 목록 구조화 저장 |
| `schema_json` | 스키마/메타데이터 구조/특성 정보 |
| `metrics_json` | downloads/views/votes/file_count 등 |
| `extra_json` | 소스 특화 메타데이터 |
| `raw_json` | 원본 응답/원본 파싱 결과 |
| `status` | 기본 `ACTIVE` |
| `last_ingested_at` | upsert 시점 자동 저장 |

### 중요한 원칙

1. **정확히 알 수 없는 값은 억지로 채우지 않는다.**
2. **원본의 의미가 손상될 수 있으면 `extra_json` / `raw_json` 에 보존한다.**
3. **사이트별 구조가 다르더라도 `source_dataset_key` 는 반드시 안정적인 고유값을 사용한다.**
4. **실제 row 수가 아닌 “파일 수 / 구축량 / 문서 수”를 `row_count` 에 함부로 넣지 않으려고 노력했다.**

---

## 7. 사이트별 구현 상세

이 섹션이 이 README의 핵심이다.

아래 설명은 “어느 컬럼이 얼마나 잘 채워지는지”, “어떤 건 부분적/휴리스틱인지”, “무엇이 한계인지”를 사이트별로 정리한 것이다.

---

### 7-1. Hugging Face (`sources/huggingface.py`)

#### 수집 방식

- 공식 Python SDK `huggingface_hub.HfApi`
- `list_datasets(full=True, sort="last_modified")` 기반

#### 고유키

- `source_dataset_key = repo_id` (`owner/dataset_name`)

#### 재개 방식

- 페이지 cursor를 직접 저장하지 않고
- `last_saved_source_dataset_key` 기준 skip 방식

#### 잘 채워지는 필드

- `title`
- `description_short`, `description_long`
- `tags`
- `languages` (`language:` 태그 계열)
- `tasks` (`task_categories:` / `task_ids:` 태그)
- `license_name`
- `source_created_at`, `source_updated_at`
- `metrics_json.downloads`, `metrics_json.likes`
- `resources_json` (repo 파일 목록/siblings)
- `access_type` (`gated`, `private` 여부)

#### 부분적으로 채워지는 필드

- `row_count`: cardData 내부에 명시적 row 수가 있을 때만
- `dataset_size_bytes`: siblings/file size에서 추정 가능한 경우만
- `modalities`: 태그/설명/파일명 기반 휴리스틱
- `publisher_name`: author 또는 cardData publisher 기준

#### 한계

- SDK가 완전한 cursor resume 정보를 쉽게 노출하지 않아서, 중간 재개 시 처음부터 다시 순회 후 skip할 수 있다.
- 모든 HF dataset이 상세 card metadata를 충실하게 갖고 있지는 않다.
- `size_categories` 같은 범위형 문자열은 실제 row 수가 아니므로 `row_count` 로 저장하지 않는다.

#### 총평

HF는 **태그/작업/언어/좋아요/다운로드 등 ML 친화 메타데이터가 풍부**해서, `dataset` 테이블을 꽤 잘 채운다.

---

### 7-2. 공공데이터포털 (`sources/public_data_portal.py`)

#### 수집 방식

- 파일 데이터 목록 페이지 크롤링
- 상세 페이지 크롤링
- 상세 페이지에서 파생 가능한 schema.org JSON (`/catalog/{id}/fileData.json`) 보조 사용

#### 고유키

- `source_dataset_key = data/{id}/fileData.do` 의 `{id}`

#### 재개 방식

- `page` + `last_saved_source_dataset_key`

#### 잘 채워지는 필드

- `title`
- `description_short`, `description_long`
- `publisher_name`
- `domains` (분류/분야)
- `tasks` / `tags` (keyword)
- `license_name`, `license_url`
- `row_count` (`All Rows` / `Whole Row`)
- `source_created_at`, `source_updated_at`
- `metrics_json.download_count`
- `resources_json` (distribution / 파일 포맷 / 상세 링크)

#### 부분적으로 채워지는 필드

- `dataset_size_bytes`: schema.org `contentSize` 가 있을 때만
- `access_type`: 무료/로그인 없이 다운로드 가능 여부 텍스트를 우선 반영, 일부 케이스는 휴리스틱
- `modalities`: 파일 확장자/설명 기반 추정

#### 한계

- HTML 구조가 바뀌면 selector와 텍스트 복구 로직을 손봐야 한다.
- 상세 페이지 안에 “자동 변환 OpenAPI 정보”가 함께 표시되므로, 파일 데이터 메타데이터와 섞이지 않도록 주의가 필요하다.
- 일부 필드는 번역 페이지/한글 페이지에 따라 라벨이 다를 수 있다.

#### 총평

공공데이터포털의 파일 데이터는 **파일형 데이터셋에 필요한 기본 메타데이터(행 수, 확장자, 키워드, 등록/수정일, 라이선스 범위)** 를 비교적 잘 확보할 수 있다.

---

### 7-3. Figshare (`sources/figshare.py`)

#### 수집 방식

- 공식 REST API
- `/v2/articles?item_type=3` 로 dataset만 수집
- `/v2/articles/{id}` 로 상세 보강

#### 고유키

- `source_dataset_key = article id`

#### 잘 채워지는 필드

- `title`
- `description_short`, `description_long`
- `license_name`, `license_url`
- `source_created_at`, `source_updated_at`
- `creators_json` (authors)
- `resources_json` (files + download_url + size)
- `dataset_size_bytes`
- `metrics_json.views`, `downloads`, `shares`, `citations`
- `tags` / `domains` (categories)

#### 부분적으로 채워지는 필드

- `languages`: 있을 때만
- `publisher_name`: group/institution/owner 중 가장 그럴듯한 값
- `modalities`: 파일명/설명 기반 추정

#### 한계

- 논문/포스터/미디어 등 다양한 article 타입이 존재하지만, 현재는 dataset(item_type=3)만 대상으로 필터링한다.
- 실제 row 수는 보통 제공되지 않아 `row_count` 는 대부분 NULL이다.

#### 총평

Figshare는 **파일 메타데이터와 저자 정보가 매우 좋고**, 라이선스도 비교적 명확해서 `resources_json` / `creators_json` 품질이 높다.

---

### 7-4. Harvard Dataverse (`sources/harvard_dataverse.py`)

#### 수집 방식

- Search API로 dataset 목록 수집
- Native API로 persistentId 기준 상세 조회

#### 고유키

- `source_dataset_key = persistentId / global_id`

#### 잘 채워지는 필드

- `title`, `subtitle`
- `description_short`, `description_long`
- `domains` (subjects)
- `tasks` (keywords)
- `languages`
- `source_created_at`, `source_updated_at`
- `source_version`
- `creators_json` (authors, producers, datasetContact)
- `resources_json` (파일 목록, contentType, filesize, restricted 여부)
- `metrics_json.file_count`

#### 부분적으로 채워지는 필드

- `license_name`, `license_url`: 상세 JSON 구조에 명시될 때만
- `publisher_name`: producer > author > ownerName 순으로 결정
- `access_type`: restricted file 존재 여부 기준으로 판정
- `schema_json`: 메타데이터 블록 이름 / citation field 이름 위주

#### 한계

- Dataverse의 메타데이터 블록 구조가 복합(compound) 형태라서, 모든 custom field를 완벽하게 평탄화하지는 않았다.
- file_count는 잘 얻어도 실제 row_count는 보통 알 수 없어 `row_count` 는 NULL로 둔다.

#### 총평

Dataverse는 **학술형 데이터셋 메타데이터가 매우 풍부**해서 설명, 저자, 주제, 파일 정보는 상당히 잘 채워진다.

---

### 7-5. Kaggle (`sources/kaggle.py`)

#### 수집 방식

- 공식 `kaggle` Python 패키지 사용
- `dataset_list` → `dataset_view` → `dataset_list_files` 흐름

#### 고유키

- `source_dataset_key = owner/dataset`

#### 재개 방식

- `page` + `last_saved_source_dataset_key`

#### 잘 채워지는 필드

- `title`, `subtitle`
- `description_short`, `description_long`
- `publisher_name` (owner)
- `license_name`
- `source_updated_at`
- `dataset_size_bytes`
- `metrics_json.download_count`, `vote_count`, `view_count`, `usability_rating`
- `resources_json` (dataset file 목록이 노출되는 경우)

#### 부분적으로 채워지는 필드

- `source_created_at`: API 응답에 있을 때만
- `row_count`: API가 명시하는 경우만
- `tags`: Kaggle tags에 의존
- `access_type`: Kaggle는 실제 다운로드가 로그인 기반이므로 기본적으로 `REGISTERED` 로 잡음

#### 한계

- Kaggle API/패키지 버전에 따라 노출되는 필드가 조금씩 달라질 수 있다.
- 공개 메타데이터라도 실제 다운로드는 로그인/약관 수락이 필요한 경우가 많다.
- 패키지 내부 메서드 시그니처 차이를 줄이기 위해 reflection 기반 호출을 사용했다.

#### 총평

Kaggle은 **인기도/다운로드/평점류 지표가 좋다.** 반면 schema/정확한 row_count 쪽은 소스별 편차가 크다.

---

### 7-6. AI Hub (`sources/aihub.py`)

#### 수집 방식

- 목록 페이지 크롤링
- 상세 페이지 크롤링
- 구조화 라벨은 `parse_kv_text_block` + 섹션 텍스트 추출 혼합

#### 고유키

- `source_dataset_key = dataSetSn`

#### 잘 채워지는 필드

- `title`
- `description_short`, `description_long`
- `publisher_name` (주관기관/수행기관/데이터 출처)
- `domains` (서비스 분야/데이터 영역/분야)
- `tasks` (라벨링 유형/포맷)
- `tags` (해시태그 + 분류 라벨)
- `source_updated_at` (`갱신년월`)
- `source_version` (`구축년도`)
- `metrics_json.view_count`, `download_count`
- `schema_json.metadata_structure`, `annotation_format`
- `extra_json.sections`

#### 부분적으로 채워지는 필드

- `resources_json`: 실제 파일 다운로드 링크를 찾을 수 있으면 저장, 아니면 상세 페이지 링크를 resource로 남김
- `modalities`: 설명/데이터 형식/라벨링 포맷 기반 휴리스틱

#### 기본 접근정책 설정

AI Hub는 실제 데이터 다운로드가 보통 로그인/신청/승인 흐름을 동반하므로, 현재 기본 구현은 아래처럼 둔다.

- `access_type = APPROVAL`
- `login_required = True`
- `approval_required = True`
- `payment_required = False`
- `is_restricted = True`

#### 한계

- AI Hub는 페이지 구조 변경 가능성이 꽤 높다.
- “구축량”은 실제 row 수가 아니라 규모 표현일 가능성이 크므로 `row_count` 에 억지 저장하지 않았다.
- 섹션 파싱은 텍스트 기반이므로, 표/이미지 기반 상세 구조는 일부 손실될 수 있다.

#### 총평

AI Hub는 **국문 데이터셋 설명, 메타데이터 구조표, 성능 지표, 어노테이션 설명**을 `extra_json` / `schema_json` 쪽에 풍부하게 보존하기 좋다.

---

### 7-7. AWS Open Data Registry (`sources/aws_odr.py`)

#### 수집 방식

- 공식 registry repository의 dataset YAML을 사용
- GitHub API로 파일 목록 조회
- raw YAML 다운로드 후 파싱

#### 고유키

- `source_dataset_key = YAML 파일 path`

#### 잘 채워지는 필드

- `title`
- `description_short`, `description_long`
- `publisher_name` (`ManagedBy` / `Contact`)
- `tags`
- `domains` (`ADXCategories`)
- `resources_json` (ARN, Region, Type, Explore, RequesterPays, AccountRequired, ControlledAccess)
- `source_version` (Git blob SHA)
- `schema_json.update_frequency`
- `extra_json.documentation`, `citation`, `deprecated_notice`

#### 부분적으로 채워지는 필드

- `license_name`, `license_url`: YAML License 형태에 따라
- `access_type`: resource별 `ControlledAccess`, `AccountRequired`, `RequesterPays` 에 따른 판정
- `dataset_size_bytes`, `row_count`: YAML에 직접 있지 않으면 NULL

#### 현재 구현에서의 중요한 판단

네가 처음 요구사항에서 “AWS ODR - api(key 필요)” 라고 적어줬지만, **현재 이 구현은 AWS key 기반 메타데이터 API가 아니라, 공식 공개 registry YAML을 기준으로 구현했다.**

즉:

- 메타데이터 수집 자체는 공개 소스 기준으로 수행
- `AWS_ODR_API_KEY` 는 현재 기본 흐름에서 사용하지 않음
- 대신 GitHub rate limit 때문에 `GITHUB_TOKEN` 을 넣는 것을 권장

#### 총평

AWS ODR는 **파일 중심 데이터셋이 아니라 클라우드 리소스 중심 메타데이터** 에 강하다. `resources_json` 의 활용 가치가 매우 높다.

---

### 7-8. Zenodo (`sources/zenodo.py`)

#### 수집 방식

- 공식 REST API `/api/records`
- `q=resource_type.type:dataset` 기준 dataset만 수집

#### 고유키

- `source_dataset_key = record id`

#### 잘 채워지는 필드

- `title`
- `description_short`, `description_long`
- `license_name`
- `source_created_at`, `source_updated_at`
- `creators_json` (creators, contributors)
- `resources_json` (files, checksum, mimetype, size, created/updated)
- `dataset_size_bytes`
- `tags`, `domains` (communities / subjects / keywords)

#### 부분적으로 채워지는 필드

- `access_type`: `access_right` 기준
- `publisher_name`: publisher 또는 첫 creator 기준
- `metrics_json.views`, `downloads`: stats가 노출되는 경우만

#### 한계

- row_count는 거의 제공되지 않는다.
- 일부 레코드는 파일 없이 메타데이터만 풍부한 경우도 있다.

#### 총평

Zenodo는 **학술/연구 데이터셋과 파일 메타데이터** 측면에서 강하고, creators / contributors / DOI 류 정보가 유용하다.

---

### 7-9. data.gov catalog (`sources/data_gov.py`)

#### 수집 방식

- GSA의 data.gov catalog API (CKAN `package_search`)
- `x-api-key` 헤더 사용

#### 고유키

- `source_dataset_key = id` (없으면 name)

#### 잘 채워지는 필드

- `title`
- `description_short`, `description_long` (`notes`)
- `publisher_name` (organization / publisher / author)
- `tags`, `domains`, `tasks`
- `license_name`, `license_url`
- `source_created_at`, `source_updated_at`
- `resources_json` (url, format, mimetype, state)
- `schema_json.described_by`, `described_by_type`, `bureau_code`, `program_code`

#### 부분적으로 채워지는 필드

- `languages`: extras에 language가 있을 때만
- `access_type`: `accessLevel` / `isopen` 기준
- `dataset_size_bytes`, `row_count`: extras/resource에 있을 때만

#### 한계

- CKAN `extras` 는 기관마다 넣는 방식이 달라 품질 편차가 크다.
- `row_count` / `size` 는 대부분 비어 있을 가능성이 높다.

#### 총평

data.gov는 **공공기관 catalog 메타데이터 정규화** 에 강하고, `extras` 를 `extra_json` 에 잘 남겨두면 후처리 가치가 크다.

---

### 7-10. data.europa.eu (`sources/data_europa.py`)

#### 수집 방식

- Search API로 page 단위 목록 수집
- Registry API로 dataset JSON-LD 상세 조회
- JSON-LD/DCAT-AP 스타일을 일반화해서 파싱

#### 고유키

- `source_dataset_key = dataset UUID`

#### 잘 채워지는 필드

- `title`
- `description_short`, `description_long`
- `domains` (theme)
- `tasks` (keyword)
- `languages`
- `source_created_at`, `source_updated_at`
- `source_version`
- `creators_json` (creator, publisher, contactPoint)
- `resources_json` (distribution title/url/format/byteSize/license)
- `schema_json.conforms_to`

#### 부분적으로 채워지는 필드

- `license_name`, `license_url`: dataset 또는 distribution에 명시될 때
- `access_type`: accessRights 문자열 기반 휴리스틱
- `dataset_size_bytes`: distribution byteSize 합산 가능 시
- `publisher_name`: agent 노드 파싱 결과에 따라

#### 한계

- JSON-LD/DCAT-AP는 키 형태가 다양해서, 완벽한 RDF parser 수준까지는 구현하지 않았다.
- Search API 응답 구조가 버전에 따라 달라질 가능성을 고려해, 목록 추출 로직을 다소 유연하게 만들었다.
- UUID를 찾지 못하는 일부 비정형 응답은 skip될 수 있다.

#### 총평

data.europa.eu 는 **DCAT-AP / distribution / publisher / accessRights / conformsTo 류 메타데이터** 를 많이 줄 수 있는 좋은 소스다.

---

## 8. 어떤 사이트가 얼마나 잘 채워지나?

대략적인 체감 품질은 아래와 같다.

| 소스 | title/desc | tags/domain | creator/resource | license | size/date | access 판정 | 종합 |
|---|---|---:|---:|---:|---:|---:|---:|
| Hugging Face | 상 | 상 | 중 | 중 | 중 | 중 | 상 |
| 공공데이터포털 | 상 | 상 | 중 | 상 | 중 | 중 | 상 |
| Figshare | 상 | 중 | 상 | 상 | 상 | 중 | 상 |
| Harvard Dataverse | 상 | 상 | 상 | 중 | 중 | 중 | 상 |
| Kaggle | 상 | 중 | 중 | 중 | 중 | 중 | 중상 |
| AI Hub | 상 | 상 | 중 | 하 | 중 | 상 | 중상 |
| AWS ODR | 상 | 중 | 상 | 중 | 하 | 상 | 중상 |
| Zenodo | 상 | 중 | 상 | 중 | 상 | 중 | 상 |
| data.gov | 상 | 중 | 중 | 상 | 중 | 중 | 중상 |
| data.europa.eu | 상 | 상 | 상 | 중 | 중 | 중 | 상 |

설명:

- **상**: 꽤 안정적으로 잘 채워짐
- **중**: 소스마다 편차가 있거나 일부 휴리스틱 포함
- **하**: 현재 구조상 많이 비거나 의미 손실 가능성 있음

---

## 9. `collection_dataset` / resume 동작 상세

이 코드는 네가 신경쓴 `collection_dataset` 구조를 실제로 활용한다.

### 실행 시작 시

- `dataset_source` row를 보장한다.
- `resume=True` 면 최근 `RUNNING / FAILED / STOPPED / PARTIAL_SUCCESS` run의 checkpoint를 읽는다.
- 새 `collection_dataset` row를 `RUNNING` 으로 생성한다.

### 실행 중

- record 1개씩 `dataset` 에 upsert
- 각 record는 commit되어 저장 안정성이 높음
- `SAVE_EVERY` 마다 run progress를 갱신

### 정상 종료 시

- 실패가 0건이면 `SUCCESS`
- 일부 실패가 있으면 `PARTIAL_SUCCESS`

### 비정상 종료 시

- 예외면 `FAILED`
- 사용자 중단(KeyboardInterrupt)면 `STOPPED`

### 왜 record 단위 commit인가?

장점:

- 크래시가 나도 직전까지 저장된 데이터는 최대한 보존
- 이후 재실행 시 upsert로 중복 안전

단점:

- 대량 수집에서는 batch commit보다 느릴 수 있음

현재 단계(1단계)에서는 **속도보다 안정성과 resume 친화성** 을 우선했다.

---

## 10. 아직 의도적으로 남겨둔 한계

이건 솔직하게 적는다.

### 10-1. 일부 소스는 live endpoint 테스트를 이 환경에서 직접 끝까지 돌리지는 못했다

현재 내가 이 산출물을 만든 환경에서는 외부 인터넷에 직접 붙여서 전체 수집을 실운영 수준으로 끝까지 돌릴 수 없었다.

즉,

- 문법 검증은 완료
- 공식 문서/실제 공개 페이지 구조를 기준으로 collector를 작성
- 하지만 실제 운영 환경에서는 **첫 실행 후 소수의 selector/응답 필드 보정** 이 필요할 수 있다

이건 특히 아래 4개가 가능성이 있다.

- 공공데이터포털 (HTML 구조 변동)
- AI Hub (HTML 구조 변동)
- Kaggle (패키지 버전별 필드 차이)
- data.europa.eu (Search API 응답 포맷 미세 차이)

### 10-2. Step 2용 완전한 비동기 orchestrator는 아직 안 만들었다

이번 범위는 1단계이므로, 수집 로직까지만 구현했다.

다만 2단계에서 붙이기 좋도록 이미 다음을 고려해두었다.

- collector 분리
- 공통 DB layer 분리
- run/checkpoint/resume 구조 사용
- CLI 진입점 분리

즉, 다음 단계에서는 FastAPI에서 이 collector들을 **background task / worker / scheduler** 형태로 감싸기만 하면 된다.

### 10-3. `dataset_chunk` 는 아직 안 건드린다

이번 구현은 오직 `dataset` 저장까지다.

즉:

- 임베딩 생성 안 함
- chunk 생성 안 함
- `dataset_chunk` 미사용

이건 네가 말한 1단계 범위에 맞춘 것이다.

---

## 11. 운영 팁

### 11-1. 초반에는 소스별로 적은 건수로 테스트해라

예:

```bash
PYTHONPATH=src python -m metadata_ingest --source public_data_portal --limit 20
PYTHONPATH=src python -m metadata_ingest --source aihub --limit 20
PYTHONPATH=src python -m metadata_ingest --source kaggle --limit 20
```

이렇게 돌려서 `raw_json`, `extra_json`, `field_presence_json` 를 직접 보면서 보정하는 게 좋다.

### 11-2. 공공/크롤링 계열은 selector가 바뀌면 해당 모듈만 수정해라

- 공공데이터포털 → `sources/public_data_portal.py`
- AI Hub → `sources/aihub.py`

### 11-3. `raw_json` 을 버리지 마라

실무에서는 오히려 `raw_json` 이 매우 중요하다.

이유:

- 초기에 놓친 필드를 나중에 다시 뽑아낼 수 있음
- 사이트별 변동을 추적 가능
- 후처리 룰을 고도화하기 좋음

---

## 12. 다음 단계(너와 내가 이어서 하면 좋은 것)

이번 단계 다음으로는 아래 순서가 가장 자연스럽다.

1. **실제 각 소스별로 10~100건씩 테스트 실행**
2. selector/필드명 보정
3. 소스별 rate-limit / timeout 조정
4. FastAPI background task / scheduler 연결
5. 필요하면 chunk/embedding 파이프라인 분리 연결

특히 2단계에서 중요할 부분은 다음이다.

- run 중복 실행 방지
- source별 동시 실행 제한
- STOP 요청 처리
- health check / 최근 run 상태 조회 API
- 실패 소스만 재시도

---

## 13. 요약

이 구현은 다음 요구를 만족하도록 만들었다.

- 10개 사이트 각각 다른 방식으로 수집
- 가능한 많은 메타데이터를 `dataset` 공통 스키마에 정규화
- 부족한 값은 NULL 또는 빈 배열/JSON으로 처리
- 원본 정보는 `raw_json` / `extra_json` 에 최대한 보존
- `collection_dataset` 를 실제로 써서 resume 가능 구조 확보
- 향후 FastAPI orchestrator와 잘 이어질 수 있도록 collector 분리

즉, 이건 “그냥 긁는 코드”가 아니라, **네 DB 설계 철학을 살린 1단계 수집 파이프라인의 뼈대** 다.
