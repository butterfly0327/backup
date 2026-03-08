-- =========================================================
-- PostgreSQL Final DDL Script
-- =========================================================

-- pgvector 사용 시 필요
CREATE EXTENSION IF NOT EXISTS vector;

-- =========================================================
-- 공통 함수: updated_at 자동 갱신
-- =========================================================
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- =========================================================
-- 1. user
-- =========================================================
CREATE TABLE "user" (
    id              BIGSERIAL PRIMARY KEY,
    ssafy_id        VARCHAR(100) NOT NULL UNIQUE,
    name            VARCHAR(50)  NOT NULL,
    email           VARCHAR(100) NOT NULL,
    edu             VARCHAR(50),
    ent_regn_cd     VARCHAR(50),
    retire_yn       VARCHAR(10),
    project         VARCHAR(100),
    role            VARCHAR(10)  NOT NULL DEFAULT 'USER',
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    is_deleted      BOOLEAN      NOT NULL DEFAULT FALSE,
    CONSTRAINT chk_user_role
        CHECK (role IN ('USER', 'ADMIN'))
);

-- =========================================================
-- 2-1. dataset_source
-- =========================================================
CREATE TABLE dataset_source (
    id                  SMALLSERIAL PRIMARY KEY,
    source_code         VARCHAR(30)  NOT NULL UNIQUE,
    source_name         VARCHAR(100) NOT NULL,
    base_url            TEXT         NOT NULL,
    collection_type     VARCHAR(20)  NOT NULL,
    is_active           BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_dataset_source_collection_type
        CHECK (collection_type IN ('API', 'CRAWL', 'FILE'))
);

-- =========================================================
-- 2-2. openapi_source
-- =========================================================
CREATE TABLE openapi_source (
    id                  SMALLSERIAL PRIMARY KEY,
    source_code         VARCHAR(30)  NOT NULL UNIQUE,
    source_name         VARCHAR(100) NOT NULL,
    base_url            TEXT         NOT NULL,
    collection_type     VARCHAR(20)  NOT NULL,
    is_active           BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_openapi_source_collection_type
        CHECK (collection_type IN ('API', 'CRAWL', 'FILE'))
);

-- =========================================================
-- 3. dataset
-- last_ingest_run_id FK는 collection_dataset 생성 후 ALTER TABLE로 추가
-- =========================================================
CREATE TABLE dataset (
    id                      BIGSERIAL PRIMARY KEY,
    dataset_source_id       SMALLINT    NOT NULL,
    last_ingest_run_id      BIGINT,
    source_dataset_key      TEXT        NOT NULL,
    record_hash             CHAR(64),
    canonical_url           TEXT,
    landing_url             TEXT,
    title                   TEXT,
    subtitle                TEXT,
    description_short       TEXT,
    description_long        TEXT,
    search_text             TEXT,
    publisher_name          TEXT,
    domains                 TEXT[]      NOT NULL DEFAULT ARRAY[]::TEXT[],
    tasks                   TEXT[]      NOT NULL DEFAULT ARRAY[]::TEXT[],
    modalities              TEXT[]      NOT NULL DEFAULT ARRAY[]::TEXT[],
    tags                    TEXT[]      NOT NULL DEFAULT ARRAY[]::TEXT[],
    languages               TEXT[]      NOT NULL DEFAULT ARRAY[]::TEXT[],
    license_name            TEXT,
    license_url             TEXT,
    commercial_use_allowed  BOOLEAN,
    access_type             VARCHAR(20),
    login_required          BOOLEAN,
    approval_required       BOOLEAN,
    payment_required        BOOLEAN,
    is_restricted           BOOLEAN,
    source_created_at       TIMESTAMPTZ,
    source_updated_at       TIMESTAMPTZ,
    source_version          TEXT,
    row_count               BIGINT,
    dataset_size_bytes      BIGINT,
    field_presence_json     JSONB       NOT NULL DEFAULT '{}'::JSONB,
    creators_json           JSONB       NOT NULL DEFAULT '[]'::JSONB,
    resources_json          JSONB       NOT NULL DEFAULT '[]'::JSONB,
    schema_json             JSONB       NOT NULL DEFAULT '{}'::JSONB,
    metrics_json            JSONB       NOT NULL DEFAULT '{}'::JSONB,
    extra_json              JSONB       NOT NULL DEFAULT '{}'::JSONB,
    raw_json                JSONB       NOT NULL DEFAULT '{}'::JSONB,
    status                  VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    last_ingested_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_dataset_source
        FOREIGN KEY (dataset_source_id) REFERENCES dataset_source(id),

    CONSTRAINT uq_dataset_source_key
        UNIQUE (dataset_source_id, source_dataset_key),

    CONSTRAINT chk_dataset_access_type
        CHECK (access_type IN ('OPEN', 'REGISTERED', 'APPROVAL', 'PAID', 'RESTRICTED', 'UNKNOWN')),

    CONSTRAINT chk_dataset_status
        CHECK (status IN ('ACTIVE', 'INACTIVE', 'DELETED', 'ERROR')),

    CONSTRAINT chk_dataset_row_count
        CHECK (row_count IS NULL OR row_count >= 0),

    CONSTRAINT chk_dataset_size_bytes
        CHECK (dataset_size_bytes IS NULL OR dataset_size_bytes >= 0)
);

-- =========================================================
-- 4. collection_dataset
-- =========================================================
CREATE TABLE collection_dataset (
    id                              BIGSERIAL PRIMARY KEY,
    dataset_source_id               SMALLINT    NOT NULL,
    parser_version                  VARCHAR(50) NOT NULL,
    run_started_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    run_finished_at                 TIMESTAMPTZ,
    status                          VARCHAR(20) NOT NULL,
    collected_count                 INTEGER     NOT NULL DEFAULT 0,
    upserted_count                  INTEGER     NOT NULL DEFAULT 0,
    failed_count                    INTEGER     NOT NULL DEFAULT 0,
    error_summary                   TEXT,
    last_saved_source_dataset_key   TEXT,
    checkpoint_json                 JSONB       NOT NULL DEFAULT '{}'::JSONB,
    created_at                      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_collection_dataset_source
        FOREIGN KEY (dataset_source_id) REFERENCES dataset_source(id),

    CONSTRAINT chk_collection_dataset_status
        CHECK (status IN ('RUNNING', 'SUCCESS', 'PARTIAL_SUCCESS', 'FAILED', 'STOPPED')),

    CONSTRAINT chk_collection_dataset_collected_count
        CHECK (collected_count >= 0),

    CONSTRAINT chk_collection_dataset_upserted_count
        CHECK (upserted_count >= 0),

    CONSTRAINT chk_collection_dataset_failed_count
        CHECK (failed_count >= 0)
);

-- dataset 와 collection_dataset 순환 참조 해결
ALTER TABLE dataset
ADD CONSTRAINT fk_dataset_last_ingest_run
FOREIGN KEY (last_ingest_run_id) REFERENCES collection_dataset(id)
ON DELETE SET NULL;

-- =========================================================
-- 6. open_api
-- =========================================================
CREATE TABLE open_api (
    id                  BIGSERIAL PRIMARY KEY,
    openapi_source_id   SMALLINT      NOT NULL,
    source_openapi_key  VARCHAR(255)  NOT NULL,
    name                VARCHAR(255)  NOT NULL,
    description         TEXT,
    provider            VARCHAR(100),
    base_url            VARCHAR(500)  NOT NULL,
    docs_url            VARCHAR(500),
    auth_type           VARCHAR(20)   NOT NULL DEFAULT 'NONE',
    category            VARCHAR(100),
    tags                TEXT[]        NOT NULL DEFAULT ARRAY[]::TEXT[],
    rate_limit          INTEGER,
    daily_limit         INTEGER,
    is_free             BOOLEAN,
    pricing_note        VARCHAR(255),
    commercial_use      BOOLEAN,
    requires_approval   BOOLEAN       NOT NULL DEFAULT FALSE,
    response_format     VARCHAR(20),
    avg_response_time   FLOAT,
    response_schema     JSONB,
    collected_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    is_deleted          BOOLEAN       NOT NULL DEFAULT FALSE,

    CONSTRAINT fk_open_api_source
        FOREIGN KEY (openapi_source_id) REFERENCES openapi_source(id),

    CONSTRAINT uq_open_api_source_key
        UNIQUE (openapi_source_id, source_openapi_key),

    CONSTRAINT chk_open_api_auth_type
        CHECK (auth_type IN ('API_KEY', 'OAUTH', 'CLIENT_SECRET', 'CONTRACT', 'NONE')),

    CONSTRAINT chk_open_api_response_format
        CHECK (response_format IS NULL OR response_format IN ('JSON', 'XML', 'JSON+XML')),

    CONSTRAINT chk_open_api_rate_limit
        CHECK (rate_limit IS NULL OR rate_limit >= 0),

    CONSTRAINT chk_open_api_daily_limit
        CHECK (daily_limit IS NULL OR daily_limit >= 0),

    CONSTRAINT chk_open_api_avg_response_time
        CHECK (avg_response_time IS NULL OR avg_response_time >= 0)
);

-- =========================================================
-- 8. collection_openapi
-- =========================================================
CREATE TABLE collection_openapi (
    id                  BIGSERIAL PRIMARY KEY,
    openapi_source_id   SMALLINT    NOT NULL,
    parser_version      VARCHAR(50) NOT NULL,
    run_started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    run_finished_at     TIMESTAMPTZ,
    status              VARCHAR(20) NOT NULL,
    collected_count     INTEGER     NOT NULL DEFAULT 0,
    upserted_count      INTEGER     NOT NULL DEFAULT 0,
    failed_count        INTEGER     NOT NULL DEFAULT 0,
    error_summary       TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_collection_openapi_source
        FOREIGN KEY (openapi_source_id) REFERENCES openapi_source(id),

    CONSTRAINT chk_collection_openapi_status
        CHECK (status IN ('RUNNING', 'SUCCESS', 'PARTIAL_SUCCESS', 'FAILED')),

    CONSTRAINT chk_collection_openapi_collected_count
        CHECK (collected_count >= 0),

    CONSTRAINT chk_collection_openapi_upserted_count
        CHECK (upserted_count >= 0),

    CONSTRAINT chk_collection_openapi_failed_count
        CHECK (failed_count >= 0)
);

-- =========================================================
-- 5. dataset_chunk
-- =========================================================
CREATE TABLE dataset_chunk (
    id              BIGSERIAL PRIMARY KEY,
    dataset_id      BIGINT       NOT NULL,
    chunk_type      VARCHAR(30)  NOT NULL,
    chunk_order     INTEGER      NOT NULL DEFAULT 0,
    chunk_text      TEXT         NOT NULL,
    token_count     INTEGER,
    lang_code       VARCHAR(10),
    embed_model     VARCHAR(100),
    embedding       VECTOR(1536),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_dataset_chunk_dataset
        FOREIGN KEY (dataset_id) REFERENCES dataset(id) ON DELETE CASCADE,

    CONSTRAINT uq_dataset_chunk_order
        UNIQUE (dataset_id, chunk_type, chunk_order),

    CONSTRAINT chk_dataset_chunk_type
        CHECK (chunk_type IN ('TITLE_SUMMARY', 'DESCRIPTION', 'TAGS', 'SCHEMA', 'RESOURCE', 'ACCESS')),

    CONSTRAINT chk_dataset_chunk_order
        CHECK (chunk_order >= 0),

    CONSTRAINT chk_dataset_chunk_token_count
        CHECK (token_count IS NULL OR token_count >= 0)
);

-- =========================================================
-- 9. openapi_chunk
-- =========================================================
CREATE TABLE openapi_chunk (
    id              BIGSERIAL PRIMARY KEY,
    openapi_id      BIGINT       NOT NULL,
    chunk_type      VARCHAR(30)  NOT NULL,
    chunk_order     INTEGER      NOT NULL DEFAULT 0,
    chunk_text      TEXT         NOT NULL,
    token_count     INTEGER,
    lang_code       VARCHAR(10),
    embed_model     VARCHAR(100),
    embedding       VECTOR(1536),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_openapi_chunk_openapi
        FOREIGN KEY (openapi_id) REFERENCES open_api(id) ON DELETE CASCADE,

    CONSTRAINT uq_openapi_chunk_order
        UNIQUE (openapi_id, chunk_type, chunk_order),

    CONSTRAINT chk_openapi_chunk_type
        CHECK (chunk_type IN ('TITLE_SUMMARY', 'DESCRIPTION', 'TAGS', 'SCHEMA', 'RESOURCE', 'ACCESS')),

    CONSTRAINT chk_openapi_chunk_order
        CHECK (chunk_order >= 0),

    CONSTRAINT chk_openapi_chunk_token_count
        CHECK (token_count IS NULL OR token_count >= 0)
);

-- =========================================================
-- 10. conversation
-- =========================================================
CREATE TABLE conversation (
    id              BIGSERIAL PRIMARY KEY,
    user_id         BIGINT       NOT NULL,
    title           VARCHAR(255),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    deleted_at      TIMESTAMPTZ,

    CONSTRAINT fk_conversation_user
        FOREIGN KEY (user_id) REFERENCES "user"(id) ON DELETE CASCADE
);

-- =========================================================
-- 11. conversation_turn
-- =========================================================
CREATE TABLE conversation_turn (
    id                  BIGSERIAL PRIMARY KEY,
    conversation_id     BIGINT       NOT NULL,
    turn_order          INTEGER      NOT NULL,
    content             TEXT         NOT NULL,
    role                VARCHAR(20)  NOT NULL,
    response_time_ms    INTEGER,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_conversation_turn_conversation
        FOREIGN KEY (conversation_id) REFERENCES conversation(id) ON DELETE CASCADE,

    CONSTRAINT uq_conversation_turn_order
        UNIQUE (conversation_id, turn_order),

    CONSTRAINT chk_conversation_turn_role
        CHECK (role IN ('USER', 'ASSISTANT', 'SYSTEM'))
);

-- =========================================================
-- 12. recommendation
-- 다형 참조이므로 resource_id 에 일반 FK는 걸지 않음
-- =========================================================
CREATE TABLE recommendation (
    id                  BIGSERIAL PRIMARY KEY,
    turn_id             BIGINT       NOT NULL,
    resource_type       VARCHAR(20)  NOT NULL,
    resource_id         BIGINT       NOT NULL,
    suitability_score   FLOAT,
    rank                INTEGER      NOT NULL,
    reason_text         TEXT,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_recommendation_turn
        FOREIGN KEY (turn_id) REFERENCES conversation_turn(id) ON DELETE CASCADE,

    CONSTRAINT uq_recommendation_rank
        UNIQUE (turn_id, rank),

    CONSTRAINT chk_recommendation_resource_type
        CHECK (resource_type IN ('DATASET', 'OPEN_API')),

    CONSTRAINT chk_recommendation_rank
        CHECK (rank >= 1)
);

-- =========================================================
-- 13. review
-- 다형 참조이므로 resource_id 에 일반 FK는 걸지 않음
-- =========================================================
CREATE TABLE review (
    id              BIGSERIAL PRIMARY KEY,
    user_id         BIGINT       NOT NULL,
    resource_type   VARCHAR(20)  NOT NULL,
    resource_id     BIGINT       NOT NULL,
    rating          SMALLINT     NOT NULL,
    content         TEXT,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    deleted_at      TIMESTAMPTZ,

    CONSTRAINT fk_review_user
        FOREIGN KEY (user_id) REFERENCES "user"(id) ON DELETE CASCADE,

    CONSTRAINT chk_review_resource_type
        CHECK (resource_type IN ('DATASET', 'OPEN_API')),

    CONSTRAINT chk_review_rating
        CHECK (rating BETWEEN 1 AND 5)
);

-- =========================================================
-- 14. bookmark
-- 다형 참조이므로 resource_id 에 일반 FK는 걸지 않음
-- =========================================================
CREATE TABLE bookmark (
    id              BIGSERIAL PRIMARY KEY,
    user_id         BIGINT       NOT NULL,
    resource_type   VARCHAR(20)  NOT NULL,
    resource_id     BIGINT       NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    deleted_at      TIMESTAMPTZ,

    CONSTRAINT fk_bookmark_user
        FOREIGN KEY (user_id) REFERENCES "user"(id) ON DELETE CASCADE,

    CONSTRAINT chk_bookmark_resource_type
        CHECK (resource_type IN ('DATASET', 'OPEN_API'))
);

-- =========================================================
-- 15. post
-- openapi_id, dataset_id 는 BIGINT[] 로 보정
-- is_deleted 는 BOOLEAN 으로 보정
-- =========================================================
CREATE TABLE post (
    id              BIGSERIAL PRIMARY KEY,
    user_id         BIGINT       NOT NULL,
    title           VARCHAR(255) NOT NULL,
    content         TEXT,
    openapi_id      BIGINT[],
    dataset_id      BIGINT[],
    view_count      INTEGER      NOT NULL DEFAULT 0,
    favorite        INTEGER      NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    is_deleted      BOOLEAN      NOT NULL DEFAULT FALSE,

    CONSTRAINT fk_post_user
        FOREIGN KEY (user_id) REFERENCES "user"(id) ON DELETE CASCADE,

    CONSTRAINT chk_post_view_count
        CHECK (view_count >= 0),

    CONSTRAINT chk_post_favorite
        CHECK (favorite >= 0)
);

-- =========================================================
-- INDEX
-- =========================================================

CREATE INDEX idx_dataset_dataset_source_id
    ON dataset(dataset_source_id);

CREATE INDEX idx_dataset_last_ingest_run_id
    ON dataset(last_ingest_run_id);

CREATE INDEX idx_collection_dataset_dataset_source_id
    ON collection_dataset(dataset_source_id);

CREATE INDEX idx_dataset_chunk_dataset_id
    ON dataset_chunk(dataset_id);

CREATE INDEX idx_open_api_openapi_source_id
    ON open_api(openapi_source_id);

CREATE INDEX idx_collection_openapi_openapi_source_id
    ON collection_openapi(openapi_source_id);

CREATE INDEX idx_openapi_chunk_openapi_id
    ON openapi_chunk(openapi_id);

CREATE INDEX idx_conversation_user_id
    ON conversation(user_id);

CREATE INDEX idx_conversation_turn_conversation_id
    ON conversation_turn(conversation_id);

CREATE INDEX idx_recommendation_turn_id
    ON recommendation(turn_id);

CREATE INDEX idx_recommendation_resource
    ON recommendation(resource_type, resource_id);

CREATE INDEX idx_review_user_id
    ON review(user_id);

CREATE INDEX idx_review_resource
    ON review(resource_type, resource_id);

CREATE INDEX idx_bookmark_user_id
    ON bookmark(user_id);

CREATE INDEX idx_bookmark_resource
    ON bookmark(resource_type, resource_id);

CREATE INDEX idx_post_user_id
    ON post(user_id);

CREATE UNIQUE INDEX uq_review_active
    ON review(user_id, resource_type, resource_id)
    WHERE deleted_at IS NULL;

CREATE UNIQUE INDEX uq_bookmark_active
    ON bookmark(user_id, resource_type, resource_id)
    WHERE deleted_at IS NULL;

CREATE INDEX idx_dataset_domains_gin
    ON dataset USING GIN (domains);

CREATE INDEX idx_dataset_tasks_gin
    ON dataset USING GIN (tasks);

CREATE INDEX idx_dataset_modalities_gin
    ON dataset USING GIN (modalities);

CREATE INDEX idx_dataset_tags_gin
    ON dataset USING GIN (tags);

CREATE INDEX idx_dataset_languages_gin
    ON dataset USING GIN (languages);

CREATE INDEX idx_open_api_tags_gin
    ON open_api USING GIN (tags);

-- =========================================================
-- TRIGGER
-- =========================================================
CREATE TRIGGER trg_user_set_updated_at
BEFORE UPDATE ON "user"
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_dataset_set_updated_at
BEFORE UPDATE ON dataset
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_dataset_chunk_set_updated_at
BEFORE UPDATE ON dataset_chunk
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_open_api_set_updated_at
BEFORE UPDATE ON open_api
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_openapi_chunk_set_updated_at
BEFORE UPDATE ON openapi_chunk
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_conversation_set_updated_at
BEFORE UPDATE ON conversation
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_review_set_updated_at
BEFORE UPDATE ON review
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_post_set_updated_at
BEFORE UPDATE ON post
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();