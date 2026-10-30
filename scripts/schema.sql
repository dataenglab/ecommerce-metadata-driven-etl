-- ============================================================
-- schema.sql : analytical warehouse for the e-commerce batch ETL
-- Target: PostgreSQL 16
-- ============================================================
CREATE SCHEMA IF NOT EXISTS etl;     -- control tables
CREATE SCHEMA IF NOT EXISTS dwh;     -- star schema

-- ---------- 1. Control tables --------------------------------

CREATE TABLE etl.pipeline_logs (
    log_id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id            UUID         NOT NULL,
    source_name       TEXT         NOT NULL,
    source_file       TEXT         NOT NULL,
    started_at        TIMESTAMPTZ  NOT NULL,
    finished_at       TIMESTAMPTZ,
    execution_time    NUMERIC(12,3),                -- seconds
    status            TEXT         NOT NULL
                      CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    rows_read         INTEGER      NOT NULL DEFAULT 0 CHECK (rows_read >= 0),
    rows_inserted     INTEGER      NOT NULL DEFAULT 0 CHECK (rows_inserted >= 0),
    rows_quarantined  INTEGER      NOT NULL DEFAULT 0 CHECK (rows_quarantined >= 0),
    message           TEXT,                          -- failure reason, no PII
    CONSTRAINT uq_run_file UNIQUE (run_id, source_file),
    -- reconciliation invariant, enforced once a source has completed
    CONSTRAINT ck_reconciliation CHECK (
        status <> 'SUCCESS'
        OR rows_read = rows_inserted + rows_quarantined)
);

CREATE TABLE etl.quarantine_staging (
    quarantine_id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id               UUID         NOT NULL,
    log_id               BIGINT       NOT NULL REFERENCES etl.pipeline_logs (log_id),
    source_name          TEXT         NOT NULL,
    source_file          TEXT         NOT NULL,
    source_row_number    INTEGER      NOT NULL,
    error_code           TEXT         NOT NULL
                         CHECK (error_code IN (
                             'NULL_VIOLATION', 'TYPE_ERROR', 'DOMAIN_VIOLATION',
                             'DUPLICATE_KEY', 'ARITHMETIC_MISMATCH',
                             'NEGATIVE_AMOUNT', 'INVALID_CURRENCY',
                             'JSON_PARSE_ERROR')),
    error_message        TEXT         NOT NULL,
    raw_record           JSONB        NOT NULL,      -- pseudonymised before insert
    ingestion_timestamp  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX ix_quarantine_log   ON etl.quarantine_staging (log_id);
CREATE INDEX ix_quarantine_error ON etl.quarantine_staging (error_code);

-- ---------- 2. Dimensions ------------------------------------

CREATE TABLE dwh.dim_date (
    date_key      INTEGER  PRIMARY KEY,              -- YYYYMMDD
    full_date     DATE     NOT NULL UNIQUE,
    year          SMALLINT NOT NULL,
    quarter       SMALLINT NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    month         SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),
    iso_week      SMALLINT NOT NULL,
    day_of_week   SMALLINT NOT NULL CHECK (day_of_week BETWEEN 1 AND 7),
    is_weekend    BOOLEAN  NOT NULL
);

INSERT INTO dwh.dim_date
SELECT to_char(d, 'YYYYMMDD')::INTEGER,
       d::DATE,
       EXTRACT(YEAR    FROM d)::SMALLINT,
       EXTRACT(QUARTER FROM d)::SMALLINT,
       EXTRACT(MONTH   FROM d)::SMALLINT,
       EXTRACT(WEEK    FROM d)::SMALLINT,
       EXTRACT(ISODOW  FROM d)::SMALLINT,
       EXTRACT(ISODOW  FROM d) IN (6, 7)
FROM generate_series(DATE '2020-01-01', DATE '2030-12-31', INTERVAL '1 day') AS d;

-- SCD Type 2: one row per version of a customer
CREATE TABLE dwh.dim_customer (
    customer_sk           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_hash         CHAR(64)     NOT NULL,     -- HMAC-SHA-256 of customer_id
    email_hash            CHAR(64),                  -- HMAC-SHA-256 of e-mail (Type 1)
    ship_country          CHAR(2),                   -- SCD2-tracked
    ship_postcode_prefix  VARCHAR(4),                -- SCD2-tracked, generalised
    valid_from            TIMESTAMPTZ  NOT NULL,
    valid_to              TIMESTAMPTZ  NOT NULL DEFAULT 'infinity',
    is_current            BOOLEAN      NOT NULL DEFAULT TRUE,
    CONSTRAINT ck_customer_hash  CHECK (customer_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_customer_valid CHECK (valid_from < valid_to)
);
CREATE UNIQUE INDEX uq_customer_current
    ON dwh.dim_customer (customer_hash) WHERE is_current;
CREATE INDEX ix_customer_lookup
    ON dwh.dim_customer (customer_hash, valid_from, valid_to);
CREATE INDEX ix_customer_email ON dwh.dim_customer (email_hash);

CREATE TABLE dwh.dim_product (
    product_sk    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_sku   TEXT         NOT NULL,
    product_name  TEXT         NOT NULL,             -- SCD2-tracked
    category      TEXT         NOT NULL,             -- SCD2-tracked
    valid_from    TIMESTAMPTZ  NOT NULL,
    valid_to      TIMESTAMPTZ  NOT NULL DEFAULT 'infinity',
    is_current    BOOLEAN      NOT NULL DEFAULT TRUE,
    CONSTRAINT ck_product_valid CHECK (valid_from < valid_to)
);
CREATE UNIQUE INDEX uq_product_current
    ON dwh.dim_product (product_sku) WHERE is_current;
CREATE INDEX ix_product_lookup
    ON dwh.dim_product (product_sku, valid_from, valid_to);

-- ---------- 3. Facts -----------------------------------------

-- Grain: one row per order line
CREATE TABLE dwh.fact_order_line (
    order_id          TEXT          NOT NULL,        -- degenerate dimension
    line_number       SMALLINT      NOT NULL CHECK (line_number >= 1),
    date_key          INTEGER       NOT NULL REFERENCES dwh.dim_date,
    customer_sk       BIGINT        NOT NULL REFERENCES dwh.dim_customer,
    product_sk        BIGINT        NOT NULL REFERENCES dwh.dim_product,
    order_ts_utc      TIMESTAMPTZ   NOT NULL,
    quantity          INTEGER       NOT NULL CHECK (quantity > 0),
    unit_price        NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
    discount_amount   NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (discount_amount >= 0),
    shipping_amount   NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (shipping_amount >= 0),
    total_amount      NUMERIC(12,2) NOT NULL,
    currency_code     CHAR(3)       NOT NULL CHECK (currency_code ~ '^[A-Z]{3}$'),
    run_id            UUID          NOT NULL,
    PRIMARY KEY (order_id, line_number)
);
CREATE INDEX ix_fol_date     ON dwh.fact_order_line (date_key);
CREATE INDEX ix_fol_customer ON dwh.fact_order_line (customer_sk);
CREATE INDEX ix_fol_product  ON dwh.fact_order_line (product_sk);

-- Grain: one row per payment transaction (charge or refund)
CREATE TABLE dwh.fact_payment (
    payment_id        TEXT          PRIMARY KEY,
    order_id          TEXT,                          -- from metadata JSON
    date_key          INTEGER       NOT NULL REFERENCES dwh.dim_date,
    created_at_utc    TIMESTAMPTZ   NOT NULL,
    gateway           TEXT          NOT NULL CHECK (gateway IN ('stripe', 'paypal')),
    transaction_type  TEXT          NOT NULL CHECK (transaction_type IN ('charge', 'refund')),
    payment_status    TEXT          NOT NULL CHECK (payment_status IN ('succeeded', 'pending', 'failed')),
    payment_method    TEXT,
    sales_channel     TEXT,                          -- from metadata JSON
    amount_minor      BIGINT        NOT NULL,        -- minor currency units
    currency_code     CHAR(3)       NOT NULL CHECK (currency_code ~ '^[A-Z]{3}$'),
    run_id            UUID          NOT NULL,
    CONSTRAINT ck_payment_sign CHECK (
        transaction_type = 'refund' OR amount_minor >= 0)
);
CREATE INDEX ix_pay_date  ON dwh.fact_payment (date_key);
CREATE INDEX ix_pay_order ON dwh.fact_payment (order_id);

-- Grain: one row per clickstream event
CREATE TABLE dwh.fact_clickstream_event (
    event_id       TEXT         PRIMARY KEY,
    session_id     TEXT         NOT NULL,
    date_key       INTEGER      NOT NULL REFERENCES dwh.dim_date,
    customer_sk    BIGINT       REFERENCES dwh.dim_customer,   -- NULL = anonymous
    product_sk     BIGINT       REFERENCES dwh.dim_product,    -- NULL = no product
    event_ts_utc   TIMESTAMPTZ  NOT NULL,
    event_type     TEXT         NOT NULL
                   CHECK (event_type IN ('page_view', 'add_to_cart', 'remove_from_cart')),
    page_url       TEXT,
    quantity       INTEGER      CHECK (quantity IS NULL OR quantity > 0),
    run_id         UUID         NOT NULL
);
CREATE INDEX ix_click_session ON dwh.fact_clickstream_event (session_id);
CREATE INDEX ix_click_date    ON dwh.fact_clickstream_event (date_key);
