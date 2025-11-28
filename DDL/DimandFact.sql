-- Dimension: Indicator
CREATE TABLE bi_db.dim_indicator (
    indicator_key   BIGINT GENERATED ALWAYS AS IDENTITY,
    indicator_code  STRING,
    indicator_name  STRING,
    section         STRING,
    domain          STRING,
    unit_default    STRING,
    description     STRING,
    source_system   STRING,
    is_active       BOOLEAN,
    valid_from      DATE,
    valid_to        DATE
);

-- Dimension: Year
CREATE TABLE bi_db.dim_year (
    year_key    BIGINT GENERATED ALWAYS AS IDENTITY,
    year        INT,
    decade      STRING,
    is_recent_5yr  BOOLEAN,
    is_recent_10yr BOOLEAN
);

-- Dimension: Unit
CREATE TABLE bi_db.dim_unit (
    unit_key            BIGINT GENERATED ALWAYS AS IDENTITY,
    unit_code           STRING,
    unit_name           STRING,
    base_unit_code      STRING,
    conversion_to_base  DOUBLE
);

-- Fact: Indicator Values
CREATE TABLE bi_db.fact_indicator_values (
    indicator_key   BIGINT,
    year_key        BIGINT,
    geo_key         BIGINT,
    unit_key        BIGINT,
    value_official  DOUBLE,
    value_base_unit DOUBLE,
    data_quality_flag STRING,
    load_ts         TIMESTAMP
);
