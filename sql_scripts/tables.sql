CREATE TABLE bitcoin_metadata
(
    id                 VARCHAR PRIMARY KEY,
    name               VARCHAR,
    symbol             VARCHAR,
    rank               INT,
    is_new             BOOLEAN,
    is_active          BOOLEAN,
    type               VARCHAR,
    description        TEXT,
    started_at         TIMESTAMP,
    development_status VARCHAR,
    hardware_wallet    BOOLEAN,
    proof_type         VARCHAR,
    org_structure      VARCHAR,
    hash_algorithm     VARCHAR,
    total_supply       BIGINT,
    max_supply         BIGINT,
    last_updated       TIMESTAMP,
    price_usd          DECIMAL(20, 8),
    is_monitored       BOOLEAN          /*Tracks if we are monitoring this coin for updates.*/
);