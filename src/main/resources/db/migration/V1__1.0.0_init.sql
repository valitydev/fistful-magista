CREATE SCHEMA IF NOT EXISTS mst;

CREATE TYPE mst.withdrawal_status AS ENUM ('pending', 'succeeded', 'failed');
CREATE TYPE mst.withdrawal_event_type AS ENUM ('WITHDRAWAL_CREATED', 'WITHDRAWAL_STATUS_CHANGED');

CREATE TABLE mst.withdrawal_data (
    id                BIGSERIAL                   NOT NULL,
    party_id          UUID,
    withdrawal_id     CHARACTER VARYING           NOT NULL,
    wallet_id         CHARACTER VARYING           NOT NULL,
    destination_id    CHARACTER VARYING           NOT NULL,
    created_at        TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    amount            BIGINT                      NOT NULL,
    currency_code     CHARACTER VARYING           NOT NULL,
    event_id          BIGINT                      NOT NULL,
    event_type        mst.withdrawal_event_type   NOT NULL,
    event_created_at  TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    event_occurred_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    withdrawal_status mst.withdrawal_status       NOT NULL,
    fee               BIGINT,
    external_id       CHARACTER VARYING,
    error_code        CHARACTER VARYING,
    error_reason      CHARACTER VARYING,
    error_sub_failure CHARACTER VARYING,
    provider_id       INT,
    terminal_id       INT,
    CONSTRAINT withdrawal_data_pkey PRIMARY KEY (id),
    CONSTRAINT withdrawal_data_ukey UNIQUE (withdrawal_id)
);

CREATE INDEX withdrawal_event_created_at_idx ON mst.withdrawal_data (event_created_at);
CREATE INDEX withdrawal_id_idx ON mst.withdrawal_data (withdrawal_id);
CREATE INDEX withdrawal_event_occurred_at_idx ON mst.withdrawal_data (event_occurred_at);
CREATE INDEX withdrawal_party_id_idx ON mst.withdrawal_data (party_id);
CREATE INDEX withdrawal_data_failure_index ON mst.withdrawal_data (error_code, error_reason, error_sub_failure);
CREATE INDEX withdrawal_provider_id_idx ON mst.withdrawal_data (provider_id);
CREATE INDEX withdrawal_terminal_id_idx ON mst.withdrawal_data (terminal_id);

CREATE TYPE mst.deposit_event_type AS ENUM ('DEPOSIT_CREATED', 'DEPOSIT_STATUS_CHANGED', 'DEPOSIT_TRANSFER_CREATED', 'DEPOSIT_TRANSFER_STATUS_CHANGED');
CREATE TYPE mst.deposit_status AS ENUM ('pending', 'succeeded', 'failed');
CREATE TYPE mst.deposit_transfer_status AS ENUM ('created', 'prepared', 'committed', 'cancelled');

CREATE TABLE mst.deposit_data (
    id                      BIGSERIAL                   NOT NULL,
    event_id                BIGINT                      NOT NULL,
    event_created_at        TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    deposit_id              CHARACTER VARYING           NOT NULL,
    event_occurred_at       TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    event_type              mst.deposit_event_type      NOT NULL,
    wallet_id               CHARACTER VARYING           NOT NULL,
    source_id               CHARACTER VARYING           NOT NULL,
    amount                  BIGINT                      NOT NULL,
    currency_code           CHARACTER VARYING           NOT NULL,
    deposit_status          mst.deposit_status          NOT NULL,
    deposit_transfer_status mst.deposit_transfer_status,
    fee                     BIGINT,
    provider_fee            BIGINT,
    party_id                UUID,
    created_at              TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    description             CHARACTER VARYING,
    wtime                   TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
    CONSTRAINT deposit_pkey PRIMARY KEY (id),
    CONSTRAINT deposit_ukey UNIQUE (deposit_id)
);

CREATE INDEX deposit_event_id_idx ON mst.deposit_data (event_id);
CREATE INDEX deposit_event_created_at_idx ON mst.deposit_data (event_created_at);
CREATE INDEX deposit_id_idx ON mst.deposit_data (deposit_id);
CREATE INDEX deposit_event_occured_at_idx ON mst.deposit_data (event_occurred_at);
CREATE INDEX deposit_wallet_id_idx ON mst.deposit_data (wallet_id);
CREATE INDEX deposit_party_id_idx ON mst.deposit_data (party_id);

CREATE TYPE mst.source_event_type AS ENUM ('SOURCE_CREATED');

CREATE TABLE mst.source_data (
    id                        BIGSERIAL                   NOT NULL,
    event_id                  BIGINT                      NOT NULL,
    event_created_at          TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    event_occured_at          TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    event_type                mst.source_event_type       NOT NULL,
    source_id                 CHARACTER VARYING           NOT NULL,
    name                      CHARACTER VARYING           NOT NULL,
    resource_internal_details CHARACTER VARYING           NOT NULL,
    external_id               CHARACTER VARYING,
    created_at                TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    context_json              CHARACTER VARYING,
    party_id                  UUID                        NOT NULL,
    account_currency          CHARACTER VARYING,
    account_id                BIGINT,
    wtime                     TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
    CONSTRAINT source_pkey PRIMARY KEY (id)
);

CREATE INDEX source_event_id_idx ON mst.source_data (event_id);
CREATE INDEX source_event_created_at_idx ON mst.source_data (event_created_at);
CREATE UNIQUE INDEX source_id_idx ON mst.source_data (source_id);
CREATE INDEX source_event_occured_at_idx ON mst.source_data (event_occured_at);
