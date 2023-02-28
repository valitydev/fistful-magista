CREATE TYPE mst.source_event_type AS ENUM (
  'SOURCE_CREATED', 'SOURCE_ACCOUNT_CHANGED', 'SOURCE_STATUS_CHANGED'
);

CREATE TYPE mst.source_status AS ENUM ('authorized', 'unauthorized');

CREATE TABLE mst.source_data (
  id                      BIGSERIAL                   NOT NULL,
  event_id                BIGINT                      NOT NULL,
  event_created_at        TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  event_occured_at        TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  event_type              mst.source_event_type       NOT NULL,
  source_id               CHARACTER VARYING           NOT NULL,
  name               CHARACTER VARYING           NOT NULL,
  resource_internal_details               CHARACTER VARYING           NOT NULL,
  external_id      CHARACTER VARYING           NOT NULL,
  status           mst.source_status           ,
  created_at       TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  context_json     CHARACTER VARYING           NOT NULL,
  account_id              CHARACTER VARYING,
  account_identity_id     CHARACTER VARYING,
  account_currency        CHARACTER VARYING,
  account_accounter_id    BIGINT,
  wtime                   TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (now() at time zone 'utc'),
  CONSTRAINT source_pkey PRIMARY KEY (id)
);

CREATE INDEX source_event_id_idx
  on mst.source_data (event_id);
CREATE INDEX source_event_created_at_idx
  on mst.source_data (event_created_at);
CREATE UNIQUE INDEX source_id_idx
  on mst.source_data (source_id);
CREATE INDEX source_event_occured_at_idx
  on mst.source_data (event_occured_at);
CREATE INDEX source_identity_id_idx
  on mst.source_data (account_identity_id);
