CREATE TABLE blob_dtype_state (
  hunk_id bigint PRIMARY KEY REFERENCES hunk (id) DEFERRABLE INITIALLY IMMEDIATE,
  blob bytea NOT NULL
) WITH (
  OIDS=FALSE
);

CREATE TABLE blob_dtype_delta (
  hunk_id bigint PRIMARY KEY REFERENCES hunk (id) DEFERRABLE INITIALLY IMMEDIATE,
  indices bigint[] NOT NULL,
  bytes bytea NOT NULL
) WITH (
  OIDS=FALSE
);
