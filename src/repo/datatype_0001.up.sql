CREATE TABLE datatype (
  id bigserial PRIMARY KEY,
  version bigint NOT NULL,
  name text UNIQUE NOT NULL
) WITH (
  OIDS=FALSE
);

CREATE TABLE identity_template (
  uuid_ uuid UNIQUE NOT NULL,
  hash bigint NOT NULL
) WITH (
  OIDS=FALSE
);
