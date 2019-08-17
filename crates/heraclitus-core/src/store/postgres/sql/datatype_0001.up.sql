CREATE TABLE identity_template (
  uuid_ uuid UNIQUE NOT NULL,
  hash bigint NOT NULL
) WITH (
  OIDS=FALSE
);

CREATE TABLE datatype (
  id bigserial PRIMARY KEY,
  LIKE identity_template INCLUDING CONSTRAINTS INCLUDING INDEXES,
  version bigint NOT NULL,
  name text UNIQUE NOT NULL
) WITH (
  OIDS=FALSE
);
