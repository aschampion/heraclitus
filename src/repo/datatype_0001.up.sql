CREATE TABLE datatype (
  id bigint PRIMARY KEY,
  version bigint NOT NULL,
  name text UNIQUE NOT NULL
) WITH (
  OIDS=FALSE
);