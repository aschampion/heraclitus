CREATE TABLE artifact_graph (
  id bigserial PRIMARY KEY,
  LIKE identity_template INCLUDING CONSTRAINTS INCLUDING INDEXES
  -- name text UNIQUE NOT NULL
) WITH (
  OIDS=FALSE
);

CREATE TABLE artifact (
  id bigserial PRIMARY KEY,
  LIKE identity_template INCLUDING CONSTRAINTS INCLUDING INDEXES,
  artifact_graph_id bigint NOT NULL REFERENCES artifact_graph (id) DEFERRABLE INITIALLY IMMEDIATE,
  datatype_id bigint NOT NULL REFERENCES datatype (id),
  name text
) WITH (
  OIDS=FALSE
);

CREATE TYPE artifact_edge_type AS ENUM (
  'dtype',
  'producer'
);

CREATE TABLE artifact_edge (
  source_id bigint NOT NULL REFERENCES artifact (id) DEFERRABLE INITIALLY IMMEDIATE,
  dependent_id bigint NOT NULL REFERENCES artifact (id) DEFERRABLE INITIALLY IMMEDIATE,
  edge_type artifact_edge_type NOT NULL,
  name text NOT NULL,
  PRIMARY KEY (source_id, dependent_id)
) WITH (
  OIDS=FALSE
);

-- Cannot use inheritance for edge types because FKs will reference them.
-- CREATE TABLE artifact_producer_edge (
--   name text NOT NULL
-- ) INHERITS (artifact_edge)
-- WITH (
--   OIDS=FALSE
-- );

-- CREATE TABLE artifact_dtype_edge (
--   name text NOT NULL
-- ) INHERITS (artifact_edge)
-- WITH (
--   OIDS=FALSE
-- );

CREATE TYPE representation_kind AS ENUM (
  'state',
  'delta',
  'cumulative_delta'
);

CREATE TYPE version_status AS ENUM (
  'staging',
  'committed'
);

CREATE TABLE version (
  id bigserial PRIMARY KEY,
  LIKE identity_template INCLUDING CONSTRAINTS INCLUDING INDEXES,
  artifact_id bigint NOT NULL REFERENCES artifact (id) DEFERRABLE INITIALLY IMMEDIATE,
  status version_status NOT NULL,
  representation representation_kind NOT NULL
) WITH (
  OIDS=FALSE
);

CREATE TABLE producer_version (
  version_id bigint PRIMARY KEY REFERENCES version (id) DEFERRABLE INITIALLY IMMEDIATE,
  strategy text NOT NULL
) WITH (
  OIDS=FALSE
);

CREATE TABLE version_parent (
  parent_id bigint NOT NULL REFERENCES version (id) DEFERRABLE INITIALLY IMMEDIATE,
  child_id bigint NOT NULL REFERENCES version (id) DEFERRABLE INITIALLY IMMEDIATE,
  PRIMARY KEY (parent_id, child_id)
  -- TODO existence of this relation implies versions reference same artifact, may be a check constraint
) WITH (
  OIDS=FALSE
);

CREATE TABLE version_relation (
  source_version_id bigint NOT NULL REFERENCES version (id) DEFERRABLE INITIALLY IMMEDIATE,
  dependent_version_id bigint NOT NULL REFERENCES version (id) DEFERRABLE INITIALLY IMMEDIATE,
  source_id bigint NOT NULL,
  dependent_id bigint NOT NULL,
  FOREIGN KEY (source_id, dependent_id) REFERENCES artifact_edge (source_id, dependent_id) DEFERRABLE INITIALLY IMMEDIATE
) WITH (
  OIDS=FALSE
);

CREATE TYPE part_completion AS ENUM (
  'complete',
  'ragged'
);

CREATE TABLE hunk (
  id bigserial PRIMARY KEY,
  LIKE identity_template INCLUDING CONSTRAINTS INCLUDING INDEXES,
  version_id bigint NOT NULL REFERENCES version (id) DEFERRABLE INITIALLY IMMEDIATE,
  partition_id bigint NOT NULL,
  representation representation_kind NOT NULL,
  completion part_completion NOT NULL
) WITH (
  OIDS=FALSE
);
