CREATE TABLE artifact (
  id bigserial PRIMARY KEY,
  LIKE identity_template INCLUDING CONSTRAINTS INCLUDING INDEXES,
  hunk_id bigint NOT NULL, -- To be altered below: REFERENCES hunk (id) DEFERRABLE INITIALLY IMMEDIATE,
  datatype_id bigint NOT NULL REFERENCES datatype (id),
  self_partitioning boolean NOT NULL,
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

CREATE TYPE production_policy AS ENUM (
  'extant',
  'leaf_bootstrap',
  'custom'
);

CREATE TABLE producer_artifact (
  artifact_id bigint PRIMARY KEY REFERENCES artifact (id) DEFERRABLE INITIALLY IMMEDIATE,
  policies production_policy[] NOT NULL
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

ALTER TABLE artifact
ADD CONSTRAINT _artifact_hunk_id_fk
FOREIGN KEY (hunk_id) REFERENCES hunk (id) DEFERRABLE INITIALLY IMMEDIATE;

CREATE TABLE artifact_removals (
  hunk_id bigint NOT NULL REFERENCES hunk (id) DEFERRABLE INITIALLY IMMEDIATE,
  removed_artifact_id bigint NOT NULL REFERENCES artifact (id) DEFERRABLE INITIALLY IMMEDIATE
) WITH (
  OIDS=FALSE
);

CREATE TABLE hunk_precedence (
  merge_version_id bigint NOT NULL REFERENCES version (id) DEFERRABLE INITIALLY IMMEDIATE,
  partition_id bigint NOT NULL,
  precedent_version_id bigint NOT NULL REFERENCES version (id) DEFERRABLE INITIALLY IMMEDIATE,
  PRIMARY KEY (merge_version_id, partition_id)
) WITH (
  OIDS=FALSE
);

CREATE TABLE origin (
  artifact_uuid uuid NOT NULL REFERENCES artifact (uuid_) DEFERRABLE INITIALLY IMMEDIATE,
  version_uuid uuid NOT NULL REFERENCES version (uuid_) DEFERRABLE INITIALLY IMMEDIATE,
  hunk_uuid uuid NOT NULL REFERENCES hunk (uuid_) DEFERRABLE INITIALLY IMMEDIATE
) WITH (
  OIDS=FALSE
);

CREATE UNIQUE INDEX origin_singleton_row
ON origin((TRUE));
