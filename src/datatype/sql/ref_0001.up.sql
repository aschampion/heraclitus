CREATE TABLE ref (
  version_id bigint PRIMARY KEY REFERENCES version (id) DEFERRABLE INITIALLY IMMEDIATE,
  message text NOT NULL
) WITH (
  OIDS=FALSE
);

CREATE TABLE branch (
  id bigserial PRIMARY KEY,
  ref_artifact_id bigint NOT NULL REFERENCES artifact (id) DEFERRABLE INITIALLY IMMEDIATE,
  name text NOT NULL,
  UNIQUE(ref_artifact_id, name)
) WITH (
  OIDS=FALSE
);

CREATE TABLE revision_path (
  branch_id bigint NOT NULL REFERENCES branch (id) DEFERRABLE INITIALLY IMMEDIATE,
  name text NOT NULL,
  ref_version_id bigint NOT NULL REFERENCES version (id) DEFERRABLE INITIALLY IMMEDIATE,
  PRIMARY KEY (branch_id, name)
) WITH (
  OIDS=FALSE
);
