CREATE TABLE artifact_graph (
  id bigserial PRIMARY KEY,
  LIKE identity_template INCLUDING CONSTRAINTS
  -- name text UNIQUE NOT NULL
) WITH (
  OIDS=FALSE
);

CREATE TABLE artifact_node (
  id bigserial PRIMARY KEY,
  LIKE identity_template INCLUDING CONSTRAINTS,
  artifact_graph_id bigint NOT NULL REFERENCES artifact_graph (id) DEFERRABLE INITIALLY IMMEDIATE
) WITH (
  OIDS=FALSE
);

CREATE TABLE artifact (
  datatype_id bigint NOT NULL REFERENCES datatype (id),
  name text
) INHERITS (artifact_node)
WITH (
  OIDS=FALSE
);

CREATE TABLE producer (
  name text NOT NULL
) INHERITS (artifact_node)
WITH (
  OIDS=FALSE
);

CREATE TABLE artifact_edge (
  source_id bigint NOT NULL REFERENCES artifact_node (id) DEFERRABLE INITIALLY IMMEDIATE,
  dependent_id bigint NOT NULL REFERENCES artifact_node (id) DEFERRABLE INITIALLY IMMEDIATE,
  PRIMARY KEY (source_id, dependent_id)
) WITH (
  OIDS=FALSE
);

CREATE TABLE artifact_producer_edge (
  name text NOT NULL
) INHERITS (artifact_edge)
WITH (
  OIDS=FALSE
);

CREATE TABLE artifact_dtype_edge (
  name text NOT NULL
) INHERITS (artifact_edge)
WITH (
  OIDS=FALSE
);
