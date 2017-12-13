CREATE TABLE arbitrary_partitioning (
  version_id bigint PRIMARY KEY REFERENCES version (id) DEFERRABLE INITIALLY IMMEDIATE,
  partition_ids bigint[] NOT NULL
) WITH (
  OIDS=FALSE
);
