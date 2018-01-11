extern crate schemer;


use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::convert::From;
use std::fmt;
use std::num::ParseIntError;
use std::str::FromStr;

use uuid::Uuid;
use postgres::error::Error as PostgresError;
use postgres::transaction::Transaction;
use schemer_postgres::{PostgresAdapter, PostgresMigration};

use ::{Artifact, HashType, Identity, RepresentationKind, Error, Version};
use ::datatype::{
    Description, DependencyDescription, DependencyTypeRestriction,
    DependencyCardinalityRestriction, DependencyStoreRestriction,
    MetaController,
    Model, PostgresMetaController, StoreMetaController};
use ::repo::{PostgresMigratable, PostgresRepoController};
use ::store::Store;


// TODO: Will scrap all of this string spec format for something more git-like.

/// A path for dereferencing a revision specifier.
///
/// # Examples
///
/// ```
/// use std::str::FromStr;
/// use heraclitus::datatype::reference::RevisionPath;
///
/// // The default path is always `HEAD`.
/// assert_eq!(RevisionPath::Head, RevisionPath::from_str("").unwrap());
/// // Any head-like string is also `HEAD`.
/// assert_eq!(RevisionPath::Head, RevisionPath::from_str("HEAD").unwrap());
/// assert_eq!(RevisionPath::Head, RevisionPath::from_str("head").unwrap());
/// // Otherwise use a named path.
/// assert_eq!(RevisionPath::Named("squash".to_string()),
///            RevisionPath::from_str("squash").unwrap());
/// ```
#[derive(Debug, Eq, Hash, PartialEq)]
pub enum RevisionPath {
    Head,
    Named(String),
}

impl FromStr for RevisionPath {
    type Err = !;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() || s.to_lowercase() == "head" {
            Ok(RevisionPath::Head)
        } else {
            Ok(RevisionPath::Named(s.to_string()))
        }
    }
}

impl fmt::Display for RevisionPath {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", match self {
            &RevisionPath::Head => "HEAD",
            &RevisionPath::Named(ref s) => s,
        })
    }
}

/// Specifies a revision path and offset from that path's tip.
///
/// # Examples
///
/// ```
/// use std::str::FromStr;
/// use heraclitus::datatype::reference::{RevisionPath, RevisionSpecifier};
///
/// // The default revision is always even with `HEAD`.
/// assert_eq!(RevisionSpecifier {path: RevisionPath::Head, offset: 0},
///            RevisionSpecifier::from_str("").unwrap());
/// // Offsets are on the `HEAD` path by default.
/// assert_eq!(RevisionSpecifier {path: RevisionPath::Head, offset: -3},
///            RevisionSpecifier::from_str("~3").unwrap());
/// // Otherwise simple strings are even with paths of the name.
/// assert_eq!(RevisionSpecifier {path: RevisionPath::Named("squash".to_string()), offset: 0},
///            RevisionSpecifier::from_str("squash").unwrap());
/// // Full specifiers use '#' as the path/offset delimiter.
/// assert_eq!(RevisionSpecifier {path: RevisionPath::Named("squash".to_string()), offset: -1},
///            RevisionSpecifier::from_str("squash~1").unwrap());
/// ```
#[derive(Debug, PartialEq)]
pub struct RevisionSpecifier {
    pub path: RevisionPath,
    pub offset: i64,
}

#[derive(Debug)]
pub enum RevisionSpecifierError {
    Format,
    Offset(ParseIntError),
}

impl From<ParseIntError> for RevisionSpecifierError {
    fn from(e: ParseIntError) -> Self {
        RevisionSpecifierError::Offset(e)
    }
}

impl FromStr for RevisionSpecifier {
    type Err = RevisionSpecifierError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let tokens: Vec<&str> = s.split('~').collect();

        match tokens.len() {
            1 => Ok(RevisionSpecifier {
                    path: RevisionPath::from_str(tokens[0])?,
                    offset: 0,
                }),
            2 => Ok(RevisionSpecifier {
                    path: RevisionPath::from_str(tokens[0])?,
                    offset: -tokens[1].parse::<i64>()?,
                }),
            _ => Err(RevisionSpecifierError::Format),
        }
    }
}

// TODO: figure out what is going on with Rust/never types here.
impl From<!> for RevisionSpecifierError {
    fn from(_: !) -> Self {
        unreachable!()
    }
}

/// Specifies a UUID, either complete or a partial prefix for lookup.
///
/// # Examples
///
/// ```
/// use std::str::FromStr;
/// use heraclitus::datatype::reference::UuidSpecifier;
///
/// assert!(match UuidSpecifier::from_str("f3b4958c-52a1-11e7-802a-010203040506").unwrap() {
///     UuidSpecifier::Complete(_) => true, _ => false });
/// assert_eq!(UuidSpecifier::Partial("abcd1".to_string()),
///            UuidSpecifier::from_str("abcd1").unwrap());
/// ```
#[derive(Debug, PartialEq)]
pub enum UuidSpecifier {
    Complete(Uuid),
    Partial(String),
}

impl FromStr for UuidSpecifier {
    type Err = !;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match Uuid::parse_str(s) {
            Ok(uuid) => UuidSpecifier::Complete(uuid),
            Err(_) => UuidSpecifier::Partial(s.to_string()),
        })
    }
}

/// Specifies an artifact identity, either via UUID (prefixed with '#') or name.
///
/// # Examples
///
/// ```
/// use std::str::FromStr;
/// use heraclitus::datatype::reference::{ArtifactSpecifier, UuidSpecifier};
///
/// assert_eq!(ArtifactSpecifier::Uuid(UuidSpecifier::Partial("abcd1".to_string())),
///            ArtifactSpecifier::from_str("#abcd1").unwrap());
/// assert_eq!(ArtifactSpecifier::Name("abcd1".to_string()),
///            ArtifactSpecifier::from_str("abcd1").unwrap());
/// ```
#[derive(Debug, PartialEq)]
pub enum ArtifactSpecifier {
    Uuid(UuidSpecifier),
    Name(String),
}

impl FromStr for ArtifactSpecifier {
    type Err = !;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with("#") {
            Ok(ArtifactSpecifier::Uuid(UuidSpecifier::from_str(&s[1..])?))
        } else {
            Ok(ArtifactSpecifier::Name(s.to_string()))
        }
    }
}

pub type BranchSpecifier = String;

#[derive(Debug, Eq, Hash, PartialEq)]
pub struct BranchRevisionTip {
    pub name: BranchSpecifier,
    pub revision: RevisionPath,
}

/// Specifies a branch and revision along it.
///
/// # Examples
///
/// ```
/// use std::str::FromStr;
/// use heraclitus::datatype::reference::{BranchRevisionSpecifier, RevisionPath, RevisionSpecifier};
///
/// assert_eq!(BranchRevisionSpecifier {
///                name: "master".to_string(),
///                revision: RevisionSpecifier {path: RevisionPath::Head, offset: 0},
///            },
///            BranchRevisionSpecifier::from_str("master").unwrap());
/// assert_eq!(BranchRevisionSpecifier {
///                name: "master".to_string(),
///                revision: RevisionSpecifier {
///                    path: RevisionPath::Named("squash".to_string()),
///                    offset: -1,
///                },
///            },
///            BranchRevisionSpecifier::from_str("master:squash~1").unwrap());
/// ```
#[derive(Debug, PartialEq)]
pub struct BranchRevisionSpecifier {
    pub name: BranchSpecifier,
    pub revision: RevisionSpecifier,
}

impl FromStr for BranchRevisionSpecifier {
    type Err = RevisionSpecifierError;  // TODO

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let tokens: Vec<&str> = s.split(':').collect();

        match tokens.len() {
            1 => Ok(BranchRevisionSpecifier {
                    name: tokens[0].to_string(),
                    revision: RevisionSpecifier::from_str("")?,
                }),
            2 => Ok(BranchRevisionSpecifier {
                    name: tokens[0].to_string(),
                    revision: RevisionSpecifier::from_str(tokens[1])?,
                }),
            _ => Err(RevisionSpecifierError::Format),
        }
    }
}

/// Specifies a particular version, either directly or via branch and artifact.
///
/// # Examples
///
/// ```
/// use std::str::FromStr;
/// use heraclitus::datatype::reference::{
///     ArtifactSpecifier,
///     BranchRevisionSpecifier,
///     RevisionPath,
///     RevisionSpecifier,
///     UuidSpecifier,
///     VersionSpecifier,
/// };
///
/// assert_eq!(VersionSpecifier::Uuid(UuidSpecifier::Partial("abcd1".to_string())),
///            VersionSpecifier::from_str("#abcd1").unwrap());
/// assert_eq!(VersionSpecifier::BranchArtifact {
///                branch_revision: BranchRevisionSpecifier::from_str("master:squash~1").unwrap(),
///                artifact: ArtifactSpecifier::Name("data".to_string()),
///            },
///            VersionSpecifier::from_str("master:squash~1/data").unwrap());
/// ```
#[derive(Debug, PartialEq)]
pub enum VersionSpecifier {
    Uuid(UuidSpecifier),
    BranchArtifact {
        branch_revision: BranchRevisionSpecifier,
        artifact: ArtifactSpecifier,
    },
}

impl FromStr for VersionSpecifier {
    type Err = RevisionSpecifierError;  // TODO

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let tokens: Vec<&str> = s.split('/').collect();

        match tokens.len() {
            1 => if tokens[0].starts_with('#') {
                    Ok(VersionSpecifier::Uuid(UuidSpecifier::from_str(&tokens[0][1..])?))
                } else {
                    Err(RevisionSpecifierError::Format)
                },
            2 => Ok(VersionSpecifier::BranchArtifact {
                    branch_revision: BranchRevisionSpecifier::from_str(tokens[0])?,
                    artifact: ArtifactSpecifier::from_str(tokens[1])?,
                }),
            _ => Err(RevisionSpecifierError::Format),
        }
    }
}


#[derive(Default)]
pub struct Ref;

impl<T> Model<T> for Ref {
    fn info(&self) -> Description {
        Description {
            name: "Ref".into(),
            version: 1,
            representations: vec![
                        RepresentationKind::State,
                    ]
                    .into_iter()
                    .collect(),
            implements: vec![],
            dependencies: vec![
                DependencyDescription::new(
                    "ref",
                    DependencyTypeRestriction::Any,
                    DependencyCardinalityRestriction::Unbounded,
                    DependencyStoreRestriction::Same,
                ),
            ],
        }
    }

    fn meta_controller(&self, store: Store) -> Option<StoreMetaController> {
        match store {
            Store::Postgres => Some(StoreMetaController::Postgres(Box::new(PostgresStore {}))),
            _ => None,
        }
    }

    fn interface_controller(
        &self,
        _store: Store,
        _name: &str,
    ) -> Option<T> {
        None
    }
}

pub fn model_controller(store: Store) -> impl ModelController {
    match store {
        Store::Postgres => PostgresStore {},
        _ => unimplemented!(),
    }
}


pub trait ModelController {
    fn get_branch_revision_tips(
        &self,
        repo_control: &mut ::repo::StoreRepoController,
        artifact: &Artifact,
    ) -> Result<HashMap<BranchRevisionTip, Identity>, Error>;

    fn set_branch_revision_tips(
        &mut self,
        repo_control: &mut ::repo::StoreRepoController,
        artifact: &Artifact,
        tip_versions: &HashMap<BranchRevisionTip, Identity>,
    ) -> Result<(), Error>;

    fn write_message(
        &mut self,
        repo_control: &mut ::repo::StoreRepoController,
        version: &Version,
        message: &Option<String>,
    ) -> Result<(), Error>;

    fn read_message(
        &self,
        repo_control: &mut ::repo::StoreRepoController,
        version: &Version,
    ) -> Result<Option<String>, Error>;

    fn create_branch(
        &mut self,
        repo_control: &mut ::repo::StoreRepoController,
        ref_version: &Version,
        name: &str,
    ) -> Result<(), Error>;

    fn get_version_id(
        &self,
        repo_control: &mut ::repo::StoreRepoController,
        specifier: &VersionSpecifier,
    ) -> Result<Identity, Error>;
}


pub struct PostgresStore {}

struct PGMigrationRefs;
migration!(
    PGMigrationRefs,
    "56f909ff-056b-4a7e-b589-c43d479cf9d6",
    [
        "7d1fb6d1-a1b0-4bd4-aa6d-e3ee71c4353b", // Artifact graph 0001.
    ],
    "create ref table");

impl PostgresMigration for PGMigrationRefs {
    fn up(&self, transaction: &Transaction) -> Result<(), PostgresError> {
        transaction.batch_execute(include_str!("sql/ref_0001.up.sql"))
    }

    fn down(&self, transaction: &Transaction) -> Result<(), PostgresError> {
        transaction.batch_execute(include_str!("sql/ref_0001.down.sql"))
    }
}

impl MetaController for PostgresStore {}

impl PostgresMigratable for PostgresStore {
    fn migrations(&self) -> Vec<Box<<PostgresAdapter as schemer::Adapter>::MigrationType>> {
        vec![
            Box::new(PGMigrationRefs),
        ]
    }
}

impl PostgresMetaController for PostgresStore {}

impl ModelController for PostgresStore {
    fn get_branch_revision_tips(
        &self,
        repo_control: &mut ::repo::StoreRepoController,
        artifact: &Artifact,
    ) -> Result<HashMap<BranchRevisionTip, Identity>, Error> {
        let rc: &mut PostgresRepoController = repo_control.borrow_mut();

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        enum BranchHeadRow {
            BranchName = 0,
            RevisionPathName,
            VersionUUID,
            VersionHash,
        };
        let branch_head_rows = trans.query(r#"
            SELECT b.name, rp.name, rv.uuid_, rv.hash
            FROM artifact a
            JOIN branch b ON (b.ref_artifact_id = a.id)
            JOIN revision_path rp ON (rp.branch_id = b.id AND rp.name = 'HEAD')
            JOIN version rv ON (rv.id = rp.ref_version_id)
            WHERE (a.uuid_ = $1::uuid AND a.hash = $2::bigint);
        "#, &[&artifact.id.uuid, &(artifact.id.hash as i64)])?;

        let map = branch_head_rows.iter().map(|row| {
            let br_tip = BranchRevisionTip {
                name: row.get(BranchHeadRow::BranchName as usize),
                revision: RevisionPath::from_str(&row.get::<_, String>(BranchHeadRow::RevisionPathName as usize))
                    .expect("TODO"),
            };
            let id = Identity {
                uuid: row.get(BranchHeadRow::VersionUUID as usize),
                hash: row.get::<_, i64>(BranchHeadRow::VersionHash as usize) as HashType,
            };
            (br_tip, id)
        }).collect();

        Ok(map)
    }

    fn set_branch_revision_tips(
        &mut self,
        repo_control: &mut ::repo::StoreRepoController,
        artifact: &Artifact,
        tip_versions: &HashMap<BranchRevisionTip, Identity>,
    ) -> Result<(), Error> {
        let rc: &mut PostgresRepoController = repo_control.borrow_mut();

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        let mut b_names = vec![];
        let mut rp_names = vec![];
        let mut v_uuids = vec![];
        let mut v_hashes = vec![];

        for (tip, id) in tip_versions {
            b_names.push(&tip.name);
            rp_names.push(tip.revision.to_string());
            v_uuids.push(id.uuid);
            v_hashes.push(id.hash as i64);
        }

        trans.execute(r#"
                INSERT INTO revision_path (branch_id, name, ref_version_id)
                (SELECT b.id, r.rp_name, v.id
                FROM UNNEST($1::text[], $2::text[], $3::uuid[], $4::bigint[])
                  AS r (b_name, rp_name, v_uuid, v_hash)
                JOIN branch b ON (
                    b.name = r.b_name AND
                    b.ref_artifact_id = (
                        SELECT id
                        FROM artifact
                        WHERE uuid_ = $5::uuid
                          AND hash = $6::bigint
                    )
                )
                JOIN version v
                  ON (v.uuid_ = r.v_uuid AND v.hash = r.v_hash))
                ON CONFLICT (branch_id, name) DO UPDATE SET ref_version_id = EXCLUDED.ref_version_id;
            "#,
            &[&b_names, &rp_names, &v_uuids, &v_hashes, &artifact.id.uuid, &(artifact.id.hash as i64)])?;

        Ok(trans.commit()?)
    }

    fn write_message(
        &mut self,
        repo_control: &mut ::repo::StoreRepoController,
        version: &Version,
        message: &Option<String>,
    ) -> Result<(), Error> {
        let rc: &mut PostgresRepoController = repo_control.borrow_mut();

        match *message {
            Some(ref t) => {
                let conn = rc.conn()?;
                let trans = conn.transaction()?;

                trans.execute(r#"
                    INSERT INTO ref (version_id, message)
                    SELECT v.id, r.message
                    FROM (VALUES ($1::uuid, $2::bigint, $3::text))
                      AS r (uuid_, hash, message)
                    JOIN version v
                      ON (v.uuid_ = r.uuid_ AND v.hash = r.hash);
                "#, &[&version.id.uuid, &(version.id.hash as i64), t])?;

                trans.set_commit();
                Ok(())
            },
            None => Ok(())
        }
    }

    fn read_message(
        &self,
        repo_control: &mut ::repo::StoreRepoController,
        version: &Version,
    ) -> Result<Option<String>, Error> {
        let rc: &mut PostgresRepoController = repo_control.borrow_mut();

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        let message_rows = trans.query(r#"
            SELECT v.message
            FROM version v
            WHERE (v.uuid_ = $1::uuid AND v.hash = $2::bigint);
        "#, &[&version.id.uuid, &(version.id.hash as i64)])?;

        Ok(message_rows.get(0).get(0))
    }

    fn create_branch(
        &mut self,
        repo_control: &mut ::repo::StoreRepoController,
        ref_version: &Version,
        name: &str,
    ) -> Result<(), Error> {
        let rc: &mut PostgresRepoController = repo_control.borrow_mut();

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        trans.execute(r#"
            WITH insert_branch AS (
                INSERT INTO branch (ref_artifact_id, name)
                SELECT a.id, $3::text
                FROM artifact a WHERE uuid_ = $4::uuid AND hash = $5::bigint
                RETURNING id
            )
            INSERT INTO revision_path (branch_id, name, ref_version_id)
            SELECT
                ib.id,
                'HEAD',
                (SELECT id FROM version
                 WHERE uuid_ = $1::uuid AND hash = $2::bigint)
            FROM insert_branch AS ib (id);
        "#, &[&ref_version.id.uuid, &(ref_version.id.hash as i64), &name,
              &ref_version.artifact.id.uuid, &(ref_version.artifact.id.hash as i64)])?;

        trans.set_commit();
        Ok(())
    }

    fn get_version_id(
        &self,
        repo_control: &mut ::repo::StoreRepoController,
        specifier: &VersionSpecifier,
    ) -> Result<Identity, Error> {
        let rc: &mut PostgresRepoController = repo_control.borrow_mut();

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        let version_rows = match *specifier {
            VersionSpecifier::Uuid(ref us) => {
                match *us {
                    UuidSpecifier::Complete(ref uuid) => {
                        trans.query(r#"
                            SELECT v.uuid_, v.hash
                            FROM version v
                            WHERE v.uuid_ = $1::uuid;
                        "#, &[uuid])?
                    },
                    UuidSpecifier::Partial(ref prefix) => {
                        trans.query(r#"
                            SELECT v.uuid_, v.hash
                            FROM version v
                            WHERE v.uuid_::text ILIKE $1::text || '%';
                        "#, &[prefix])?
                    }
                }
            },
            VersionSpecifier::BranchArtifact {branch_revision: ref br, artifact: ref art} => {
                assert_eq!(br.revision.offset, 0, "Non-tip revisions not yet supported"); // TODO

                let br_rev_path_name = match br.revision.path {
                    RevisionPath::Head => "HEAD",
                    RevisionPath::Named(ref name) => name,
                };

                enum ArtFilterParam<'a> {
                    Uuid(&'a Uuid),
                    Text(&'a str),
                }

                impl<'a> ArtFilterParam<'a> {
                    fn uuid(&self) -> Option<&Uuid> {
                        match *self {
                            ArtFilterParam::Uuid(ref uuid) => Some(uuid),
                            ArtFilterParam::Text(_) => None,
                        }
                    }

                    fn text(&self) -> Option<&str> {
                        match *self {
                            ArtFilterParam::Uuid(_) => None,
                            ArtFilterParam::Text(ref s) => Some(s),
                        }
                    }
                }

                let (art_filter, filter_param) = match *art {
                    ArtifactSpecifier::Uuid(ref us) => {
                        match *us {
                            UuidSpecifier::Complete(ref uuid) =>
                                ("$4::text IS NULL AND tva.uuid_ = $3::uuid", ArtFilterParam::Uuid(uuid)),
                            UuidSpecifier::Partial(ref prefix) =>
                                ("$3::uuid IS NULL AND tva.uuid_::text ILIKE $4::text || '%'", ArtFilterParam::Text(prefix)),
                        }
                    },
                    ArtifactSpecifier::Name(ref name) =>
                        ("$3::uuid IS NULL AND tva.name = $4::text", ArtFilterParam::Text(name)),
                };

                trans.query(
                    &format!(r#"
                        SELECT tv.uuid_, tv.hash
                        FROM branch b
                        JOIN revision_path rp ON (rp.branch_id = b.id)
                        JOIN version_relation rvr ON (rvr.dependent_version_id = rp.ref_version_id)
                        JOIN version tv ON (tv.id = rvr.source_version_id)
                        JOIN artifact tva ON (tva.id = tv.artifact_id)
                        WHERE b.name = $1::text
                          AND rp.name = $2::text
                          AND {};
                    "#, art_filter),
                    &[&br.name, &br_rev_path_name, &filter_param.uuid(), &filter_param.text()])?
            },
        };

        match version_rows.len() {
            0 => panic!("TODO: no rows"),
            1 => Ok(Identity {
                uuid: version_rows.get(0).get(0),
                hash: version_rows.get(0).get::<_, i64>(1) as HashType,
            }),
            _ => panic!("TODO: too many rows"),
        }
    }
}
