use std::collections::HashMap;
use std::convert::From;
use std::fmt;
use std::num::ParseIntError;
use std::str::FromStr;

use heraclitus_core::uuid;
use heraclitus_macros::{
    DatatypeMarker,
    stored_datatype_controller,
};
use uuid::Uuid;

use crate::{
    Artifact,
    Identity,
    RepresentationKind,
    Error,
    Version,
};
use crate::datatype::{
    Description,
    DependencyDescription,
    DependencyTypeRestriction,
    DependencyCardinalityRestriction,
    DependencyStoreRestriction,
    InterfaceControllerEnum,
    Model,
};
use crate::repo::Repository;


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
        write!(f, "{}", match *self {
            RevisionPath::Head => "HEAD",
            RevisionPath::Named(ref s) => s,
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
        if s.starts_with('#') {
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
///                ref_artifact: ArtifactSpecifier::Name("all".to_string()),
///                branch_revision: BranchRevisionSpecifier::from_str("master:squash~1").unwrap(),
///                artifact: ArtifactSpecifier::Name("data".to_string()),
///            },
///            VersionSpecifier::from_str("all/master:squash~1/data").unwrap());
/// ```
#[derive(Debug, PartialEq)]
pub enum VersionSpecifier {
    Uuid(UuidSpecifier),
    BranchArtifact {
        ref_artifact: ArtifactSpecifier,
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
            3 => Ok(VersionSpecifier::BranchArtifact {
                    ref_artifact: ArtifactSpecifier::from_str(tokens[0])?,
                    branch_revision: BranchRevisionSpecifier::from_str(tokens[1])?,
                    artifact: ArtifactSpecifier::from_str(tokens[2])?,
                }),
            _ => Err(RevisionSpecifierError::Format),
        }
    }
}


#[derive(Default, DatatypeMarker)]
pub struct Ref;

impl<T: InterfaceControllerEnum> Model<T> for Ref {
    fn info(&self) -> Description<T> {
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

    datatype_controllers!(Ref, ());
}


#[stored_datatype_controller(Ref)]
pub trait Storage {
    fn get_branch_revision_tips(
        &self,
        repo: &Repository,
        artifact: &Artifact,
    ) -> Result<HashMap<BranchRevisionTip, Identity>, Error>;

    fn set_branch_revision_tips(
        &mut self,
        repo: &Repository,
        artifact: &Artifact,
        tip_versions: &HashMap<BranchRevisionTip, Identity>,
    ) -> Result<(), Error>;

    fn write_message(
        &mut self,
        repo: &Repository,
        version: &Version,
        message: &Option<String>,
    ) -> Result<(), Error>;

    fn read_message(
        &self,
        repo: &Repository,
        version: &Version,
    ) -> Result<Option<String>, Error>;

    fn create_branch(
        &mut self,
        repo: &Repository,
        ref_version: &Version,
        name: &str,
    ) -> Result<(), Error>;

    fn get_version_id(
        &self,
        repo: &Repository,
        specifier: &VersionSpecifier,
    ) -> Result<Identity, Error>;
}
