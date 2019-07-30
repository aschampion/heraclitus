use prettytable::{Table, cell, row,};
use structopt::StructOpt;
use url::Url;

use heraclitus::{
    url,
    datatype::{
        artifact_graph::{
            ArtifactGraphDtype,
            Storage,
        },
        DatatypeMarker,
    },
    repo::{
        Repository,
        RepoController,
    },
};

#[derive(StructOpt, Debug)]
#[structopt(name = "heracli")]
struct Options {
    #[structopt(short = "r", long = "repo")]
    repo: String,
    #[structopt(subcommand)]
    command: Command,
}

#[derive(StructOpt, Debug)]
enum Command {
    #[structopt(name = "init")]
    Init,
    #[structopt(name = "ls")]
    List {
        #[structopt(long = "origin")]
        resolve_origin: bool,
    },
}

fn main() -> Result<(), heraclitus::Error> {
    let opt = Options::from_args();

    let repo_location = heraclitus::RepositoryLocation {
        url: Url::parse(&opt.repo).expect("TODO"),
    };
    let mut repo = Repository::new(&repo_location);
    // TODO: should not be in testing module, should be configurable, etc.
    let dtype_registry = heraclitus::datatype::testing::init_default_dtypes_registry();

    match opt.command {
        Command::Init => {
            repo.init(&dtype_registry)?;
            let mut ag_store = ArtifactGraphDtype::store(&repo);
            ag_store.get_or_create_origin_root(&dtype_registry, &repo)?;
        },
        Command::List {resolve_origin} => {
            let mut ag_store = ArtifactGraphDtype::store(&repo);
            let (origin_ag, root_ag) = ag_store.get_or_create_origin_root(&dtype_registry, &repo)?;

            let resolve_root = if resolve_origin {
                origin_ag
            } else {
                root_ag
            };

            let mut table = Table::new();

            for art_idx in resolve_root.artifacts.graph().node_indices() {
                let art = &resolve_root.artifacts[art_idx];
                table.add_row(row![
                    art.id.uuid,
                    art.id.hash,
                    art.name().as_ref().map_or("", String::as_ref),
                    art.dtype.name,
                ]);
            }

            table.printstd();
        },
    }

    Ok(())
}
