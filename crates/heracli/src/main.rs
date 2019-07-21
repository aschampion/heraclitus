use structopt::StructOpt;
use url::Url;

use heraclitus::{
    url,
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
        }
    }

    Ok(())
}
