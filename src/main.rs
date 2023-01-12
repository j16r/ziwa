use std::io;
use std::path::Path;

use clap::{Arg, Command};
use tracing::{debug, error, info};

mod server;

struct Lake {
}

impl Lake {
    fn add(&mut self, file: &Path) {
    }


}

struct Job {
    input: File
}

struct File {
    path: Path,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    tracing_subscriber::fmt::init();

    let matches = Command::new("ziwa")
        .version("1.0")
        .author("John Barker <me@j16r.net>")
        .about("Knowledge Transformation CLI")
        .subcommand_required(true)
        .subcommand(
            Command::new("server")
                .about("Server commands")
                .subcommand(
                    Command::new("run")
                        .about("Run the server")
                )
        )
        .subcommand(
            Command::new("files")
                .about("Manage files in the lake")
                .subcommand(
                    Command::new("add")
                        .about("Add files to the lake")
                        .arg(
                            Arg::new("file")
                        )
                )
        )
        .get_matches();

    if let Some(subcommand) = matches.subcommand_matches("server") {
        if let Some(subcommand) = subcommand.subcommand_matches("run") {
            server::run().await?;
        }
    } else if let Some(subcommand) = matches.subcommand_matches("files") {
        if let Some(subcommand) = subcommand.subcommand_matches("add") {
        }
    }

    Ok(())
}
