use std::io;
use std::path::Path;

use clap::{arg, Command};

mod client;
mod rpc;
mod server;

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
                .subcommand(Command::new("run").about("Run the server")),
        )
        .subcommand(
            Command::new("files")
                .about("Manage files in the lake")
                .subcommand(
                    Command::new("add")
                        .about("Add files to the lake")
                        .arg(arg!(<PATH> "path where file or files live")),
                ),
        )
        .get_matches();

    if let Some(subcommand) = matches.subcommand_matches("server") {
        if let Some(_subcommand) = subcommand.subcommand_matches("run") {
            server::run().await?;
        }
    } else if let Some(subcommand) = matches.subcommand_matches("files") {
        if let Some(subcommand) = subcommand.subcommand_matches("add") {
            let path = subcommand.get_one::<String>("PATH").unwrap();
            client::add(Path::new(path)).await?;
        }
    }

    Ok(())
}
