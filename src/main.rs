use std::io;
use std::path::Path;

use clap::{arg, Command};
use tracing_subscriber::EnvFilter;

mod client;
mod rpc;
mod server;

#[tokio::main]
async fn main() -> io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .event_format(
            tracing_subscriber::fmt::format()
                .with_file(true)
                .with_line_number(true),
        )
        .init();

    let matches = Command::new("ziwa")
        .version("1.0")
        .author("John Barker <me@j16r.net>")
        .about("Knowledge Transformation CLI")
        .subcommand_required(true)
        .subcommand(
            Command::new("server")
                .about("Server commands")
                .arg_required_else_help(true)
                .subcommand(Command::new("run").about("Run the server")),
        )
        .subcommand(
            Command::new("files")
                .about("Manage files in the lake")
                .arg_required_else_help(true)
                .subcommand(
                    Command::new("add")
                        .about("Add files to the lake")
                        .arg(arg!(<PATH> "path where file or files live")),
                ),
        )
        .get_matches();

    if let Some(subcommand) = matches.subcommand_matches("server") {
        if let Some(_subcommand) = subcommand.subcommand_matches("run") {
            if let Err(e) = server::run().await {
                tracing::error!("error running server {}", e);
            }
        }
    } else if let Some(subcommand) = matches.subcommand_matches("files") {
        if let Some(subcommand) = subcommand.subcommand_matches("add") {
            let path = subcommand.get_one::<String>("PATH").unwrap();
            client::add(Path::new(path)).await?;
        }
    }

    Ok(())
}
