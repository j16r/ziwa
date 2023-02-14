use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum Command {
    ShutDown,
    FilesAdd(PathBuf),
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum Response {
    Ok,
}
