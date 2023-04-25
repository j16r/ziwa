use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use strum_macros::{Display, EnumString};
use ulid::Ulid;

#[derive(Serialize, Deserialize, Display, Clone, Debug, Eq, PartialEq, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum Command {
    ShutDown,
    FilesAdd(PathBuf),
    SummarizePdf(Ulid),
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum Response {
    Ok,
    Error,
}
