use std::collections::HashMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use strum_macros::{Display, EnumString};
use ulid::Ulid;

// trait ParamValue: Serialize + for<'a> Deserialize<'a> + std::fmt::Display + Clone + std::fmt::Debug + Eq {
// }

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct Command {
    pub name: CommandName,
    // inputs: HashSet<String, dyn ParamValue>,
    pub inputs: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, Display, Clone, Debug, Eq, PartialEq, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum CommandName {
    ShutDown,
    Reset,
    FilesAdd,      // (PathBuf),
    FetchFile,     // (Ulid),
    SummarizePdf,  // (Ulid),
    SummarizeEpub, // (Ulid),
    ExtractZip,    // (Ulid),
    ExtractExif,   // (Ulid),
    SniffMimeType, // (Ulid),
    Docker,
    Wasm,
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum Response {
    Ok,
    Error,
}
