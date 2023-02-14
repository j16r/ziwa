use std::io;
use std::path::Path;

use actix_rt::net::TcpStream;
use postcard::to_allocvec;

use crate::rpc::Command;

pub async fn add(path: &Path) -> io::Result<()> {
    tracing::info!("adding path: {:?}", &path);

    let stream = TcpStream::connect("127.0.0.1:34982").await?;
    let command = Command::FilesAdd(path.to_path_buf());

    let output = to_allocvec(&command).unwrap();
    let result = stream.try_write(&output).unwrap();
    tracing::trace!("wrote {} with result: {:?}", &output.len(), result);

    Ok(())
}
