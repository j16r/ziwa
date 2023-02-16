use std::io;

use std::path::Path;

use actix_rt::net::TcpStream;
use bytes::BytesMut;
use postcard::{from_bytes, to_allocvec};

use crate::rpc::{Command, Response};

pub async fn add(path: &Path) -> io::Result<()> {
    tracing::info!("adding path: {:?}", &path);

    let stream = TcpStream::connect("127.0.0.1:34982").await?;
    let command = Command::FilesAdd(path.to_path_buf());

    let output = to_allocvec(&command).unwrap();
    let len_output = output.len().to_be_bytes();
    stream.try_write(&len_output).unwrap();

    let result = stream.try_write(&output).unwrap();
    tracing::trace!("wrote {} with result: {:?}", &output.len(), result);

    let mut bytes = BytesMut::new();
    loop {
        match stream.try_read_buf(&mut bytes) {
            Ok(0) => break,
            Ok(_) => continue,
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                continue;
            }
            Err(err) => {
                tracing::error!("stream error: {:?}", err);
                unimplemented!();
            }
        }
    }

    let buffer: bytes::Bytes = bytes.freeze();
    let response: Response = from_bytes(&buffer[..]).unwrap();
    match response {
        Response::Ok => tracing::trace!("completed successfully"),
        Response::Error => tracing::error!("command failed"),
    }

    Ok(())
}
