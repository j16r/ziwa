use std::collections::HashMap;
use std::io;
use std::path::Path;

use actix_rt::net::TcpStream;
use bytes::BytesMut;

use crate::rpc::{Command, CommandName, Response};

pub async fn add(path: &Path) -> io::Result<()> {
    let command = Command {
        name: CommandName::FilesAdd,
        inputs: HashMap::from([("path".to_string(), path.to_string_lossy().into())]),
    };
    // (path.to_path_buf());
    send_command(&command).await
}

pub async fn reset() -> io::Result<()> {
    let command = Command {
        name: CommandName::Reset,
        inputs: HashMap::default(),
    };
    send_command(&command).await
}

pub async fn shutdown() -> io::Result<()> {
    let command = Command {
        name: CommandName::ShutDown,
        inputs: HashMap::default(),
    };
    send_command(&command).await
}

async fn send_command(command: &Command) -> io::Result<()> {
    let stream = TcpStream::connect("127.0.0.1:34982").await?;

    let output = bincode::serialize(&command).unwrap();
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
    let response: Response = bincode::deserialize(&buffer[..]).unwrap();
    match response {
        Response::Ok => tracing::trace!("completed successfully"),
        Response::Error => tracing::error!("command failed"),
    }

    Ok(())
}
