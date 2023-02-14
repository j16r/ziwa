use std::io;
use std::path::{Path, PathBuf};

use actix_rt::net::TcpStream;
use actix_server::Server;
use actix_service::{fn_service, ServiceFactoryExt as _};
use bytes::BytesMut;
use futures_util::future::ok;
use postcard::from_bytes;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use walkdir::WalkDir;

use crate::rpc::{Command, Response};

pub async fn run() -> io::Result<()> {
    let addr = ("127.0.0.1", 34982);
    tracing::info!("starting server on port: {}", &addr.0);

    Server::build()
        .bind("control", addr, move || {
            fn_service(move |mut stream: TcpStream| {
                async move {
                    let mut size = 0;
                    let mut buf = BytesMut::new();

                    loop {
                        match stream.read_buf(&mut buf).await {
                            // end of stream; bail from loop
                            Ok(0) => break,

                            // more bytes to process
                            Ok(bytes_read) => {
                                tracing::info!("read {} bytes", bytes_read);
                                stream.write_all(&buf[size..]).await.unwrap();
                                size += bytes_read;
                            }

                            // stream error; bail from loop with error
                            Err(err) => {
                                tracing::error!("stream error: {:?}", err);
                                return Err(());
                            }
                        }
                    }

                    // send data down service pipeline
                    Ok((buf.freeze(), size))
                }
            })
            .map_err(|err| tracing::error!("service error: {:?}", err))
            .and_then(move |(bytes, size)| {
                let command: Command = from_bytes(&(bytes as bytes::Bytes)[..]).unwrap();
                tracing::trace!("got command {:?}", command);

                tokio::spawn(async move {
                    work(&command).await;
                });

                ok(size)
            })
        })?
        .workers(2)
        .run()
        .await
}

pub async fn work(command: &Command) -> io::Result<()> {
    match command {
        Command::ShutDown => {
            unimplemented!();
        },
        Command::FilesAdd(path) => {
            add_path(path).await?;
        }
    }

    Ok(())
}

pub async fn add_path(path: &Path) -> io::Result<()> {
    let mut walker = WalkDir::new(path).into_iter();
    loop {
        if let Some(entry) = walker.next() {
            let entry = entry?;
            if !entry.file_type().is_file() {
                continue;
            }

            tracing::trace!("adding {:?}", entry);
        } else {
            break;
        }
    }

    Ok(())
}
