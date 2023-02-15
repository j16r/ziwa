use std::io::{self, Cursor};
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use actix_rt::net::TcpStream;
use actix_server::Server;
use actix_service::{fn_service, ServiceFactoryExt as _};
use bytes::BytesMut;
use postcard::{from_bytes, to_allocvec};
use rayon::{ThreadPool, ThreadPoolBuilder};
use surrealdb::Datastore;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use walkdir::WalkDir;

use crate::rpc::{Command, Response};

async fn spawn_worker(
    ds: Arc<Mutex<Datastore>>,
    thread_pool: Arc<Mutex<ThreadPool>>,
    command: &Command,
) -> io::Result<()> {
    let worker_command = command.clone();
    let worker = move || {
        tracing::trace!("worker...");
        work(&worker_command);
    };

    let thread_pool = thread_pool.clone();

    let ds = ds.lock().unwrap();
    let mut tx = ds.transaction(true, false).await.unwrap();
    let bytes = to_allocvec(&command).unwrap();
    tx.put("job1", bytes).await.unwrap();
    tx.commit().await.unwrap();

    thread_pool.lock().unwrap().install(worker);

    Ok(())
}

pub async fn run() -> io::Result<()> {
    let addr = ("127.0.0.1", 34982);
    tracing::info!("starting server on port: {}", &addr.0);

    let ds = Arc::new(Mutex::new(Datastore::new("memory").await.unwrap()));
    let thread_pool = Arc::new(Mutex::new(ThreadPoolBuilder::new().build().unwrap()));

    Server::build()
        .bind("control", addr, move || {
            let thread_pool = thread_pool.clone();
            let ds = ds.clone();

            fn_service(move |mut stream: TcpStream| {
                let thread_pool = thread_pool.clone();
                let ds = ds.clone();

                async move {
                    let mut len_input = 0_usize.to_be_bytes();
                    match stream.read_exact(&mut len_input).await {
                        Ok(bytes_read) => {
                            tracing::trace!("read bytes {:?}", bytes_read);
                        },
                        Err(err) => {
                            tracing::error!("stream error: {:?}", err);
                            return Err(());
                        }
                    }

                    let len = usize::from_be_bytes(len_input);
                    let mut bytes = vec![0; len];
                    match stream.read_exact(&mut bytes).await {
                        Ok(bytes_read) => {
                            tracing::trace!("read bytes {:?}", bytes_read);
                        },
                        Err(err) => {
                            tracing::error!("stream error: {:?}", err);
                            return Err(());
                        }
                    }

                    tracing::trace!("completed reading input from cli");

                    let command: Command = from_bytes(&bytes).unwrap();
                    tracing::trace!("got command {:?}", command);

                    let response = match spawn_worker(ds, thread_pool, &command).await {
                        Ok(()) => {
                            Response::Ok
                        },
                        Err(_) => {
                            Response::Error
                        }
                    };

                    let mut output = Cursor::new(to_allocvec(&response).unwrap());
                    stream.write_buf(&mut output).await.unwrap();
                    
                    Ok(())
                }
            })
            .map_err(|err| tracing::error!("service error: {:?}", err))
        })?
        .run()
        .await
}

pub fn work(command: &Command) {
    tracing::info!("sarting worker on {:?}", &command);

    let result = match command {
        Command::ShutDown => {
            unimplemented!();
        }
        Command::FilesAdd(path) => add_path(path),
    };

    match result {
        Ok(()) => {
            tracing::info!("worker completed");
        }
        Err(e) => {
            tracing::error!("worker failed with {}", e);
        }
    }
}

pub fn add_path(path: &Path) -> io::Result<()> {
    tracing::trace!("add_path processing ...");
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

pub fn fetch_file(path: &Path) -> io::Result<()> {
    tracing::trace!("fetch_file processing ...");

    Ok(())
}
