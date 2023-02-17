use std::io::{self, Cursor};
use std::path::Path;
use std::sync::{Arc, Mutex};

use actix_rt::net::TcpStream;
use actix_server::Server;
use actix_service::{fn_service, ServiceFactoryExt as _};
use awscreds::Credentials;
use awsregion::Region;
use postcard::{from_bytes, to_allocvec};
use rayon::{ThreadPool, ThreadPoolBuilder};
use s3::{Bucket, BucketConfiguration};
use s3::error::S3Error;
use surrealdb::Datastore;
use tokio::fs::File;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use ulid::Ulid;
use walkdir::WalkDir;

use crate::rpc::{Command, Response};

async fn spawn_worker(
    ds: Arc<Datastore>,
    _thread_pool: Arc<Mutex<ThreadPool>>,
    command: &Command,
) -> io::Result<()> {
    let worker_ds = ds.clone();
    let worker_command = command.clone();

    let mut tx = ds.transaction(true, false).await.unwrap();
    let bytes = to_allocvec(&command).unwrap();
    tx.put(format!("jobs:{}", Ulid::new()), bytes)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    tokio::spawn(async move {
        tracing::trace!("worker...");
        work(worker_ds, &worker_command).await;
    });

    Ok(())
}

pub async fn run() -> io::Result<()> {
    let addr = ("127.0.0.1", 34982);
    tracing::info!("starting server on port: {}", &addr.0);

    let ds = Arc::new(Datastore::new("memory").await.unwrap());
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
                        }
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
                        }
                        Err(err) => {
                            tracing::error!("stream error: {:?}", err);
                            return Err(());
                        }
                    }

                    tracing::trace!("completed reading input from cli");

                    let command: Command = from_bytes(&bytes).unwrap();
                    tracing::trace!("got command {:?}", command);

                    let response = match spawn_worker(ds, thread_pool, &command).await {
                        Ok(()) => Response::Ok,
                        Err(_) => Response::Error,
                    };

                    let mut output = Cursor::new(to_allocvec(&response).unwrap());
                    stream.write_buf(&mut output).await.unwrap();

                    Ok(())
                }
            })
            .map_err(|err| tracing::error!("service error: {:?}", err))
        })?
        .workers(1)
        .run()
        .await
}

pub async fn work(ds: Arc<Datastore>, command: &Command) {
    tracing::info!("starting worker on {:?}", &command);

    let result = match command {
        Command::ShutDown => {
            unimplemented!();
        }
        Command::FilesAdd(path) => add_path(ds, path),
    };

    match result.await {
        Ok(()) => {
            tracing::info!("worker completed");
        }
        Err(e) => {
            tracing::error!("worker failed with {}", e);
        }
    }
}

pub async fn add_path(ds: Arc<Datastore>, path: &Path) -> io::Result<()> {
    tracing::trace!("add_path processing ...");
    for entry in WalkDir::new(path).into_iter() {
        let entry = entry?;
        if !entry.file_type().is_file() {
            continue;
        }

        tracing::trace!("adding {:?}", entry);

        let mut tx = ds.transaction(true, false).await.unwrap();
        let command = Command::FilesAdd(entry.path().to_path_buf());
        let bytes = to_allocvec(&command).unwrap();
        tx.put(format!("jobs:{}", Ulid::new()), bytes)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        fetch_file(entry.path()).await?;
    }

    Ok(())
}

pub async fn fetch_file(path: &Path) -> io::Result<()> {
    tracing::trace!("fetch_file processing {}", path.display());

    // let status_code = Bucket::create(
    //     "blobs",
    //     Region::Custom {
    //         region: Region::EuWest2.to_string(),
    //         endpoint: "http://localhost:9000".to_owned(),
    //     },
    //     Credentials::new(Some("ziwa"), Some("ziwadevpass"), None, None, None).unwrap(),
    //     BucketConfiguration::default(),
    // ).await.unwrap();
    
    // tracing::trace!("status from bucket create {:?}", status_code.response_text);
    
    let bucket = Bucket::new(
        "blobs",
        Region::Custom {
            region: Region::EuWest2.to_string(),
            endpoint: "http://localhost:9000".to_owned(),
        },
        Credentials::new(Some("ziwa"), Some("ziwadevpass"), None, None, None).unwrap(),
    ).unwrap()
    .with_path_style();

    let mut file = File::open(path).await?;
    let status_code = bucket.put_object_stream(&mut file, path.to_str().unwrap()).await.unwrap();
    tracing::trace!("status from bucket write {:?}", status_code);

    Ok(())
}
