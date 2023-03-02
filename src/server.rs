use std::fs;
use std::io::{self, Cursor, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use actix_rt::net::TcpStream;
use actix_rt::task::spawn_blocking;
use actix_server::Server;
use actix_service::{fn_service, ServiceFactoryExt as _};
use awscreds::Credentials;
use awsregion::Region;
use faktory::{ConsumerBuilder, Producer, Job};
use mime_sniffer::MimeTypeSniffer;
use postcard::{from_bytes, to_allocvec};
use rayon::{ThreadPool, ThreadPoolBuilder};
use s3::error::S3Error;
use s3::{Bucket, BucketConfiguration};
use serde::{Deserialize, Serialize};
use surrealdb::Datastore;
use time::{format_description::well_known::iso8601, OffsetDateTime};
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
    // let worker_ds = ds.clone();
    // let worker_command = command.clone();

    let mut p = Producer::connect(None).unwrap();
    let bytes = to_allocvec(&command).unwrap();
    p.enqueue(Job::new("jobs", vec![bytes])).unwrap();

    // let mut tx = ds.transaction(true, false).await.unwrap();
    // let bytes = to_allocvec(&command).unwrap();
    // tx.put(format!("jobs:{}", Ulid::new()), bytes)
    //     .await
    //     .unwrap();
    // tx.commit().await.unwrap();

    // tokio::spawn(async move {
    //     tracing::trace!("worker...");
    //     work(worker_ds, &worker_command).await;
    // });

    Ok(())
}

pub async fn run() -> io::Result<()> {
    let addr = ("127.0.0.1", 34982);
    tracing::info!("starting server on port: {}", &addr.0);

    let ds = Arc::new(Datastore::new("memory").await.unwrap());
    let thread_pool = Arc::new(Mutex::new(ThreadPoolBuilder::new().build().unwrap()));

    let mut c = ConsumerBuilder::default();
    let worker_ds = ds.clone();
    c.register("jobs", move |job| -> io::Result<()> {
        tracing::trace!("running job {:?}", job);

        let input: Vec<u8> = serde_json::to_vec(&job.args()[0]).unwrap();
        let command: Command = from_bytes(&input[..]).unwrap();

        tracing::trace!("command {:?}", command);
        work(worker_ds.clone(), &command);
        Ok(())
    });

    let mut c = c.connect(None).unwrap();

    spawn_blocking(move || {
        if let Err(e) = c.run(&["default"]) {
            tracing::error!("worker failed {:?}", e);
        }
        tracing::trace!("worker finished");
    });

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

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
struct BlobDescription {
    id: Ulid,
    original_path: PathBuf,
    created: SystemTime,
    modified: SystemTime,
    size: u64,
    mime_type: Option<String>,
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

        let metadata = fs::metadata(entry.path()).unwrap();
        let mut blob = BlobDescription {
            id: Ulid::new(),
            original_path: entry.path().to_path_buf(),
            created: metadata.created().unwrap(),
            modified: metadata.modified().unwrap(),
            size: metadata.len(),
            mime_type: None,
        };
        let bytes = to_allocvec(&blob).unwrap();
        tx.put(format!("blobs:{}", blob.id), bytes).await.unwrap();
        tx.commit().await.unwrap();

        fetch_file(ds.clone(), &mut blob).await?;
    }

    Ok(())
}

async fn fetch_file(ds: Arc<Datastore>, blob: &mut BlobDescription) -> io::Result<()> {
    tracing::trace!("fetch_file processing {}", blob.original_path.display());

    let bucket = Bucket::new(
        "blobs",
        Region::Custom {
            region: Region::EuWest2.to_string(),
            endpoint: "http://localhost:9000".to_owned(),
        },
        Credentials::new(Some("ziwa"), Some("ziwadevpass"), None, None, None).unwrap(),
    )
    .unwrap()
    .with_path_style();

    let mut file = File::open(&blob.original_path).await?;

    let status_code = bucket
        .put_object_stream(&mut file, format!("blobs:{}", blob.id))
        .await
        .unwrap();
    tracing::trace!("status from bucket write {:?}", status_code);

    determine_file_type(ds.clone(), blob).await.unwrap();

    Ok(())
}

fn fmttime<T>(time: T) -> String
where
    T: Into<OffsetDateTime>,
{
    time.into().format(&iso8601::Iso8601::DEFAULT).unwrap()
}

async fn determine_file_type(ds: Arc<Datastore>, blob: &mut BlobDescription) -> io::Result<()> {
    let metadata = fs::metadata(&blob.original_path).unwrap();

    // Use local file, it is not modified
    let mut file = if metadata.created().unwrap() == blob.created
        && metadata.modified().unwrap() == blob.modified
    {
        tracing::trace!("local file unmodified, using original");
        File::open(&blob.original_path).await?
    } else {
        tracing::trace!(
            "local file modified {} != {} retrieving new copy from cold storage",
            fmttime(blob.modified),
            fmttime(metadata.modified().unwrap()),
        );

        let bucket = Bucket::new(
            "blobs",
            Region::Custom {
                region: Region::EuWest2.to_string(),
                endpoint: "http://localhost:9000".to_owned(),
            },
            Credentials::new(Some("ziwa"), Some("ziwadevpass"), None, None, None).unwrap(),
        )
        .unwrap()
        .with_path_style();

        let mut file = File::create(format!("data/blobs/{}", blob.id))
            .await
            .unwrap();
        let status_code = bucket
            .get_object_stream(format!("blobs:{}", blob.id), &mut file)
            .await
            .unwrap();
        tracing::trace!("status from bucket write {:?}", status_code);
        // file.seek(SeekFrom::Start(0))?;
        file
    };

    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await.unwrap();
    let bytes = &buffer[..];
    let mime_type = bytes.sniff_mime_type().unwrap();
    dbg!(&mime_type);

    let mut tx = ds.transaction(true, false).await.unwrap();
    blob.mime_type = Some(mime_type.to_owned());
    let bytes = to_allocvec(&blob).unwrap();
    tx.put(format!("blobs:{}", blob.id), bytes).await.unwrap();
    tx.commit().await.unwrap();

    Ok(())
}
