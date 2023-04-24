use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use actix_rt::net::TcpStream;
use actix_server::Server;
use actix_service::{fn_service, ServiceFactoryExt as _};
use anyhow::Result;
use awscreds::Credentials;
use awsregion::Region;
use base64::{engine::general_purpose, Engine as _};
use futures_util::TryStreamExt;
use mime_sniffer::MimeTypeSniffer;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{stream_consumer::StreamConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;
use s3::Bucket;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha512};
use sqlx::migrate::Migrator;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::{types::Json, Row};
use time::{format_description::well_known::iso8601, OffsetDateTime};
use tokio::fs::File;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use walkdir::WalkDir;

use crate::rpc::{Command, Response};

async fn run_consumer(ds: Arc<PgPool>) {
    tracing::trace!("creating stream consumer config");
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "0.0.0.0:9092")
        .set("group.id", "dev")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Consumer creation failed");

    tracing::trace!("subscribing...");
    consumer
        .subscribe(&["jobs"])
        .expect("Can't subscribe to specified topic");

    tracing::trace!("starting event consumer");
    let stream_ds = &ds.clone();
    let stream_processor = consumer.stream().try_for_each(|message| async move {
        let task_ds = stream_ds.clone();
        let message = message.detach();
        let task = tokio::spawn(async move {
            let scope_ds = task_ds.clone();
            process_message(scope_ds, &message).await;
        });
        task.await.unwrap();
        Ok(())
    });

    stream_processor.await.expect("stream processing failed");
}

async fn manage_ds(ds: Arc<PgPool>) -> Result<()> {
    let migrator = Migrator::new(Path::new("./migrations")).await?;
    tracing::trace!("running migrations");
    migrator.run(&*ds).await?;
    tracing::trace!("migrations done");
    Ok(())
}

pub async fn run() -> Result<()> {
    tracing::trace!("connecting to surreal");

    let ds = Arc::new(
        PgPoolOptions::new()
            .max_connections(5)
            .connect("postgres://ziwadev:ziwadevpass@localhost/ziwadev")
            .await?,
    );

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "0.0.0.0:9092")
        .create()
        .expect("Consumer creation failed");

    producer
        .send(
            FutureRecord::to("jobs").key("startup").payload(&vec![]),
            Duration::from_secs(0),
        )
        .await
        .unwrap();

    manage_ds(ds.clone()).await?;
    tokio::spawn(run_consumer(ds));

    let addr = ("127.0.0.1", 34982);
    tracing::trace!("starting server on: {}:{}", &addr.0, &addr.1);
    Server::build()
        .bind("control", addr, move || {
            fn_service(move |mut stream: TcpStream| async move {
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

                let command: Command = bincode::deserialize(&bytes).unwrap();
                tracing::trace!("got command from cli {:?}", command);

                let producer: FutureProducer = ClientConfig::new()
                    .set("bootstrap.servers", "0.0.0.0:9092")
                    .create()
                    .expect("Consumer creation failed");

                producer
                    .send(
                        FutureRecord::to("jobs").key("some key").payload(&bytes),
                        Duration::from_secs(0),
                    )
                    .await
                    .unwrap();

                let response = Response::Ok;
                let mut output = Cursor::new(bincode::serialize(&response).unwrap());
                stream.write_buf(&mut output).await.unwrap();

                Ok(())
            })
            .map_err(|err| tracing::error!("service error: {:?}", err))
        })?
        .workers(1)
        .run()
        .await?;

    Ok(())
}

async fn process_message(ds: Arc<PgPool>, msg: &rdkafka::message::OwnedMessage) {
    if let Some(bytes) = msg.payload() {
        if bytes.is_empty() {
            return;
        }
        let command: Command = bincode::deserialize(bytes).unwrap();

        tracing::trace!("got command from event source {:?}", command);

        work(ds, &command).await;
    }
}

async fn work(ds: Arc<PgPool>, command: &Command) {
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
    id: i64,
    original_path: PathBuf,
    created: SystemTime,
    modified: SystemTime,
    size: u64,
    mime_type: Option<String>,
    digest: Option<String>,
}

async fn add_path(ds: Arc<PgPool>, path: &Path) -> Result<()> {
    tracing::trace!("add_path processing ...");
    for entry in WalkDir::new(path).into_iter() {
        let entry = entry?;
        if !entry.file_type().is_file() {
            continue;
        }

        tracing::trace!("adding {:?}", entry);

        let command = Command::FilesAdd(entry.path().to_path_buf());
        sqlx::query(
            r#"
INSERT INTO jobs ( job, data )
VALUES ( $1::job_type, $2 )
RETURNING id
        "#,
        )
        .bind(command.to_string())
        .bind(Json(&command))
        .fetch_one(&*ds)
        .await?;
        dbg!(&command);

        let metadata = fs::metadata(entry.path())?;
        let mut blob = BlobDescription {
            id: 0,
            original_path: entry.path().to_path_buf(),
            created: metadata.created()?,
            modified: metadata.modified()?,
            size: metadata.len(),
            mime_type: None,
            digest: None,
        };
        let rec = sqlx::query(
            r#"
INSERT INTO blobs ( data )
VALUES ( $1 )
RETURNING id
        "#,
        )
        .bind(Json(&blob))
        .fetch_one(&*ds)
        .await?;
        blob.id = rec.try_get("id")?;
        dbg!(&blob);

        fetch_file(ds.clone(), &mut blob).await?;
    }

    Ok(())
}

async fn fetch_file(ds: Arc<PgPool>, blob: &mut BlobDescription) -> Result<()> {
    tracing::trace!("fetch_file processing {}", blob.original_path.display());

    let bucket = Bucket::new(
        "ziwadev",
        Region::Custom {
            region: Region::EuWest2.to_string(),
            endpoint: "http://localhost:9000".to_owned(),
        },
        Credentials::new(Some("ziwa"), Some("ziwadevpass"), None, None, None)?,
    )?
    .with_path_style();

    let mut file = File::open(&blob.original_path).await?;

    let status_code = bucket
        .put_object_stream(&mut file, format!("blob:{}", blob.id))
        .await?;
    tracing::trace!("status from bucket write {:?}", status_code);

    determine_file_type(ds.clone(), blob).await?;

    Ok(())
}

fn fmttime<T>(time: T) -> String
where
    T: Into<OffsetDateTime>,
{
    time.into().format(&iso8601::Iso8601::DEFAULT).unwrap()
}

async fn determine_file_type(ds: Arc<PgPool>, blob: &mut BlobDescription) -> Result<()> {
    let metadata = fs::metadata(&blob.original_path)?;

    // Use local file, it is not modified
    let mut file = if metadata.created()? == blob.created && metadata.modified()? == blob.modified {
        tracing::trace!("local file unmodified, using original");
        File::open(&blob.original_path).await?
    } else {
        tracing::trace!(
            "local file modified {} != {} retrieving new copy from cold storage",
            fmttime(blob.modified),
            fmttime(metadata.modified()?),
        );

        let bucket = Bucket::new(
            "blobs",
            Region::Custom {
                region: Region::EuWest2.to_string(),
                endpoint: "http://localhost:9000".to_owned(),
            },
            Credentials::new(Some("ziwa"), Some("ziwadevpass"), None, None, None)?,
        )?
        .with_path_style();

        let mut file = File::create(format!("data/blobs/{}", blob.id)).await?;

        let response = bucket
            .get_object_to_writer(format!("blobs:{}", blob.id), &mut file)
            .await?;

        tracing::trace!("status from bucket write {:?}", response);

        // file.seek(SeekFrom::Start(0))?;
        file
    };

    let mut digest = Sha512::new();
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await?;
    let bytes = &buffer[..];
    digest.update(bytes);
    let mime_type = bytes.sniff_mime_type();
    tracing::debug!("determined mime type {:?}", mime_type);

    let hash: String = general_purpose::STANDARD_NO_PAD.encode(digest.finalize());
    tracing::debug!("file digest {:?}", hash);
    blob.digest = Some(hash);

    blob.mime_type = mime_type.map(|t| t.to_owned());

    sqlx::query(
        r#"
UPDATE blobs
SET DATA = $1
WHERE
    id = $2
        "#,
    )
    .bind(Json(&blob))
    .bind(blob.id)
    .execute(&*ds)
    .await?;

    dbg!(&blob);

    Ok(())
}
