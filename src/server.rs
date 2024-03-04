use std::collections::HashMap;
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use actix_rt::net::TcpStream;
use actix_server::Server;
use actix_service::{fn_service, ServiceFactoryExt as _};
use anyhow::Result;
use async_trait::async_trait;
use async_zip::tokio::read::seek::ZipFileReader;
use awscreds::Credentials;
use awsregion::Region;
use base64::{engine::general_purpose, Engine as _};
use epub::doc::EpubDoc;
use mime_sniffer::MimeTypeSniffer;
use pdf::file::FileOptions;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{stream_consumer::StreamConsumer, CommitMode, Consumer};
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
use ulid::Ulid;
use walkdir::WalkDir;

use crate::rpc::{Command, CommandName, Response};

async fn run_consumer(ctx: Arc<Mutex<Context>>) {
    let topic = "ziwa.jobs";
    let group_id = "ziwa.jobs";

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "0.0.0.0:9092")
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[topic])
        .expect("Can't subscribe to specified topic");

    let mut shutdown = false;
    while !shutdown {
        match consumer.recv().await {
            Err(e) => {
                tracing::warn!("Kafka error: {}", e);
            }
            Ok(message) => {
                let message = message.detach();
                shutdown = process_message(ctx.clone(), &message).await;
                consumer.commit_consumer_state(CommitMode::Async).unwrap();
            }
        }
    }
}

async fn manage_ds(ds: Arc<PgPool>) -> Result<()> {
    let migrator = Migrator::new(Path::new("./migrations")).await?;
    migrator.run(&*ds).await?;
    Ok(())
}

struct Context {
    queue: Arc<FutureProducer>,
    store: Arc<PgPool>,
    registry: Arc<Registry>,
}

pub async fn run() -> Result<()> {
    tracing::trace!("connecting to surreal");

    let store = Arc::new(
        PgPoolOptions::new()
            .max_connections(5)
            .connect("postgres://ziwadev:ziwadevpass@localhost/ziwadev")
            .await?,
    );

    let queue: Arc<FutureProducer> = Arc::new(
        ClientConfig::new()
            .set("bootstrap.servers", "0.0.0.0:9092")
            .create()
            .expect("Consumer creation failed"),
    );
    queue
        .send(
            FutureRecord::to("ziwa.jobs")
                .key("startup")
                .payload(&vec![]),
            Duration::from_secs(0),
        )
        .await
        .unwrap();

    // let mut files_add_worker = Arc::new(FilesAddWorker{});
    // let processor = files_add_worker.setup();

    let registry = Arc::new(Registry {
        workers: HashMap::default(),
    });
    //workers: HashMap::from([
    //     ("add_file".to_string(), files_add_worker),
    // ])
    // }
    // ));
    // registry.workers.insert("add_file".to_string(), files_add_worker);

    let context = Arc::new(Mutex::new(Context {
        store,
        queue,
        registry,
    }));

    manage_ds(context.lock().unwrap().store.clone()).await?;
    tokio::spawn(run_consumer(context.clone()));

    let addr = ("127.0.0.1", 34982);
    tracing::trace!("starting server on: {}:{}", &addr.0, &addr.1);
    Server::build()
        .bind("control", addr, move || {
            let queue = context.queue.clone();
            fn_service(move |mut stream: TcpStream| {
                let queue = queue.clone();

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

                    let command: Command = bincode::deserialize(&bytes).unwrap();
                    tracing::trace!("got command from cli {:?}", command);

                    queue_job(queue, &command).await.unwrap();

                    let response = Response::Ok;
                    let mut output = Cursor::new(bincode::serialize(&response).unwrap());
                    stream.write_buf(&mut output).await.unwrap();

                    Ok(())
                }
            })
            .map_err(|err| tracing::error!("service error: {:?}", err))
        })?
        .workers(1)
        .run()
        .await?;

    Ok(())
}

async fn queue_job(queue: Arc<FutureProducer>, job: &Command) -> Result<()> {
    let bytes = bincode::serialize(&job)?;

    tracing::trace!("queuing new job {:#?}", &job);
    let key = Ulid::new().to_string();
    queue
        .send(
            FutureRecord::to("ziwa.jobs").key(&key).payload(&bytes),
            Duration::from_secs(0),
        )
        .await
        .unwrap();

    Ok(())
}

async fn process_message(ctx: Arc<Mutex<Context>>, msg: &rdkafka::message::OwnedMessage) -> bool {
    if let Some(bytes) = msg.payload() {
        if bytes.is_empty() {
            return false;
        }
        let command: Command = bincode::deserialize(bytes).unwrap();

        tracing::trace!("got command from event source {:?}", command);

        return work(ctx, &command).await;
    }

    false
}

async fn work(ctx: Arc<Mutex<Context>>, command: &Command) -> bool {
    tracing::info!("starting worker on {:?}", &command);

    if let Some(worker) = ctx
        .lock()
        .unwrap()
        .registry
        .workers
        .get(&command.name.to_string())
    {
    } else {
        unimplemented!();
    }

    // let result = match command {
    //     Command::ShutDown => return true,
    //     Command::Reset => reset(ctx.store.clone()).await,
    //     Command::ExtractExif(blob) => {
    //         FetchFileWorker{
    //             ctx: WorkerCtx{
    //                 ds: ctx.store.clone(),
    //                 queue: ctx.queue.clone(),
    //                 params: HashMap::from([("blob", ParamValue::BlobId(*blob))]),
    //             },
    //         }.run().await
    //     }
    //     Command::FetchFile(blob) => {
    //         FetchFileWorker{
    //             ctx: WorkerCtx{
    //                 ds: ctx.store.clone(),
    //                 queue: ctx.queue.clone(),
    //                 params: HashMap::from([("blob", ParamValue::BlobId(*blob))]),
    //             },
    //         }.run().await
    //     }
    //     Command::SniffMimeType(blob) => {
    //         SniffMimeTypeWorker{
    //             ctx: WorkerCtx{
    //                 ds: ctx.store.clone(),
    //                 queue: ctx.queue.clone(),
    //                 params: HashMap::from([("blob", ParamValue::BlobId(*blob))]),
    //             },
    //         }.run().await
    //     }
    //     Command::FilesAdd(path) => {
    //         FilesAddWorker{
    //             ctx: WorkerCtx{
    //                 ds: ctx.store.clone(),
    //                 queue: ctx.queue.clone(),
    //                 params: HashMap::from([("path", ParamValue::Path(path))]),
    //             },
    //         }.run().await
    //     }
    //     Command::SummarizePdf(ulid) => summarize_pdf(ctx.store.clone(), ulid).await,
    //     Command::SummarizeEpub(ulid) => summarize_epub(ctx.store.clone(), ulid).await,
    //     Command::ExtractZip(ulid) => extract_zip(ctx.store.clone(), ulid).await,
    //     Command::Docker => unimplemented!(),
    //     Command::Wasm => unimplemented!(),
    // };

    // match result {
    //     Ok(()) => {
    //         tracing::info!("worker completed");
    //     }
    //     Err(e) => {
    //         tracing::error!("worker failed with {}", e);
    //     }
    // }

    false
}

enum ParamType {
    Path,
    BlobId,
    Blob,
}

enum ParamValue {
    Path(PathBuf),
    BlobId(Ulid),
    Blob(BlobDescription),
}

#[async_trait]
pub trait Worker {
    fn setup(&mut self) -> Result<()> {
        Ok(())
    }

    async fn run(&mut self) -> Result<()>;
}

pub struct WorkerCtx {
    ds: Arc<PgPool>,
    queue: Arc<FutureProducer>,
    params: HashMap<String, ParamValue>,
}

impl WorkerCtx {
    pub async fn enqueue(&self, job: &Command) -> Result<()> {
        queue_job(self.queue.clone(), &job).await
    }
}

pub struct Registry {
    workers: HashMap<String, Arc<Mutex<dyn Worker>>>,
}

#[derive(Default)]
struct Processor {
    inputs: HashMap<String, ParamType>,
    outputs: HashMap<String, ParamType>,
}

pub struct FilesAddWorker {
    ctx: WorkerCtx,
}

async fn blob_id_to_blob_description(ctx: &WorkerCtx, name: &str) -> Result<BlobDescription> {
    if let Some(ParamValue::BlobId(blob_id)) = ctx.params.get(name) {
        let ds = &*ctx.ds;
        let row = sqlx::query(r#"SELECT * FROM blobs WHERE ulid = $1"#)
            .bind(blob_id.to_string())
            .fetch_one(&*ds)
            .await?;

        let blob_json: serde_json::Value = row.try_get("data")?;
        let mut blob: BlobDescription = serde_json::from_value(blob_json)?;
        blob.ulid = Ulid::from_string(row.try_get("ulid")?)?;
        blob.id = row.try_get("id")?;
        Ok(blob)
    } else {
        unimplemented!();
    }
}

#[async_trait]
impl Worker for FilesAddWorker {
    fn setup(&mut self) -> Result<Processor> {
        let registry = Processor::default();
        registry.inputs.insert("path".to_string(), ParamType::Path);
        registry.outputs.insert("file".to_string(), ParamType::File);
        Ok(registry)
    }

    async fn run(&mut self) -> Result<()> {
        let path = match self.ctx.params.get("path") {
            Some(ParamValue::Path(p)) => p,
            _ => unimplemented!(),
        };

        tracing::trace!("add_path processing {:?} ...", path);

        let metadata = fs::metadata(path)?;
        if metadata.is_file() {
            let mut blob = BlobDescription {
                id: 0,
                ulid: Ulid::new(),
                original_path: path.to_path_buf(),
                created: metadata.created()?,
                modified: metadata.modified()?,
                size: metadata.len(),
                mime_type: None,
                digest: None,
            };
            let rec = sqlx::query(concat!(
                "INSERT INTO blobs ( ulid, data ) ",
                "VALUES ( $1, $2 ) ",
                "RETURNING id"
            ))
            .bind(blob.ulid.to_string())
            .bind(Json(&blob))
            .fetch_one(&*self.ctx.ds)
            .await?;
            blob.id = rec.try_get("id")?;

            let command = Command {
                name: CommandName::FetchFile,
                inputs: HashMap::from([("id".to_string(), blob.ulid.to_string())]),
            };
            self.ctx.enqueue(&command).await?;

            return Ok(());
        }

        for entry in WalkDir::new(path).min_depth(1).max_depth(1).into_iter() {
            let entry = entry?;
            tracing::trace!("adding {:?}", entry);
            let command = Command {
                name: CommandName::FilesAdd,
                inputs: HashMap::from([(
                    "path".to_string(),
                    entry.path().to_str().unwrap().to_owned(),
                )]),
            };
            queue_job(self.ctx.queue.clone(), &command).await?;
        }

        Ok(())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
struct BlobDescription {
    id: i64,
    ulid: Ulid,
    original_path: PathBuf,
    created: SystemTime,
    modified: SystemTime,
    size: u64,
    mime_type: Option<String>,
    digest: Option<String>,
}

impl std::fmt::Display for BlobDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.original_path)
    }
}

#[tracing::instrument(skip(ds))]
async fn reset(ds: Arc<PgPool>) -> Result<()> {
    sqlx::query("TRUNCATE blobs;").execute(&*ds).await?;

    Ok(())
}

pub struct FetchFileWorker {
    ctx: WorkerCtx,
}

#[async_trait]
impl Worker for FetchFileWorker {
    async fn run(&mut self) -> Result<()> {
        let blob = match self.ctx.params.get("blob") {
            Some(ParamValue::Blob(b)) => b,
            _ => unimplemented!(),
        };

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

        let key = blob.original_path.display().to_string();
        let status_code = bucket.put_object_stream(&mut file, key).await?;
        tracing::trace!("status from bucket write {:?}", status_code);

        let command = Command{
            name: CommandName::SniffMimeType,
            inputs: HashMap::from([(
                "id".to_string(),
                blob.ulid.to_string(),
            )]),
        };
        self.ctx.enqueue(&command).await?;

        Ok(())
    }
}

fn fmttime<T>(time: T) -> String
where
    T: Into<OffsetDateTime>,
{
    time.into().format(&iso8601::Iso8601::DEFAULT).unwrap()
}

async fn concentre_blob(blob: &BlobDescription) -> Result<File> {
    let metadata = fs::metadata(&blob.original_path)?;

    // Use local file, it is not modified
    if metadata.created()? == blob.created && metadata.modified()? == blob.modified {
        tracing::trace!("local file unmodified, using original");
        Ok(File::open(&blob.original_path).await?)
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
        Ok(file)
    }
}

pub struct SniffMimeTypeWorker {
    ctx: WorkerCtx,
}

#[async_trait]
impl Worker for SniffMimeTypeWorker {
    async fn run(&mut self) -> Result<()> {
        let blob = match self.ctx.params.get_mut("blob") {
            Some(ParamValue::Blob(b)) => b,
            _ => unimplemented!(),
        };

        let mut file = concentre_blob(blob).await?;
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

        sqlx::query("UPDATE blobs SET DATA = $1 WHERE id = $2")
            .bind(Json(&blob))
            .bind(blob.id)
            .execute(&*self.ctx.ds)
            .await?;

        match &blob.mime_type {
            Some(s) => match s.as_ref() {
                "application/pdf" => {
                    let command = Command{
                        name: CommandName::SummarizePdf,
                        inputs: HashMap::from([("id".to_string(), blob.ulid.to_string())]),
                    };
                    self.ctx.enqueue(&command).await?;
                }
                // FIXME: mime type sniffer won't generate this
                // "application/epub+zip" => {
                //     let command = Command::SummarizeEpub(blob.ulid);
                //     self.ctx.enqueue(&command);
                // }
                "application/zip" => {
                    let command = Command{
                        name: CommandName::ExtractZip,
                        inputs: HashMap::from([("id".to_string(), blob.ulid.to_string())]),
                    };
                    self.ctx.enqueue(&command).await?;
                }
                "image/jpeg" => {
                    let command = Command{
                        name: CommandName::ExtractExif,
                        inputs: HashMap::from([("id".to_string(), blob.ulid.to_string())]),
                    };
                    self.ctx.enqueue(&command).await?;
                }
                other => tracing::warn!("unrecognized file type {}", other),
            },
            None => {}
        }

        Ok(())
    }
}

#[tracing::instrument(skip(ds))]
async fn retrieve_blob(ds: Arc<PgPool>, blob_id: &Ulid) -> Result<BlobDescription> {
    let row = sqlx::query(r#"SELECT * FROM blobs WHERE ulid = $1"#)
        .bind(blob_id.to_string())
        .fetch_one(&*ds)
        .await?;

    let blob_json: serde_json::Value = row.try_get("data")?;
    let mut blob: BlobDescription = serde_json::from_value(blob_json)?;
    blob.ulid = Ulid::from_string(row.try_get("ulid")?)?;
    blob.id = row.try_get("id")?;
    Ok(blob)
}

#[tracing::instrument(skip(ds))]
async fn extract_exif(ds: Arc<PgPool>, blob_id: &Ulid) -> Result<()> {
    let blob = retrieve_blob(ds, blob_id).await?;
    let mut file = concentre_blob(&blob).await?;

    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await?;

    let exifreader = exif::Reader::new();
    let mut reader = Cursor::new(&mut buffer);
    if let Ok(exif) = exifreader.read_from_container(&mut reader) {
        for f in exif.fields() {
            tracing::trace!(
                "exif field {} {} {}",
                f.tag,
                f.ifd_num,
                f.display_value().with_unit(&exif)
            );
        }
    }

    Ok(())
}

#[tracing::instrument(skip(ds))]
async fn extract_zip(ds: Arc<PgPool>, blob_id: &Ulid) -> Result<()> {
    let blob = retrieve_blob(ds, blob_id).await?;
    let mut file = concentre_blob(&blob).await?;

    let mut zip = ZipFileReader::with_tokio(&mut file).await?;

    let entries = zip.file().entries().len();
    for index in 0..entries {
        let reader = zip.reader_without_entry(index);
        eprintln!("entry = {:?}", index);
    }

    Ok(())
}

#[tracing::instrument(skip(ds))]
async fn summarize_pdf(ds: Arc<PgPool>, blob_id: &Ulid) -> Result<()> {
    let blob = retrieve_blob(ds, blob_id).await?;
    let mut file = concentre_blob(&blob).await?;

    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await?;

    let pdf = FileOptions::cached().load(buffer.as_slice())?;

    if let Some(ref info) = pdf.trailer.info_dict {
        let title = info.get("Title").and_then(|p| p.to_string_lossy().ok());
        let author = info.get("Author").and_then(|p| p.to_string_lossy().ok());
        tracing::trace!("pdf {} has title: {:?} author {:?}", blob, title, author);
    }

    for page in pdf.pages() {
        let page = page?;
        if let Some(ref c) = page.contents {
            tracing::trace!("{:?}", c);
        }
    }

    Ok(())
}

#[tracing::instrument(skip(ds))]
async fn summarize_epub(ds: Arc<PgPool>, blob_id: &Ulid) -> Result<()> {
    let blob = retrieve_blob(ds, blob_id).await?;
    let mut file = concentre_blob(&blob).await?;

    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await?;

    let cursor = Cursor::new(buffer);

    let doc = EpubDoc::from_reader(cursor)?;

    let title = doc.metadata.get("title");
    tracing::trace!("epub title {:?}", title);
    Ok(())
}
