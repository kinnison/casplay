use std::{
    cmp::min,
    io::{ErrorKind, SeekFrom},
    path::{Path, PathBuf},
};

use sha256::digest_file;
use tokio::{
    fs::{self, File},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::mpsc,
};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{async_trait, Code, Status};

use crate::build::bazel::remote::execution::v2::Digest;

use super::{
    ReadSessionInstance, ReadSessionStream, Result, StorageBackend, WriteSession,
    WriteSessionInstance, WriteSessionStream,
};

// On disk storage backend

pub struct OnDiskStorage {
    base: PathBuf,
}

impl OnDiskStorage {
    pub fn instantiate(base: &Path) -> std::io::Result<Box<dyn StorageBackend>> {
        let base = base.to_path_buf();
        std::fs::create_dir_all(&base)?;
        drop(std::fs::File::create(base.join(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855-0",
        ))?);
        Ok(Box::new(Self { base }) as Box<dyn StorageBackend>)
    }

    fn path_for(&self, digest: &Digest) -> PathBuf {
        self.base
            .join(format!("{}-{}", digest.hash, digest.size_bytes))
    }

    fn temp_path_for(&self, digest: &Digest) -> PathBuf {
        self.base
            .join(format!("{}-{}.tmp", digest.hash, digest.size_bytes))
    }
}

fn map_io_error(err: std::io::Error) -> Status {
    match err.kind() {
        ErrorKind::NotFound => Status::new(Code::NotFound, "not found"),
        _ => Status::new(Code::Unknown, format!("unknown error: {:?}", err)),
    }
}

const BLOCK_SIZE: usize = 1024 * 1024;

struct OnDiskStorageWriter {
    file: Option<File>,
    size: u64,
    writing: PathBuf,
    target: PathBuf,
}

#[async_trait]
impl WriteSession for OnDiskStorageWriter {
    async fn write_block(&mut self, block: &[u8]) -> Result<()> {
        self.file
            .as_mut()
            .unwrap()
            .write_all(block)
            .await
            .map_err(map_io_error)?;
        self.size += block.len() as u64;

        Ok(())
    }

    async fn finish(&mut self) -> Result<Digest> {
        let mut file = self.file.take().unwrap();
        file.flush().await.map_err(map_io_error)?;
        drop(file);
        let written = Digest {
            hash: digest_file(&self.writing).map_err(map_io_error)?,
            size_bytes: self.size as i64,
        };
        fs::rename(&self.writing, &self.target)
            .await
            .map_err(map_io_error)?;
        Ok(written)
    }
}

#[async_trait]
impl StorageBackend for OnDiskStorage {
    async fn start_write(&self, digest: &Digest) -> Result<WriteSessionInstance> {
        let writing = self.temp_path_for(digest);
        let target = self.path_for(digest);
        let file = fs::File::create(&writing).await.map_err(map_io_error)?;
        Ok((Box::new(OnDiskStorageWriter {
            file: Some(file),
            size: 0,
            writing,
            target,
        }) as Box<WriteSessionStream>)
            .into())
    }

    async fn start_read(
        &self,
        digest: &Digest,
        offset: i64,
        mut limit: i64,
    ) -> Result<ReadSessionInstance> {
        let full_path = self.path_for(digest);
        let mut file = fs::File::open(full_path).await.map_err(map_io_error)?;

        let flen = file.metadata().await.map_err(map_io_error)?.len();

        if flen < (offset as u64) {
            return Err(Status::new(Code::InvalidArgument, "offset out of range"));
        }

        if limit == 0 {
            limit = flen as i64;
        }

        let to_send = min((flen as i64) - offset, limit) as usize;

        file.seek(SeekFrom::Start(offset as u64))
            .await
            .map_err(map_io_error)?;

        let (tx, rx) = mpsc::channel(4);

        tokio::spawn(async move {
            let mut sent = 0;
            while sent < to_send {
                let mut buffer = vec![0; BLOCK_SIZE];
                let read_size = match file.read(&mut buffer).await.map_err(map_io_error) {
                    Ok(size) => size,
                    Err(e) => {
                        tx.send(Err(e)).await.unwrap();
                        break;
                    }
                };
                buffer.resize(read_size, 0);
                tx.send(Ok(buffer)).await.unwrap();
                sent += read_size;
            }
        });

        Ok((Box::new(ReceiverStream::new(rx)) as Box<ReadSessionStream>).into())
    }

    async fn contains(&self, digest: &Digest) -> Result<bool> {
        let full_path = self.path_for(digest);
        match fs::metadata(full_path).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(false),
            Err(e) => Err(Status::new(
                Code::Unknown,
                format!("unknown error: {:?}", e),
            )),
        }
    }
}
