//! Simple in-memory storage type

use std::{cmp::min, collections::HashMap, mem::take, sync::Arc};

use sha256::digest_bytes;
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{async_trait, Code, Status};

use crate::build::bazel::remote::execution::v2::Digest;

use super::{
    ReadSessionInstance, ReadSessionStream, Result, StorageBackend, StorageBackendInstance,
    WriteSession, WriteSessionInstance, WriteSessionStream,
};

pub struct MemoryStorage {
    content: Arc<Mutex<HashMap<Digest, Arc<[u8]>>>>,
}

impl MemoryStorage {
    pub fn instantiate() -> StorageBackendInstance {
        let empty_digest = Digest {
            hash: digest_bytes(&[]),
            size_bytes: 0,
        };
        let base_map = Some((empty_digest, vec![].into())).into_iter().collect();
        Box::new(Self {
            content: Arc::new(Mutex::new(base_map)),
        }) as StorageBackendInstance
    }
}

const BLOCK_SIZE: usize = 512 * 1024;

struct MemoryStorageWriter {
    size: usize,
    buffer: Vec<u8>,
    content: Arc<Mutex<HashMap<Digest, Arc<[u8]>>>>,
}

#[async_trait]
impl WriteSession for MemoryStorageWriter {
    async fn write_block(&mut self, block: &[u8]) -> Result<()> {
        self.buffer.extend_from_slice(block);
        if self.buffer.len() > self.size {
            Err(Status::new(Code::InvalidArgument, "too much data provided"))
        } else {
            Ok(())
        }
    }

    async fn finish(&mut self) -> Result<Digest> {
        if self.buffer.len() != self.size {
            Err(Status::new(
                Code::InvalidArgument,
                "insufficient data provided",
            ))
        } else {
            let new_digest = Digest {
                hash: digest_bytes(&self.buffer),
                size_bytes: self.buffer.len() as i64,
            };

            self.content
                .lock()
                .await
                .insert(new_digest.clone(), take(&mut self.buffer).into());
            Ok(new_digest)
        }
    }
}

#[async_trait]
impl StorageBackend for MemoryStorage {
    async fn make_copy(&self) -> Result<StorageBackendInstance> {
        Ok(Box::new(Self {
            content: Arc::clone(&self.content),
        }) as StorageBackendInstance)
    }

    async fn start_write(&self, digest: &Digest) -> Result<WriteSessionInstance> {
        let writer = MemoryStorageWriter {
            size: digest.size_bytes as usize,
            buffer: Vec::with_capacity(digest.size_bytes as usize),
            content: Arc::clone(&self.content),
        };
        Ok((Box::new(writer) as Box<WriteSessionStream>).into())
    }

    async fn start_read(
        &self,
        digest: &Digest,
        offset: i64,
        mut limit: i64,
    ) -> Result<ReadSessionInstance> {
        let data = match self.content.lock().await.get(digest).map(Arc::clone) {
            Some(data) => data,
            None => return Err(Status::new(Code::NotFound, "not found")),
        };
        if offset > data.len() as i64 {
            return Err(Status::new(Code::InvalidArgument, "offset out of range"));
        }

        if limit == 0 {
            limit = data.len() as i64
        }

        let to_send = min((data.len() as i64) - offset, limit);

        let (tx, rx) = mpsc::channel(4);

        let offset = offset as usize;
        let to_send = to_send as usize;

        tokio::spawn(async move {
            let mut sent = 0;
            while sent < to_send {
                let to_send = min(BLOCK_SIZE, to_send - sent);
                if (tx
                    .send(Ok(data[offset + sent..(offset + sent + to_send)].to_vec()))
                    .await)
                    .is_err()
                {
                    // Failed to send, we may as well give up because the stream is broken somehow so
                    // we can't signal that to the consumer anyway.
                    break;
                }
                sent += to_send;
            }
        });

        Ok((Box::new(ReceiverStream::new(rx)) as Box<ReadSessionStream>).into())
    }

    async fn contains(&self, digest: &Digest) -> Result<bool> {
        Ok(self.content.lock().await.contains_key(digest))
    }
}

#[cfg(test)]
mod test {
    use sha256::digest_bytes;
    use tokio_stream::StreamExt;

    use crate::build::bazel::remote::execution::v2::Digest;

    use super::super::Result;
    use super::MemoryStorage;

    #[tokio::test]
    async fn new_storage_has_empty_digest() -> Result<()> {
        let memory = MemoryStorage::instantiate();
        let empty_digest = Digest {
            hash: digest_bytes(&[]),
            size_bytes: 0,
        };
        assert!(memory.contains(&empty_digest).await?);
        Ok(())
    }

    #[tokio::test]
    async fn new_storage_doesnt_have_data() -> Result<()> {
        let memory = MemoryStorage::instantiate();
        let some_digest = Digest {
            hash: digest_bytes(b"hello"),
            size_bytes: 5,
        };
        assert!(!memory.contains(&some_digest).await?);
        Ok(())
    }

    #[tokio::test]
    async fn can_insert_data() -> Result<()> {
        let memory = MemoryStorage::instantiate();
        let some_digest = Digest {
            hash: digest_bytes(b"hello"),
            size_bytes: 5,
        };
        let mut writer = memory.start_write(&some_digest).await?;
        writer.write_block(b"hello").await?;
        let written = writer.finish().await?;
        assert_eq!(some_digest, written);
        assert!(memory.contains(&some_digest).await?);
        Ok(())
    }

    #[tokio::test]
    async fn can_retrieve_data() -> Result<()> {
        let memory = MemoryStorage::instantiate();
        let some_digest = Digest {
            hash: digest_bytes(b"hello"),
            size_bytes: 5,
        };
        let mut writer = memory.start_write(&some_digest).await?;
        writer.write_block(b"hello").await?;
        let written = writer.finish().await?;
        assert_eq!(some_digest, written);
        let mut reader = memory.start_read(&some_digest, 0, 0).await?;
        let mut accumulator = Vec::new();
        while let Some(data) = reader.next().await {
            let data = data?;
            accumulator.extend_from_slice(&data[..]);
        }
        assert_eq!(&accumulator, b"hello");
        Ok(())
    }
}
