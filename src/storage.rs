//! Storage for CAS Servers
//!

use std::pin::Pin;

use prost::Message;
use sha256::digest_bytes;
use tokio_stream::{Stream, StreamExt};
use tonic::{async_trait, Code, Status};

use crate::build::bazel::remote::execution::v2::Digest;

type Result<T, E = Status> = std::result::Result<T, E>;

#[async_trait]
pub trait WriteSession {
    async fn write_block(&mut self, block: &[u8]) -> Result<()>;

    async fn finish(&mut self) -> Result<Digest>;
}

type WriteSessionStream = dyn WriteSession + Unpin + Send + Sync;
type WriteSessionInstance = Pin<Box<WriteSessionStream>>;

type ReadSessionStream = dyn Stream<Item = Result<Vec<u8>>> + Send + Sync;
type ReadSessionInstance = Pin<Box<ReadSessionStream>>;

type BoxedIterator<T> = Box<dyn Iterator<Item = T> + Send + Sync>;

#[async_trait]
pub trait StorageBackend: Send + Sync {
    async fn make_copy(&self) -> Result<StorageBackendInstance>;
    async fn start_write(&self, digest: &Digest) -> Result<WriteSessionInstance>;

    async fn start_read(
        &self,
        digest: &Digest,
        offset: i64,
        limit: i64,
    ) -> Result<ReadSessionInstance>;

    async fn contains(&self, digest: &Digest) -> Result<bool>;

    // The following have default impls which are naive but work

    async fn read_blob(&self, digest: &Digest) -> Result<Vec<u8>> {
        let mut ret = Vec::new();
        let mut reader = self.start_read(digest, 0, 0).await?;

        while let Some(block) = reader.next().await {
            let data = block?;
            ret.extend_from_slice(&data[..]);
        }

        Ok(ret)
    }

    async fn write_blob(&self, digest: &Digest, data: &[u8]) -> Result<Digest> {
        let mut writer = self.start_write(digest).await?;
        writer.write_block(data).await?;
        let written = writer.finish().await?;
        if &written != digest {
            Err(Status::new(
                Code::InvalidArgument,
                "data does not match digest",
            ))
        } else {
            Ok(written)
        }
    }

    async fn batch_update_blobs(
        &self,
        blobs: BoxedIterator<(Digest, Vec<u8>)>,
    ) -> Result<Vec<(Digest, Status)>> {
        let mut ret = Vec::new();
        for (blob, data) in blobs {
            match self.start_write(&blob).await {
                Ok(mut writer) => match writer.write_block(&data).await {
                    Ok(_) => match writer.finish().await {
                        Ok(written) => {
                            if blob != written {
                                ret.push((
                                    blob,
                                    Status::new(Code::Internal, "digest inconsistency detected"),
                                ));
                            } else {
                                ret.push((blob, Status::new(Code::Ok, "")));
                            }
                        }
                        Err(e) => ret.push((blob, e)),
                    },
                    Err(e) => ret.push((blob, e)),
                },
                Err(e) => ret.push((blob, e)),
            }
        }
        Ok(ret)
    }

    async fn batch_read_blobs(
        &self,
        blobs: BoxedIterator<Digest>,
    ) -> Result<Vec<(Digest, Result<Vec<u8>>)>> {
        let mut ret = Vec::new();
        for blob in blobs {
            let mut reader = self.start_read(&blob, 0, 0).await?;
            let mut data = Vec::new();
            while let Some(block) = reader.next().await {
                let block = block?;
                data.extend_from_slice(&block[..])
            }
            ret.push((blob, Ok(data)))
        }
        Ok(ret)
    }

    async fn find_missing_blobs(&self, blobs: BoxedIterator<Digest>) -> Result<Vec<Digest>> {
        let mut ret = Vec::new();
        for blob in blobs {
            if !self.contains(&blob).await? {
                ret.push(blob);
            }
        }
        Ok(ret)
    }
}

#[async_trait]
pub trait StorageBackendExt {
    async fn get_message<M>(&self, digest: &Digest) -> Result<M>
    where
        M: Message + Default;
    async fn store_message<M>(&self, message: &M) -> Result<Digest>
    where
        M: Message;
}

pub type StorageBackendInstance = Box<dyn StorageBackend>;

pub mod disk;
pub mod memory;
pub mod remote;

#[async_trait]
impl StorageBackend for Box<dyn StorageBackend> {
    async fn make_copy(&self) -> Result<StorageBackendInstance> {
        self.as_ref().make_copy().await
    }
    async fn start_write(&self, digest: &Digest) -> Result<WriteSessionInstance> {
        self.as_ref().start_write(digest).await
    }

    async fn start_read(
        &self,
        digest: &Digest,
        offset: i64,
        limit: i64,
    ) -> Result<ReadSessionInstance> {
        self.as_ref().start_read(digest, offset, limit).await
    }

    async fn contains(&self, digest: &Digest) -> Result<bool> {
        self.as_ref().contains(digest).await
    }
}

#[async_trait]
impl StorageBackendExt for Box<dyn StorageBackend> {
    async fn get_message<M>(&self, digest: &Digest) -> Result<M>
    where
        M: Message + Default,
    {
        let body = self.read_blob(digest).await?;
        Message::decode(body.as_ref()).map_err(|_| {
            Status::internal(format!(
                "Unable to decode blob {}/{}",
                digest.hash, digest.size_bytes,
            ))
        })
    }

    async fn store_message<M>(&self, message: &M) -> Result<Digest>
    where
        M: Message,
    {
        let body = message.encode_to_vec();
        let digest = Digest {
            hash: digest_bytes(&body),
            size_bytes: body.len() as i64,
        };
        self.write_blob(&digest, &body).await
    }
}

#[cfg(test)]
mod test {
    use sha256::digest_bytes;

    use super::*;
    const MEGABYTE: usize = 1024 * 1024;

    #[tokio::test]
    async fn check_missing_blobs() -> Result<()> {
        let memory = memory::MemoryStorage::instantiate(MEGABYTE);
        let empty_digest = Digest {
            hash: digest_bytes(b""),
            size_bytes: 0,
        };
        let hello_digest = Digest {
            hash: digest_bytes(b"hello"),
            size_bytes: 5,
        };
        let both_digests = vec![empty_digest.clone(), hello_digest.clone()];
        let missing = memory
            .find_missing_blobs(Box::new(both_digests.into_iter()))
            .await?;

        assert_eq!(&missing, &[hello_digest]);
        Ok(())
    }

    #[tokio::test]
    async fn batch_update_blobs() -> Result<()> {
        let memory = memory::MemoryStorage::instantiate(MEGABYTE);
        let hello_digest = Digest {
            hash: digest_bytes(b"hello"),
            size_bytes: 5,
        };
        let upload = vec![(hello_digest.clone(), b"hello".to_vec())];
        let output = memory
            .batch_update_blobs(Box::new(upload.into_iter()))
            .await?;
        assert_eq!(output.len(), 1);
        for (digest, status) in output {
            assert_eq!(digest, hello_digest);
            assert_eq!(status.code(), Code::Ok);
        }

        Ok(())
    }

    #[tokio::test]
    async fn batch_read_blobs() -> Result<()> {
        let memory = memory::MemoryStorage::instantiate(MEGABYTE);
        let hello_digest = Digest {
            hash: digest_bytes(b"hello"),
            size_bytes: 5,
        };
        let mut writer = memory.start_write(&hello_digest).await?;
        writer.write_block(b"hello").await?;
        assert_eq!(writer.finish().await?, hello_digest);
        let wanted = vec![hello_digest.clone()];
        let output = memory
            .batch_read_blobs(Box::new(wanted.into_iter()))
            .await?;
        assert_eq!(output.len(), 1);
        for (digest, content) in output {
            assert_eq!(digest, hello_digest);
            assert!(content.is_ok());
            assert_eq!(&content.unwrap(), b"hello");
        }
        Ok(())
    }

    #[tokio::test]
    async fn read_blob() -> Result<()> {
        let memory = memory::MemoryStorage::instantiate(MEGABYTE);
        let hello_digest = Digest {
            hash: digest_bytes(b"hello"),
            size_bytes: 5,
        };
        let mut writer = memory.start_write(&hello_digest).await?;
        writer.write_block(b"hello").await?;
        assert_eq!(writer.finish().await?, hello_digest);
        let read_data = memory.read_blob(&hello_digest).await?;
        assert_eq!(&read_data, b"hello");
        Ok(())
    }

    #[tokio::test]
    async fn write_blob() -> Result<()> {
        let memory = memory::MemoryStorage::instantiate(MEGABYTE);
        let hello_digest = Digest {
            hash: digest_bytes(b"hello"),
            size_bytes: 5,
        };
        memory.write_blob(&hello_digest, b"hello").await?;
        let read_data = memory.read_blob(&hello_digest).await?;
        assert_eq!(&read_data, b"hello");
        Ok(())
    }
}
