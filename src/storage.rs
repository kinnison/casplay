//! Storage for CAS Servers
//!

use std::pin::Pin;

use tokio_stream::{Stream, StreamExt};
use tonic::{async_trait, Code, Status};

use crate::build::bazel::remote::execution::v2::Digest;

type Result<T, E = Status> = std::result::Result<T, E>;

#[async_trait]
pub trait WriteSession {
    async fn write_block(&mut self, block: Vec<u8>) -> Result<()>;

    async fn finish(&mut self) -> Result<Digest>;
}

type WriteSessionStream = dyn WriteSession + Unpin + Send + Sync;
type WriteSessionInstance = Pin<Box<WriteSessionStream>>;

type ReadSessionStream = dyn Stream<Item = Result<Vec<u8>>> + Send + Sync;
type ReadSessionInstance = Pin<Box<ReadSessionStream>>;

type BoxedIterator<T> = Box<dyn Iterator<Item = T> + Send + Sync>;

#[async_trait]
pub trait StorageBackend: Send + Sync {
    async fn start_write(&self, digest: &Digest) -> Result<WriteSessionInstance>;

    async fn start_read(&self, digest: &Digest) -> Result<Option<ReadSessionInstance>>;

    async fn contains(&self, digest: &Digest) -> Result<bool>;

    async fn batch_update_blobs(
        &self,
        blobs: BoxedIterator<(Digest, Vec<u8>)>,
    ) -> Result<Vec<(Digest, Status)>> {
        let mut ret = Vec::new();
        for (blob, data) in blobs {
            match self.start_write(&blob).await {
                Ok(mut writer) => match writer.write_block(data).await {
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
            match self.start_read(&blob).await? {
                Some(mut reader) => {
                    let mut data = Vec::new();
                    while let Some(block) = reader.next().await {
                        let block = block?;
                        data.extend_from_slice(&block[..])
                    }
                    ret.push((blob, Ok(data)))
                }
                None => ret.push((blob, Err(Status::new(Code::NotFound, "not found")))),
            }
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

pub type StorageBackendInstance = Box<dyn StorageBackend>;

pub mod memory;

#[cfg(test)]
mod test {
    use sha256::digest_bytes;

    use super::*;

    #[tokio::test]
    async fn check_missing_blobs() -> Result<()> {
        let memory = memory::MemoryStorage::instantiate();
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
        let memory = memory::MemoryStorage::instantiate();
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
        let memory = memory::MemoryStorage::instantiate();
        let hello_digest = Digest {
            hash: digest_bytes(b"hello"),
            size_bytes: 5,
        };
        let mut writer = memory.start_write(&hello_digest).await?;
        writer.write_block(b"hello".to_vec()).await?;
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
}
