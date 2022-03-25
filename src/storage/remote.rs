//! Simple in-memory storage type

use std::sync::Arc;

use async_stream::stream;

use tokio::{
    sync::{
        mpsc::{self, Sender},
        Mutex,
    },
    task::JoinHandle,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{async_trait, transport::Channel, Status};

use crate::{
    build::bazel::remote::execution::v2::{
        content_addressable_storage_client::ContentAddressableStorageClient, Digest,
        FindMissingBlobsRequest,
    },
    google::bytestream::{byte_stream_client::ByteStreamClient, ReadRequest, WriteRequest},
};

use super::{
    ReadSessionInstance, ReadSessionStream, Result, StorageBackend, StorageBackendInstance,
    WriteSession, WriteSessionInstance, WriteSessionStream,
};

pub struct RemoteStorage {
    instance_name: String,
    bs_client: Arc<Mutex<ByteStreamClient<Channel>>>,
    cas_client: Arc<Mutex<ContentAddressableStorageClient<Channel>>>,
}

impl RemoteStorage {
    pub async fn instantiate(
        instance_name: &str,
        client_base: &str,
    ) -> Result<StorageBackendInstance> {
        let bs_client = ByteStreamClient::connect(client_base.to_string())
            .await
            .map_err(|e| Status::internal(format!("bs connect failed: {:?}", e)))?;
        let cas_client = ContentAddressableStorageClient::connect(client_base.to_string())
            .await
            .map_err(|e| Status::internal(format!("bs connect failed: {:?}", e)))?;

        Ok(Box::new(Self {
            instance_name: instance_name.to_string(),
            bs_client: Arc::new(Mutex::new(bs_client)),
            cas_client: Arc::new(Mutex::new(cas_client)),
        }) as StorageBackendInstance)
    }
}

struct RemoteStorageWriter {
    writer: Option<Sender<Vec<u8>>>,
    writer_task: Option<JoinHandle<Result<Digest>>>,
}

#[async_trait]
impl WriteSession for RemoteStorageWriter {
    async fn write_block(&mut self, block: &[u8]) -> Result<()> {
        self.writer
            .as_ref()
            .unwrap()
            .send(block.to_vec())
            .await
            .map_err(|_| Status::internal("Unable to pass data to writer task"))
    }

    async fn finish(&mut self) -> Result<Digest> {
        drop(std::mem::take(&mut self.writer));
        self.writer_task
            .take()
            .unwrap()
            .await
            .map_err(|_| Status::internal("join error?"))?
    }
}

#[async_trait]
impl StorageBackend for RemoteStorage {
    async fn make_copy(&self) -> Result<StorageBackendInstance> {
        Ok(Box::new(Self {
            instance_name: self.instance_name.clone(),
            bs_client: Arc::clone(&self.bs_client),
            cas_client: Arc::clone(&self.cas_client),
        }) as StorageBackendInstance)
    }

    async fn start_write(&self, digest: &Digest) -> Result<WriteSessionInstance> {
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(4);
        let base_request = WriteRequest {
            resource_name: format!(
                "{}{}uploads/whateva/blobs/{}/{}",
                self.instance_name,
                if self.instance_name.is_empty() {
                    ""
                } else {
                    "/"
                },
                digest.hash,
                digest.size_bytes
            ),
            write_offset: 0,
            finish_write: false,
            data: vec![],
        };
        let client = Arc::clone(&self.bs_client);
        let digest = digest.clone();
        let mut total_bytes = 0;
        let writer_task = tokio::spawn(async move {
            let write_stream = stream! {
                while let Some(data) = rx.recv().await {
                    let mut request = base_request.clone();
                    request.write_offset = total_bytes;
                    total_bytes += data.len() as i64;
                    request.data = data;
                    yield request;
                }
                let mut request = base_request.clone();
                request.write_offset = total_bytes;
                request.finish_write = true;
                yield request;
            };
            let response = client.lock().await.write(write_stream).await?;
            let response = response.into_inner();
            if response.committed_size != digest.size_bytes {
                Err(Status::data_loss(format!(
                    "Committed {} bytes, expected {} bytes",
                    response.committed_size, digest.size_bytes
                )))
            } else {
                Ok(digest)
            }
        });
        let writer = RemoteStorageWriter {
            writer: Some(tx),
            writer_task: Some(writer_task),
        };
        Ok((Box::new(writer) as Box<WriteSessionStream>).into())
    }

    async fn start_read(
        &self,
        digest: &Digest,
        offset: i64,
        limit: i64,
    ) -> Result<ReadSessionInstance> {
        let request = ReadRequest {
            resource_name: format!(
                "{}{}blobs/{}/{}",
                self.instance_name,
                if self.instance_name.is_empty() {
                    ""
                } else {
                    "/"
                },
                digest.hash,
                digest.size_bytes
            ),
            read_offset: offset,
            read_limit: limit,
        };
        let mut response = self
            .bs_client
            .lock()
            .await
            .read(request)
            .await?
            .into_inner();

        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            loop {
                match response.message().await {
                    Ok(None) => break,
                    Ok(Some(rr)) => tx.send(Ok(rr.data)).await.unwrap(),
                    Err(e) => tx.send(Err(e)).await.unwrap(),
                }
            }
        });

        Ok((Box::new(ReceiverStream::new(rx)) as Box<ReadSessionStream>).into())
    }

    async fn contains(&self, digest: &Digest) -> Result<bool> {
        let request = FindMissingBlobsRequest {
            instance_name: self.instance_name.clone(),
            blob_digests: vec![digest.clone()],
        };
        let response = self
            .cas_client
            .lock()
            .await
            .find_missing_blobs(request)
            .await?
            .into_inner();
        Ok(response.missing_blob_digests.is_empty())
    }
}
