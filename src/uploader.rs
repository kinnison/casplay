//! Uploader type for REAPI operations
//!
use std::{
    cmp::min,
    future::Future,
    io::Cursor,
    path::Path,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
};

use async_stream::stream;
use bytes::BytesMut;
use prost::Message;
use sha256::{digest_bytes, digest_file};
use tokio::{fs, io::AsyncReadExt};
use tonic::{
    transport::{Channel, Endpoint, Error},
    Code, Status,
};

use crate::{
    build::bazel::remote::execution::v2::{
        batch_update_blobs_request, capabilities_client::CapabilitiesClient, compressor,
        content_addressable_storage_client::ContentAddressableStorageClient,
        BatchUpdateBlobsRequest, Digest, GetCapabilitiesRequest,
    },
    google::bytestream::{byte_stream_client::ByteStreamClient, WriteRequest},
};

pub type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;

const MAX_BATCH_ELEMENTS: usize = 500;
const MAX_REQUEST_SIZE: usize = (4 * 1024 * 1024) - 1024; // Max gRPC message size, minus a KiB for message content

pub struct Uploader {
    instance_name: String,
    uuid: String,
    cas: ContentAddressableStorageClient<Channel>,
    bs: ByteStreamClient<Channel>,
    caps: CapabilitiesClient<Channel>,
    blob_queue: Vec<(Digest, Vec<u8>)>,
    max_batch_size: usize,
    max_individual: usize,
}

impl Uploader {
    pub async fn new<D>(
        dst: D,
        instance_name: Option<&str>,
        uuid: Option<&str>,
    ) -> Result<Self, Error>
    where
        D: TryInto<Endpoint>,
        D::Error: Into<StdError>,
    {
        let endpoint = Endpoint::new(dst)?.connect().await?;

        let cas = ContentAddressableStorageClient::new(endpoint.clone());
        let bs = ByteStreamClient::new(endpoint.clone());

        let caps = CapabilitiesClient::new(endpoint);

        Ok(Self {
            instance_name: instance_name.unwrap_or("").to_string(),
            uuid: uuid
                .map(str::to_string)
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            cas,
            bs,
            caps,
            blob_queue: Vec::new(),
            max_batch_size: 0,
            max_individual: 0,
        })
    }

    pub async fn with<F, FR, R, E>(&mut self, func: F) -> Result<R, E>
    where
        F: FnOnce(&mut Uploader) -> FR,
        FR: Future<Output = Result<R, E>>,
        E: From<Status>,
    {
        self.check_batches().await?;

        let ret = func(self).await?;

        self.flush().await?;

        Ok(ret)
    }

    async fn check_batches(&mut self) -> Result<(), Status> {
        if self.max_batch_size != 0 {
            return Ok(());
        }
        let request = GetCapabilitiesRequest {
            instance_name: self.instance_name.clone(),
        };
        let caps = self.caps.get_capabilities(request).await?;
        if let Some(caps) = caps.into_inner().cache_capabilities {
            let max_batch = min(caps.max_batch_total_size_bytes, MAX_REQUEST_SIZE as i64) as usize;
            self.max_batch_size = max_batch;
            self.max_individual = max_batch >> 2; // 25% of max batch size
        } else {
            self.max_batch_size = MAX_REQUEST_SIZE >> 1; // Conservative half max request batch
            self.max_individual = MAX_REQUEST_SIZE >> 3; // 25% of conservative batch size
        }
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), Status> {
        if self.blob_queue.is_empty() {
            return Ok(());
        }

        // The blob queue is not empty, so let's build and send a batch update
        let mut request = BatchUpdateBlobsRequest {
            instance_name: self.instance_name.clone(),
            requests: Vec::new(),
        };

        for (digest, bytes) in std::mem::take(&mut self.blob_queue) {
            // Let's assert that the upload matches
            let hash = digest_bytes(&bytes);
            assert_eq!(digest.hash, hash);
            assert_eq!(digest.size_bytes, bytes.len() as i64);
            let inner_request = batch_update_blobs_request::Request {
                digest: Some(digest),
                data: bytes,
                compressor: compressor::Value::Identity.into(),
            };
            request.requests.push(inner_request);
        }

        println!(
            "Running a batch of {} entries totalling {} data bytes",
            request.requests.len(),
            request
                .requests
                .iter()
                .map(|r| r.data.len() as u64)
                .sum::<u64>()
        );

        let response = self.cas.batch_update_blobs(request).await?.into_inner();

        for response in response.responses {
            if let (Some(digest), Some(status)) = (response.digest, response.status) {
                let code = Code::from_i32(status.code);
                if code != Code::Ok {
                    // At least one blob upload failed, so fail out
                    println!("Digest: {}/{}", digest.hash, digest.size_bytes);
                    println!("Message: {}", status.message);
                    return Err(Status::new(code, "failed batch update blobs"));
                }
            }
        }

        Ok(())
    }

    fn count_queue(&self) -> usize {
        self.blob_queue
            .iter()
            .map(|(_, b)| b.len() + 128 /* overhead for the Digest */)
            .sum()
    }

    pub async fn queue_blob(&mut self, digest: Digest, data: Vec<u8>) -> Result<(), Status> {
        self.check_batches().await?;

        if data.len() > self.max_individual {
            return self.upload_blob(digest, data).await;
        }

        if self.blob_queue.len() == MAX_BATCH_ELEMENTS {
            self.flush().await?;
        }
        if self.count_queue() + data.len() > self.max_batch_size {
            self.flush().await?;
        }

        self.blob_queue.push((digest, data));

        Ok(())
    }

    pub async fn queue_message(&mut self, data: impl Message) -> Result<Digest, Status> {
        let data = data.encode_to_vec();
        let digest = Digest {
            hash: digest_bytes(&data),
            size_bytes: data.len() as i64,
        };
        self.queue_blob(digest.clone(), data).await?;
        Ok(digest)
    }

    fn format_resource(&self, digest: &Digest) -> String {
        if self.instance_name.is_empty() {
            format!(
                "uploads/{}/blobs/{}/{}",
                self.uuid, digest.hash, digest.size_bytes
            )
        } else {
            format!(
                "{}/uploads/{}/blobs/{}/{}",
                self.instance_name, self.uuid, digest.hash, digest.size_bytes
            )
        }
    }

    pub async fn upload_blob(&mut self, digest: Digest, data: Vec<u8>) -> Result<(), Status> {
        if data.len() > MAX_REQUEST_SIZE {
            let datalen = data.len();
            let data = Cursor::new(data);
            return self.upload_stream(digest, data, Some(datalen as i64)).await;
        }
        let data_len = data.len();
        let request = WriteRequest {
            resource_name: self.format_resource(&digest),
            write_offset: 0,
            finish_write: true,
            data,
        };

        let stream = stream! {
            yield request
        };

        let response = self.bs.write(stream).await?;

        if response.into_inner().committed_size != data_len as i64 {
            return Err(Status::new(Code::DataLoss, "upload didn't complete?"));
        }

        Ok(())
    }

    pub async fn upload_stream(
        &mut self,
        digest: Digest,
        data: impl AsyncReadExt + Send + Sync + 'static,
        size: Option<i64>,
    ) -> Result<(), Status> {
        let datalen = Arc::new(AtomicI64::new(0));

        let base_request = WriteRequest {
            resource_name: self.format_resource(&digest),
            write_offset: 0,
            finish_write: false,
            data: Vec::new(),
        };

        self.check_batches().await?;

        let mut data = Box::pin(data);

        let buf_max = min(
            self.max_batch_size,
            MAX_REQUEST_SIZE - 64 - base_request.resource_name.len(), /* overhead for WriteRequest */
        ) >> 1;

        let stream = {
            let datalen = Arc::clone(&datalen);
            stream! {
                let mut total_len = 0;
                let mut buffer = BytesMut::with_capacity(buf_max);
                if loop {
                    buffer.truncate(0);
                    let bytes = match data.read_buf(&mut buffer).await {
                        Ok(n) => n,
                        Err(_) => break false,
                    };
                    if bytes == 0 {
                        break true;
                    }
                    // Read in n bytes, yield up a buffer
                    let mut request = base_request.clone();
                    request.write_offset = total_len;
                    request.data = buffer[..].to_vec();
                    yield request;
                    total_len += bytes as i64;
                } {
                    // Successfully read/wrote the entire file, so yield a finish buffer
                    let mut request = base_request.clone();
                    request.write_offset = total_len as i64;
                    request.finish_write = true;
                    datalen.store(total_len as i64, Ordering::SeqCst);
                    yield request;
                }
            }
        };

        let response = self.bs.write(stream).await?;

        let expected = size.unwrap_or_else(|| datalen.load(Ordering::SeqCst));

        if response.into_inner().committed_size != expected {
            return Err(Status::new(Code::DataLoss, "upload didn't complete?"));
        }

        Ok(())
    }

    pub async fn upload_file(&mut self, fname: &Path) -> Result<Digest, Status> {
        let metadata = fs::metadata(fname).await?;
        let flen = metadata.len();
        if flen <= MAX_REQUEST_SIZE as u64 {
            // upload it in one go as a blob
            let mut data = BytesMut::with_capacity(flen as usize);
            let mut fh = fs::File::open(fname).await?;
            while data.len() < flen as usize {
                fh.read_buf(&mut data).await?;
            }
            drop(fh);
            let digest = Digest {
                hash: digest_bytes(&data[..]),
                size_bytes: flen as i64,
            };
            let data = data.freeze().to_vec();
            self.queue_blob(digest.clone(), data).await?;
            Ok(digest)
        } else {
            // We have to stream the file through the digester
            let fpath = fname.to_owned();
            let hash = tokio::task::spawn_blocking(move || digest_file(fpath))
                .await
                .map_err(|_| Status::new(Code::Unknown, "failed to digest input file"))??;
            let digest = Digest {
                hash,
                size_bytes: flen as i64,
            };
            self.upload_stream(
                digest.clone(),
                fs::File::open(fname).await?,
                Some(flen as i64),
            )
            .await?;
            Ok(digest)
        }
    }
}
