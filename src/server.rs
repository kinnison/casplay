//! CASPlay server

use std::{
    cmp::min,
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex, MutexGuard},
};

use lazy_static::lazy_static;
use regex::Regex;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{async_trait, transport::Server, Code, Request, Response, Status, Streaming};
use tracing::instrument;

use crate::{
    build::bazel::{
        remote::execution::v2::{
            capabilities_server::{Capabilities, CapabilitiesServer},
            digest_function, symlink_absolute_path_strategy, CacheCapabilities, Digest,
            GetCapabilitiesRequest, ServerCapabilities,
        },
        semver::SemVer,
    },
    google::bytestream::{
        byte_stream_server::{ByteStream, ByteStreamServer},
        QueryWriteStatusRequest, QueryWriteStatusResponse, ReadRequest, ReadResponse, WriteRequest,
        WriteResponse,
    },
};

pub async fn serve(dst: SocketAddr, instance_name: &str) -> anyhow::Result<()> {
    let server = Arc::new(Mutex::new(CASServer::new(instance_name)));
    Server::builder()
        .add_service(CapabilitiesServer::new(PlayCapabilities::new(Arc::clone(
            &server,
        ))))
        .add_service(ByteStreamServer::new(PlayByteStream::new(Arc::clone(
            &server,
        ))))
        .serve(dst)
        .await?;

    Ok(())
}

struct CASServer {
    instance_name: String,
    content: HashMap<Digest, Arc<[u8]>>,
}

impl CASServer {
    fn new(instance_name: &str) -> Self {
        Self {
            instance_name: instance_name.to_string(),
            content: HashMap::new(),
        }
    }
}

struct PlayCapabilities {
    inner: Arc<Mutex<CASServer>>,
}

impl PlayCapabilities {
    fn new(inner: Arc<Mutex<CASServer>>) -> Self {
        PlayCapabilities { inner }
    }
}

#[async_trait]
impl Capabilities for PlayCapabilities {
    async fn get_capabilities(
        &self,
        request: Request<GetCapabilitiesRequest>,
    ) -> Result<Response<ServerCapabilities>, Status> {
        if request.get_ref().instance_name
            == self
                .inner
                .lock()
                .map_err(|_| Status::new(Code::Unknown, "Mutex go boom"))?
                .instance_name
        {
            Ok(Response::new(ServerCapabilities {
                cache_capabilities: Some(CacheCapabilities {
                    digest_functions: vec![digest_function::Value::Sha256 as i32],
                    action_cache_update_capabilities: None,
                    cache_priority_capabilities: None,
                    max_batch_total_size_bytes: 4193280,
                    symlink_absolute_path_strategy:
                        symlink_absolute_path_strategy::Value::Disallowed as i32,
                    supported_compressors: vec![],
                    supported_batch_update_compressors: vec![],
                }),
                execution_capabilities: None,
                deprecated_api_version: None,
                low_api_version: Some(SemVer {
                    major: 2,
                    minor: 0,
                    patch: 0,
                    prerelease: "".into(),
                }),
                high_api_version: Some(SemVer {
                    major: 2,
                    minor: 2,
                    patch: 0,
                    prerelease: "".into(),
                }),
            }))
        } else {
            Err(Status::new(
                Code::InvalidArgument,
                format!("Unknown instance '{}'", request.get_ref().instance_name),
            ))
        }
    }
}

struct PlayByteStream {
    inner: Arc<Mutex<CASServer>>,
}

lazy_static! {
    static ref WRITE_RESOURCE_PARTS: Regex =
        Regex::new("^(.*?)/?uploads/[^/]+?/blobs/([a-f0-9]{64})/([0-9]+)").unwrap();
    static ref READ_RESOURCE_PARTS: Regex =
        Regex::new("^(.*?)/?blobs/([a-f0-9]{64})/([0-9]+)").unwrap();
}

impl PlayByteStream {
    fn new(inner: Arc<Mutex<CASServer>>) -> Self {
        Self { inner }
    }
    fn server(&self) -> Result<MutexGuard<CASServer>, Status> {
        self.inner
            .lock()
            .map_err(|_| Status::new(Code::Unknown, "Mutex poisoned?"))
    }

    fn extract_digest(&self, resource_name: &str, is_write: bool) -> Result<Digest, Status> {
        let resource_name = match resource_name.chars().next() {
            Some('/') => &resource_name[1..],
            _ => resource_name,
        };
        println!("Extracting digest from '{}'", resource_name);
        let parts = match if is_write {
            WRITE_RESOURCE_PARTS.captures(resource_name)
        } else {
            READ_RESOURCE_PARTS.captures(resource_name)
        } {
            Some(m) => m,
            None => return Err(Status::new(Code::InvalidArgument, "Bad resource name")),
        };

        let instance_name = parts.get(1).map(|m| m.as_str()).unwrap_or("");
        if self.server()?.instance_name != instance_name {
            return Err(Status::new(
                Code::InvalidArgument,
                format!("Unknown instance: '{}'", instance_name),
            ));
        }

        let hash = parts.get(2).unwrap().as_str();
        let size = parts.get(3).unwrap().as_str();
        let size = size.parse().unwrap();

        Ok(Digest {
            hash: hash.to_string(),
            size_bytes: size,
        })
    }
}

#[async_trait]
impl ByteStream for PlayByteStream {
    type ReadStream = ReceiverStream<Result<ReadResponse, Status>>;

    async fn query_write_status(
        &self,
        request: Request<QueryWriteStatusRequest>,
    ) -> Result<Response<QueryWriteStatusResponse>, Status> {
        todo!()
    }

    async fn write(
        &self,
        request: Request<Streaming<WriteRequest>>,
    ) -> Result<Response<WriteResponse>, Status> {
        let mut wstream = request.into_inner();
        let firstmsg = match wstream.message().await? {
            Some(msg) => msg,
            None => return Err(Status::new(Code::InvalidArgument, "No write messages?")),
        };

        let digest = self.extract_digest(&firstmsg.resource_name, true)?;

        println!("Receiving a digest: {:?}", digest);

        if digest.size_bytes > 512 * 1024 * 1024 {
            return Err(Status::new(
                Code::ResourceExhausted,
                "I cannot cope with data that big",
            ));
        }

        let mut data: Vec<u8> = Vec::with_capacity(digest.size_bytes as usize);

        data.extend_from_slice(&firstmsg.data);

        let mut finish_write = false;

        while let Some(msg) = wstream.message().await? {
            // Check the write_offset
            data.extend_from_slice(&msg.data);
            // Make sure the last chunk is a finish_write
            finish_write = msg.finish_write;
        }

        if !finish_write {
            return Err(Status::new(
                Code::Cancelled,
                "You stopped uploading, you fool!",
            ));
        }

        // At this point we have a Digest, and a Vec of bytes, let's put it into the server.

        println!("Inserting {}/{} into map", digest.hash, digest.size_bytes);

        let response = WriteResponse {
            committed_size: digest.size_bytes,
        };

        self.server()?.content.insert(digest, data.into());

        Ok(Response::new(response))
    }

    async fn read(
        &self,
        request: Request<ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let digest = self.extract_digest(&request.get_ref().resource_name, false)?;
        let offset = request.get_ref().read_offset as usize;
        let mut limit = request.get_ref().read_limit as usize;

        let data = match self.server()?.content.get(&digest).map(Arc::clone) {
            Some(data) => data,
            None => return Err(Status::new(Code::NotFound, "not found")),
        };

        if offset >= data.len() {
            return Err(Status::new(Code::InvalidArgument, "offset out of range"));
        }

        if limit == 0 {
            limit = data.len()
        }

        let to_send = min(data.len() - offset, limit);

        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            let mut pos = offset;
            let mut left = to_send;
            while left > 0 {
                let to_send = min(left, 1024);
                let response = ReadResponse {
                    data: data[pos..(pos + to_send)].to_vec(),
                };
                pos += to_send;
                left -= to_send;
                match tx.send(Ok(response)).await {
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
