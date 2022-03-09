//! CASPlay server

use std::{
    cmp::min,
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::{Arc, Mutex, MutexGuard},
};

use lazy_static::lazy_static;
use regex::Regex;

use prost::Message;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    async_trait,
    codegen::http::{self, HeaderMap},
    metadata::MetadataMap,
    service::interceptor,
    transport::Server,
    Code, Request, Response, Status, Streaming,
};
use tracing::{info, span, trace, Level, Span};

use crate::{
    build::bazel::{
        remote::execution::v2::{
            batch_update_blobs_response,
            capabilities_server::{Capabilities, CapabilitiesServer},
            content_addressable_storage_server::{
                ContentAddressableStorage, ContentAddressableStorageServer,
            },
            digest_function, symlink_absolute_path_strategy, BatchReadBlobsRequest,
            BatchReadBlobsResponse, BatchUpdateBlobsRequest, BatchUpdateBlobsResponse,
            CacheCapabilities, Digest, Directory, FindMissingBlobsRequest,
            FindMissingBlobsResponse, GetCapabilitiesRequest, GetTreeRequest, GetTreeResponse,
            RequestMetadata, ServerCapabilities, ToolDetails,
        },
        semver::SemVer,
    },
    google::{
        bytestream::{
            byte_stream_server::{ByteStream, ByteStreamServer},
            QueryWriteStatusRequest, QueryWriteStatusResponse, ReadRequest, ReadResponse,
            WriteRequest, WriteResponse,
        },
        rpc,
    },
};

pub async fn serve(dst: SocketAddr, instance_name: &str) -> anyhow::Result<()> {
    let server = Arc::new(Mutex::new(CASServer::new(instance_name)));
    Server::builder()
        .trace_fn(tracing_span)
        .layer(interceptor(show_metadata))
        .add_service(CapabilitiesServer::new(PlayCapabilities::new(Arc::clone(
            &server,
        ))))
        .add_service(ByteStreamServer::new(PlayByteStream::new(Arc::clone(
            &server,
        ))))
        .add_service(ContentAddressableStorageServer::new(PlayCASServer::new(
            Arc::clone(&server),
        )))
        .serve(dst)
        .await?;

    Ok(())
}

trait HeaderGet {
    fn get(&self, key: &str) -> Option<&[u8]>;
}

impl HeaderGet for HeaderMap {
    fn get(&self, key: &str) -> Option<&[u8]> {
        self.get(key).map(|v| v.as_bytes())
    }
}

impl HeaderGet for MetadataMap {
    fn get(&self, key: &str) -> Option<&[u8]> {
        self.get_bin(key).map(|v| v.as_ref())
    }
}

fn extract_metadata(headers: &impl HeaderGet) -> Option<RequestMetadata> {
    headers
        .get("build.bazel.remote.execution.v2.requestmetadata-bin")
        .and_then(|v| base64::decode(v).ok())
        .and_then(|v| RequestMetadata::decode(v.as_ref()).ok())
}

fn tracing_span(req: &http::Request<()>) -> Span {
    if let Some(metadata) = extract_metadata(req.headers()) {
        let metadata: RequestMetadata = metadata;
        let tool = metadata
            .tool_details
            .as_ref()
            .map(|tool_details| format!("{}/{}", tool_details.tool_name, tool_details.tool_version))
            .unwrap_or_else(|| "unknown".into());
        let invocation = metadata.tool_invocation_id;
        span!(Level::INFO, "", tool = ?tool, invocation = ?invocation)
    } else {
        Span::none()
    }
}
fn show_metadata(mut req: Request<()>) -> Result<Request<()>, Status> {
    if let Some(metadata) = extract_metadata(req.metadata()) {
        req.extensions_mut().insert(Arc::new(metadata));
    } else {
        let metadata = RequestMetadata {
            tool_details: Some(ToolDetails {
                tool_name: "unknown".into(),
                tool_version: "0.0.0".into(),
            }),
            action_id: "".into(),
            tool_invocation_id: "unknown".into(),
            correlated_invocations_id: "unknown".into(),
            action_mnemonic: "unknown".into(),
            target_id: "unknown".into(),
            configuration_id: "unknown".into(),
        };
        req.extensions_mut().insert(Arc::new(metadata));
    }
    Ok(req)
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

fn get_metadata<T>(req: &Request<T>) -> Arc<RequestMetadata> {
    req.extensions()
        .get::<Arc<RequestMetadata>>()
        .map(Arc::clone)
        .unwrap()
}

#[async_trait]
impl Capabilities for PlayCapabilities {
    async fn get_capabilities(
        &self,
        request: Request<GetCapabilitiesRequest>,
    ) -> Result<Response<ServerCapabilities>, Status> {
        let metadata = get_metadata(&request);
        info!(
            "Handling capabilities request for invocation {}",
            metadata.tool_invocation_id
        );
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
        trace!("Extracting digest from '{}'", resource_name);
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
        _request: Request<QueryWriteStatusRequest>,
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

        info!("Handling write for: {:?}", digest);

        if digest.size_bytes > 512 * 1024 * 1024 {
            return Err(Status::new(
                Code::ResourceExhausted,
                "I cannot cope with data that big",
            ));
        }

        let mut data: Vec<u8> = Vec::with_capacity(digest.size_bytes as usize);

        data.extend_from_slice(&firstmsg.data);

        let mut finish_write = firstmsg.finish_write;

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

        trace!("Inserting {}/{} into map", digest.hash, digest.size_bytes);

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

        info!(
            "Handling read of {} bytes starting at {} for {:?}",
            to_send, offset, digest
        );

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

#[derive(Clone)]
struct PlayCASServer {
    inner: Arc<Mutex<CASServer>>,
}

impl PlayCASServer {
    fn new(inner: Arc<Mutex<CASServer>>) -> Self {
        Self { inner }
    }
    fn server(&self) -> Result<MutexGuard<CASServer>, Status> {
        self.inner
            .lock()
            .map_err(|_| Status::new(Code::Unknown, "Mutex poisoned?"))
    }

    fn get_dir(&self, digest: &Digest) -> Result<Directory, Status> {
        let data = self
            .server()?
            .content
            .get(digest)
            .ok_or_else(|| Status::new(Code::NotFound, "unknown digest"))
            .map(Arc::clone)?;
        let dir: Directory = Directory::decode(data.as_ref()).map_err(|e| {
            Status::new(
                Code::InvalidArgument,
                format!("Unable to decode directory {:?}: {:?}", digest, e),
            )
        })?;
        Ok(dir)
    }
}

#[async_trait]
impl ContentAddressableStorage for PlayCASServer {
    type GetTreeStream = ReceiverStream<Result<GetTreeResponse, Status>>;

    async fn find_missing_blobs(
        &self,
        _request: Request<FindMissingBlobsRequest>,
    ) -> Result<Response<FindMissingBlobsResponse>, Status> {
        info!("Find missing blobs - unimplemented");
        Err(Status::new(Code::Unimplemented, "not implemented"))
    }

    async fn batch_update_blobs(
        &self,
        request: Request<BatchUpdateBlobsRequest>,
    ) -> Result<Response<BatchUpdateBlobsResponse>, Status> {
        if request.get_ref().instance_name != self.server()?.instance_name {
            return Err(Status::new(Code::InvalidArgument, "Unknown instance"));
        }

        let mut response = BatchUpdateBlobsResponse { responses: vec![] };

        info!(
            "Handling batch update blobs ({} updates)",
            request.get_ref().requests.len()
        );

        for request in request.into_inner().requests {
            let data = request.data.into();
            let digest = request.digest.unwrap();
            trace!("Batch: Inserting {:?}", digest);
            self.server()?.content.insert(digest.clone(), data);
            let entry = batch_update_blobs_response::Response {
                digest: Some(digest),
                status: Some(rpc::Status {
                    code: rpc::Code::Ok as i32,
                    message: "".into(),
                    details: vec![],
                }),
            };
            response.responses.push(entry);
        }

        Ok(Response::new(response))
    }

    async fn batch_read_blobs(
        &self,
        _request: Request<BatchReadBlobsRequest>,
    ) -> Result<Response<BatchReadBlobsResponse>, Status> {
        Err(Status::new(Code::Unimplemented, "not implemented"))
    }

    async fn get_tree(
        &self,
        request: Request<GetTreeRequest>,
    ) -> Result<Response<Self::GetTreeStream>, Status> {
        if request.get_ref().instance_name != self.server()?.instance_name {
            return Err(Status::new(Code::InvalidArgument, "Unknown instance"));
        }

        let root_digest = request.into_inner().root_digest.unwrap();

        if !self.server()?.content.contains_key(&root_digest) {
            return Err(Status::new(Code::NotFound, "Unknown root digest"));
        }
        info!("Retrieving tree for {:?}", root_digest);

        let (tx, rx) = mpsc::channel(4);

        let server = self.clone();

        tokio::spawn(async move {
            let root_dir: Directory = match server.get_dir(&root_digest) {
                Ok(d) => d,
                Err(e) => {
                    tx.send(Err(e)).await.unwrap();
                    return;
                }
            };
            tx.send(Ok(GetTreeResponse {
                directories: vec![root_dir.clone()],
                next_page_token: "".into(),
            }))
            .await
            .unwrap();
            let mut to_send = VecDeque::new();
            for dir in root_dir.directories {
                to_send.push_back(dir.digest.unwrap());
            }

            while let Some(next_digest) = to_send.pop_front() {
                let dir = match server.get_dir(&next_digest) {
                    Ok(d) => d,
                    Err(e) => {
                        tx.send(Err(e)).await.unwrap();
                        return;
                    }
                };
                tx.send(Ok(GetTreeResponse {
                    directories: vec![dir.clone()],
                    next_page_token: "".into(),
                }))
                .await
                .unwrap();
                for dir in dir.directories {
                    to_send.push_back(dir.digest.unwrap());
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
