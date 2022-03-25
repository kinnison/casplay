//! CASPlay server

use std::{collections::VecDeque, net::SocketAddr, path::Path, sync::Arc};

use lazy_static::lazy_static;
use regex::Regex;

use prost::Message;
use tokio::sync::{mpsc, Mutex, MutexGuard};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
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
    actioncache::{
        disk::OnDiskActionStorage, memory::MemoryActionStorage, remote::RemoteActionStorage,
        ActionCacheStorageInstance,
    },
    build::bazel::{
        remote::execution::v2::{
            action_cache_server::{ActionCache, ActionCacheServer},
            batch_read_blobs_response, batch_update_blobs_response,
            capabilities_server::{Capabilities, CapabilitiesServer},
            compressor,
            content_addressable_storage_server::{
                ContentAddressableStorage, ContentAddressableStorageServer,
            },
            digest_function, symlink_absolute_path_strategy, ActionCacheUpdateCapabilities,
            ActionResult, BatchReadBlobsRequest, BatchReadBlobsResponse, BatchUpdateBlobsRequest,
            BatchUpdateBlobsResponse, CacheCapabilities, Digest, Directory,
            FindMissingBlobsRequest, FindMissingBlobsResponse, GetActionResultRequest,
            GetCapabilitiesRequest, GetTreeRequest, GetTreeResponse, RequestMetadata,
            ServerCapabilities, ToolDetails, Tree, UpdateActionResultRequest,
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
    storage::{
        disk::OnDiskStorage, memory::MemoryStorage, remote::RemoteStorage, StorageBackend,
        StorageBackendInstance,
    },
};

const MAX_BATCH_BYTES: i64 = 4193280;
pub async fn serve(
    dst: SocketAddr,
    instance_name: &str,
    base: Option<&Path>,
    remote: Option<&str>,
    remote_instance: &str,
    lru_memory_limit: usize,
) -> anyhow::Result<()> {
    let storage = match (base, remote) {
        (None, None) => MemoryStorage::instantiate(lru_memory_limit),
        (Some(path), None) => OnDiskStorage::instantiate(path.join("cas"))?,
        (None, Some(url)) => RemoteStorage::instantiate(remote_instance, url).await?,
        (_, _) => unreachable!(),
    };
    let action_storage = match (base, remote) {
        (None, None) => {
            MemoryActionStorage::instantiate(storage.make_copy().await?, lru_memory_limit >> 16)
        }
        (Some(path), None) => {
            OnDiskActionStorage::instantiate(storage.make_copy().await?, path.join("ac"))?
        }
        (None, Some(url)) => RemoteActionStorage::instantiate(remote_instance, url).await?,
        (_, _) => unreachable!(),
    };
    let server = Arc::new(Mutex::new(CASServer::new(
        instance_name,
        storage,
        action_storage,
    )));
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
        .add_service(ActionCacheServer::new(PlayActionCache::new(Arc::clone(
            &server,
        ))))
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
    storage: StorageBackendInstance,
    actions: ActionCacheStorageInstance,
}

impl CASServer {
    fn new(
        instance_name: &str,
        storage: StorageBackendInstance,
        actions: ActionCacheStorageInstance,
    ) -> Self {
        Self {
            instance_name: instance_name.to_string(),
            storage,
            actions,
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
        if request.get_ref().instance_name == self.inner.lock().await.instance_name {
            Ok(Response::new(ServerCapabilities {
                cache_capabilities: Some(CacheCapabilities {
                    digest_functions: vec![digest_function::Value::Sha256 as i32],
                    action_cache_update_capabilities: Some(ActionCacheUpdateCapabilities {
                        update_enabled: true,
                    }),
                    cache_priority_capabilities: None,
                    max_batch_total_size_bytes: MAX_BATCH_BYTES,
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
    async fn server(&self) -> MutexGuard<'_, CASServer> {
        self.inner.lock().await
    }

    async fn extract_digest(&self, resource_name: &str, is_write: bool) -> Result<Digest, Status> {
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
        if self.server().await.instance_name != instance_name {
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
        let _digest = self
            .extract_digest(&request.get_ref().resource_name, true)
            .await?;
        Err(Status::new(Code::NotFound, "We never have partial writes"))
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

        let digest = self.extract_digest(&firstmsg.resource_name, true).await?;

        info!("Handling write for: {:?}", digest);

        if digest.size_bytes > 512 * 1024 * 1024 {
            return Err(Status::new(
                Code::ResourceExhausted,
                "I cannot cope with data that big",
            ));
        }

        let server = self.server().await;
        let mut writer = server.storage.start_write(&digest).await?;

        writer.write_block(&firstmsg.data).await?;

        let mut finish_write = firstmsg.finish_write;

        while let Some(msg) = wstream.message().await? {
            // Check the write_offset
            writer.write_block(&msg.data).await?;
            // Make sure the last chunk is a finish_write
            finish_write = msg.finish_write;
        }

        if !finish_write {
            return Err(Status::new(
                Code::Cancelled,
                "You stopped uploading, you fool!",
            ));
        }

        let uploaded_digest = writer.finish().await?;

        if uploaded_digest != digest {
            return Err(Status::new(
                Code::InvalidArgument,
                format!(
                    "Uploaded data has digest {}/{} whereas upload said it would be {}/{}",
                    uploaded_digest.hash,
                    uploaded_digest.size_bytes,
                    digest.hash,
                    digest.size_bytes
                ),
            ));
        }

        // At this point we have a Digest, and a Vec of bytes, let's put it into the server.

        trace!("Inserted {}/{} into map", digest.hash, digest.size_bytes);

        let response = WriteResponse {
            committed_size: digest.size_bytes,
        };

        Ok(Response::new(response))
    }

    async fn read(
        &self,
        request: Request<ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let digest = self
            .extract_digest(&request.get_ref().resource_name, false)
            .await?;
        let offset = request.get_ref().read_offset;
        let limit = request.get_ref().read_limit;

        let mut reader = self
            .server()
            .await
            .storage
            .start_read(&digest, offset, limit)
            .await?;

        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            while let Some(block) = reader.next().await {
                match block {
                    Ok(data) => {
                        let msg = ReadResponse { data };
                        tx.send(Ok(msg)).await.unwrap();
                    }
                    Err(e) => {
                        tx.send(Err(e)).await.unwrap();
                        break;
                    }
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
    async fn server(&self) -> MutexGuard<'_, CASServer> {
        self.inner.lock().await
    }

    async fn get_dir(&self, digest: &Digest) -> Result<Directory, Status> {
        let data = self.server().await.storage.read_blob(digest).await?;
        let dir: Directory = Directory::decode(data.as_ref()).map_err(|e| {
            Status::new(
                Code::InvalidArgument,
                format!("Unable to decode directory {:?}: {:?}", digest, e),
            )
        })?;
        Ok(dir)
    }
}

fn rpc_code_for_status(status: Code) -> rpc::Code {
    match status {
        Code::Ok => rpc::Code::Ok,
        Code::Cancelled => rpc::Code::Cancelled,
        Code::Unknown => rpc::Code::Unknown,
        Code::InvalidArgument => rpc::Code::InvalidArgument,
        Code::DeadlineExceeded => rpc::Code::DeadlineExceeded,
        Code::NotFound => rpc::Code::NotFound,
        Code::AlreadyExists => rpc::Code::AlreadyExists,
        Code::PermissionDenied => rpc::Code::PermissionDenied,
        Code::ResourceExhausted => rpc::Code::ResourceExhausted,
        Code::FailedPrecondition => rpc::Code::FailedPrecondition,
        Code::Aborted => rpc::Code::Aborted,
        Code::OutOfRange => rpc::Code::OutOfRange,
        Code::Unimplemented => rpc::Code::Unimplemented,
        Code::Internal => rpc::Code::Internal,
        Code::Unavailable => rpc::Code::Unavailable,
        Code::DataLoss => rpc::Code::DataLoss,
        Code::Unauthenticated => rpc::Code::Unauthenticated,
    }
}

#[async_trait]
impl ContentAddressableStorage for PlayCASServer {
    type GetTreeStream = ReceiverStream<Result<GetTreeResponse, Status>>;

    async fn find_missing_blobs(
        &self,
        request: Request<FindMissingBlobsRequest>,
    ) -> Result<Response<FindMissingBlobsResponse>, Status> {
        if request.get_ref().instance_name != self.server().await.instance_name {
            return Err(Status::new(Code::InvalidArgument, "Unknown instance"));
        }

        let request = request.into_inner();
        let response = FindMissingBlobsResponse {
            missing_blob_digests: self
                .server()
                .await
                .storage
                .find_missing_blobs(Box::new(request.blob_digests.into_iter()))
                .await?,
        };

        Ok(Response::new(response))
    }

    async fn batch_update_blobs(
        &self,
        request: Request<BatchUpdateBlobsRequest>,
    ) -> Result<Response<BatchUpdateBlobsResponse>, Status> {
        if request.get_ref().instance_name != self.server().await.instance_name {
            return Err(Status::new(Code::InvalidArgument, "Unknown instance"));
        }

        info!(
            "Handling batch update blobs ({} updates)",
            request.get_ref().requests.len()
        );

        let update = request
            .into_inner()
            .requests
            .into_iter()
            .map(|r| (r.digest.unwrap(), r.data));
        let update_result = self
            .server()
            .await
            .storage
            .batch_update_blobs(Box::new(update))
            .await?;

        let response = BatchUpdateBlobsResponse {
            responses: update_result
                .into_iter()
                .map(|(digest, status)| batch_update_blobs_response::Response {
                    digest: Some(digest),
                    status: Some(rpc::Status {
                        code: rpc_code_for_status(status.code()) as i32,
                        message: "".into(),
                        details: vec![],
                    }),
                })
                .collect(),
        };

        Ok(Response::new(response))
    }

    async fn batch_read_blobs(
        &self,
        request: Request<BatchReadBlobsRequest>,
    ) -> Result<Response<BatchReadBlobsResponse>, Status> {
        if request.get_ref().instance_name != self.server().await.instance_name {
            return Err(Status::new(Code::InvalidArgument, "Unknown instance"));
        }

        let request = request.into_inner();

        let total_bytes = request.digests.iter().map(|d| d.size_bytes).sum::<i64>();
        if total_bytes > MAX_BATCH_BYTES {
            return Err(Status::new(
                Code::InvalidArgument,
                "Too much data requested",
            ));
        }

        let batchread = self
            .server()
            .await
            .storage
            .batch_read_blobs(Box::new(request.digests.into_iter()))
            .await?;

        let response = BatchReadBlobsResponse {
            responses: batchread
                .into_iter()
                .map(|(digest, value)| {
                    let (data, code) = match value {
                        Ok(data) => (data, rpc::Code::Ok),
                        Err(s) => (vec![], rpc_code_for_status(s.code())),
                    };
                    batch_read_blobs_response::Response {
                        digest: Some(digest),
                        data,
                        compressor: compressor::Value::Identity as i32,
                        status: Some(rpc::Status {
                            code: code as i32,
                            message: "".into(),
                            details: vec![],
                        }),
                    }
                })
                .collect(),
        };

        Ok(Response::new(response))
    }

    async fn get_tree(
        &self,
        request: Request<GetTreeRequest>,
    ) -> Result<Response<Self::GetTreeStream>, Status> {
        if request.get_ref().instance_name != self.server().await.instance_name {
            return Err(Status::new(Code::InvalidArgument, "Unknown instance"));
        }

        let root_digest = request.into_inner().root_digest.unwrap();

        if !self.server().await.storage.contains(&root_digest).await? {
            return Err(Status::new(Code::NotFound, "Unknown root digest"));
        }
        info!("Retrieving tree for {:?}", root_digest);

        let (tx, rx) = mpsc::channel(4);

        let server = self.clone();

        tokio::spawn(async move {
            let root_dir: Directory = match server.get_dir(&root_digest).await {
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
                let dir = match server.get_dir(&next_digest).await {
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

struct PlayActionCache {
    inner: Arc<Mutex<CASServer>>,
}

impl PlayActionCache {
    fn new(inner: Arc<Mutex<CASServer>>) -> Self {
        Self { inner }
    }

    async fn server(&self) -> MutexGuard<'_, CASServer> {
        self.inner.lock().await
    }

    async fn validate_result(
        storage: &dyn StorageBackend,
        result: &ActionResult,
    ) -> Result<bool, Status> {
        for file in result.output_files.iter() {
            let digest = file.digest.as_ref().unwrap();
            if !storage.contains(digest).await? {
                // file didn't exist
                info!(
                    "Failed to find digest {}/{} for file {}",
                    digest.hash, digest.size_bytes, file.path
                );
                return Ok(false);
            }
        }
        for dir in result.output_directories.iter() {
            let tree = dir.tree_digest.as_ref().unwrap();
            let tree = storage.read_blob(tree).await?;
            if let Ok(tree) = Tree::decode(&tree[..]) {
                let tree: Tree = tree;
                for dir in tree.children.iter().chain(tree.root.as_ref()) {
                    for filenode in dir.files.iter() {
                        let digest = filenode.digest.as_ref().unwrap();
                        if !storage.contains(digest).await? {
                            // filenode wasn't found
                            info!(
                                "Failed to find digest {}/{} for nested filenode {}",
                                digest.hash, digest.size_bytes, filenode.name
                            );
                            return Ok(false);
                        }
                    }
                    for dirnode in dir.directories.iter() {
                        let digest = dirnode.digest.as_ref().unwrap();
                        if !storage.contains(digest).await? {
                            // dirnode wasn't found
                            info!(
                                "Failed to find digest {}/{} for nested directorynode {}",
                                digest.hash, digest.size_bytes, dirnode.name
                            );
                            return Ok(false);
                        }
                    }
                }
            } else {
                // tree didn't decode
                info!("Tree did not decode");
                return Ok(false);
            }
        }

        if let Some(digest) = result.stdout_digest.as_ref() {
            if !digest.hash.is_empty() && !storage.contains(digest).await? {
                // stdout is missing
                info!("stdout missing: {}/{}", digest.hash, digest.size_bytes);
                return Ok(false);
            }
        }

        if let Some(digest) = result.stderr_digest.as_ref() {
            if !digest.hash.is_empty() && !storage.contains(digest).await? {
                // stderr is missing
                info!("stderr missing: {}/{}", digest.hash, digest.size_bytes);
                return Ok(false);
            }
        }

        Ok(true)
    }
}

#[async_trait]
impl ActionCache for PlayActionCache {
    async fn get_action_result(
        &self,
        request: Request<GetActionResultRequest>,
    ) -> Result<Response<ActionResult>, Status> {
        if request.get_ref().instance_name != self.server().await.instance_name {
            return Err(Status::new(Code::InvalidArgument, "Unknown instance"));
        }

        let request = request.into_inner();
        let action_digest = request.action_digest.as_ref().map(Digest::clone).unwrap();
        info!(
            "get_action_result({}/{})",
            action_digest.hash, action_digest.size_bytes
        );
        let server = self.server().await;
        let result = server.actions.get_action_result(&action_digest).await?;
        if Self::validate_result(&server.storage, &result).await? {
            Ok(Response::new(result))
        } else {
            info!("Something was missing when validating action result");
            Err(Status::new(
                Code::NotFound,
                "Action result references something not present in the CAS",
            ))
        }
    }

    async fn update_action_result(
        &self,
        request: Request<UpdateActionResultRequest>,
    ) -> Result<Response<ActionResult>, Status> {
        if request.get_ref().instance_name != self.server().await.instance_name {
            return Err(Status::new(Code::InvalidArgument, "Unknown instance"));
        }

        let request = request.into_inner();
        let action_digest = request.action_digest.as_ref().map(Digest::clone).unwrap();
        info!("update_action_result({:?})", action_digest);

        let result = request.action_result.unwrap();
        let result = self
            .server()
            .await
            .actions
            .update_action_result(&action_digest, result)
            .await?;

        Ok(Response::new(result))
    }
}
