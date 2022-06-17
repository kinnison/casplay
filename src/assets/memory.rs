//! Memory based asset cache
//!

use std::{collections::HashMap, sync::Arc};

use tokio::sync::Mutex;
use tonic::{async_trait, Status};

use crate::{
    build::bazel::remote::{
        asset::v1::{
            FetchBlobRequest, FetchBlobResponse, FetchDirectoryRequest, FetchDirectoryResponse,
            PushBlobRequest, PushBlobResponse, PushDirectoryRequest, PushDirectoryResponse,
            Qualifier,
        },
        execution::v2::Digest,
    },
    google::rpc,
    storage::StorageBackendInstance,
};

use super::{AssetCacheStorage, AssetCacheStorageInstance, Result};

#[derive(Debug, PartialEq, Eq, Hash)]
struct AssetKey {
    uri: String,
    directory: bool,
    qualifiers: Vec<Qualifier>,
}

impl AssetKey {
    fn generate(
        uris: Vec<String>,
        directory: bool,
        qualifiers: Vec<Qualifier>,
        digest: Digest,
    ) -> impl Iterator<Item = (AssetKey, Digest)> {
        Self::generate_keys(uris, directory, qualifiers).map(move |key| (key, digest.clone()))
    }

    fn generate_keys(
        uris: Vec<String>,
        directory: bool,
        qualifiers: Vec<Qualifier>,
    ) -> impl Iterator<Item = AssetKey> {
        uris.into_iter().map(move |uri| AssetKey {
            uri,
            directory,
            qualifiers: qualifiers.clone(),
        })
    }
}

pub struct MemoryAssetStorage {
    _storage: StorageBackendInstance,
    assets: Arc<Mutex<HashMap<AssetKey, Digest>>>,
}

impl MemoryAssetStorage {
    pub fn instantiate(storage: StorageBackendInstance) -> AssetCacheStorageInstance {
        Box::new(Self {
            _storage: storage,
            assets: Arc::new(Mutex::new(HashMap::new())),
        }) as AssetCacheStorageInstance
    }
}

#[async_trait]
impl AssetCacheStorage for MemoryAssetStorage {
    async fn push_blob(&self, req: PushBlobRequest) -> Result<PushBlobResponse> {
        let PushBlobRequest {
            blob_digest,
            uris,
            mut qualifiers,
            ..
        } = req;
        let digest = blob_digest.ok_or_else(|| Status::invalid_argument("missing digest"))?;
        qualifiers.sort_unstable();
        let mut assets = self.assets.lock().await;
        assets.extend(AssetKey::generate(uris, false, qualifiers, digest));
        Ok(PushBlobResponse {})
    }

    async fn push_directory(&self, req: PushDirectoryRequest) -> Result<PushDirectoryResponse> {
        let PushDirectoryRequest {
            root_directory_digest,
            uris,
            mut qualifiers,
            ..
        } = req;
        let digest =
            root_directory_digest.ok_or_else(|| Status::invalid_argument("missing digest"))?;
        qualifiers.sort_unstable();
        let mut assets = self.assets.lock().await;
        assets.extend(AssetKey::generate(uris, true, qualifiers, digest));
        Ok(PushDirectoryResponse {})
    }

    async fn fetch_blob(&self, req: FetchBlobRequest) -> Result<FetchBlobResponse> {
        let FetchBlobRequest {
            uris,
            mut qualifiers,
            ..
        } = req;
        qualifiers.sort_unstable();
        let assets = self.assets.lock().await;
        for key in AssetKey::generate_keys(uris, false, qualifiers) {
            if let Some(digest) = assets.get(&key) {
                return Ok(FetchBlobResponse {
                    status: Some(rpc::Status {
                        code: rpc::Code::Ok as i32,
                        message: "".into(),
                        details: vec![],
                    }),
                    uri: key.uri,
                    qualifiers: key.qualifiers,
                    expires_at: None,
                    blob_digest: Some(digest.clone()),
                });
            }
        }
        Err(Status::not_found("unable to find any matching blob asset"))
    }

    async fn fetch_directory(&self, req: FetchDirectoryRequest) -> Result<FetchDirectoryResponse> {
        let FetchDirectoryRequest {
            uris,
            mut qualifiers,
            ..
        } = req;
        qualifiers.sort_unstable();
        let assets = self.assets.lock().await;
        for key in AssetKey::generate_keys(uris, true, qualifiers) {
            if let Some(digest) = assets.get(&key) {
                return Ok(FetchDirectoryResponse {
                    status: Some(rpc::Status {
                        code: rpc::Code::Ok as i32,
                        message: "".into(),
                        details: vec![],
                    }),
                    uri: key.uri,
                    qualifiers: key.qualifiers,
                    expires_at: None,
                    root_directory_digest: Some(digest.clone()),
                });
            }
        }
        Err(Status::not_found(
            "unable to find any matching directory asset",
        ))
    }
}
