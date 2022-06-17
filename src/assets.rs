//! Remote asset cache stuff

use tonic::{async_trait, Status};

use crate::build::bazel::remote::asset::v1::{
    FetchBlobRequest, FetchBlobResponse, FetchDirectoryRequest, FetchDirectoryResponse,
    PushBlobRequest, PushBlobResponse, PushDirectoryRequest, PushDirectoryResponse,
};

type Result<T, E = Status> = std::result::Result<T, E>;

#[async_trait]
pub trait AssetCacheStorage: Send + Sync {
    async fn push_blob(&self, req: PushBlobRequest) -> Result<PushBlobResponse>;
    async fn push_directory(&self, req: PushDirectoryRequest) -> Result<PushDirectoryResponse>;

    async fn fetch_blob(&self, req: FetchBlobRequest) -> Result<FetchBlobResponse>;
    async fn fetch_directory(&self, req: FetchDirectoryRequest) -> Result<FetchDirectoryResponse>;
}

pub type AssetCacheStorageInstance = Box<dyn AssetCacheStorage>;

pub mod memory;
