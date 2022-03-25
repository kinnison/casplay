//! Disk based action cache, stores the action results in a CAS

use std::path::{Path, PathBuf};

use tokio::io;
use tonic::{async_trait, Status};

use prost::Message;

use crate::{
    build::bazel::remote::execution::v2::{ActionResult, Digest},
    storage::{StorageBackendExt, StorageBackendInstance},
};

use super::{ActionCacheStorage, ActionCacheStorageInstance, Result};

pub struct OnDiskActionStorage {
    storage: StorageBackendInstance,
    basepath: PathBuf,
}

impl OnDiskActionStorage {
    pub fn instantiate<P>(
        storage: StorageBackendInstance,
        basepath: P,
    ) -> io::Result<ActionCacheStorageInstance>
    where
        P: AsRef<Path>,
    {
        let basepath = basepath.as_ref().to_path_buf();
        std::fs::create_dir_all(&basepath)?;
        Ok(Box::new(Self { storage, basepath }) as ActionCacheStorageInstance)
    }

    fn path_for(&self, digest: &Digest) -> PathBuf {
        self.basepath
            .join(format!("{}-{}", digest.hash, digest.size_bytes))
    }

    fn temp_path_for(&self, digest: &Digest) -> PathBuf {
        self.basepath
            .join(format!("{}-{}.tmp", digest.hash, digest.size_bytes))
    }
}

#[async_trait]
impl ActionCacheStorage for OnDiskActionStorage {
    async fn get_action_result(&self, digest: &Digest) -> Result<ActionResult> {
        let result_content = tokio::fs::read(self.path_for(digest)).await?;
        let result_digest = Digest::decode(&result_content[..])
            .map_err(|_| Status::internal("Digest decode failed"))?;
        self.storage.get_message(&result_digest).await
    }

    async fn update_action_result(
        &self,
        digest: &Digest,
        action_result: ActionResult,
    ) -> Result<ActionResult> {
        let result_digest = self.storage.store_message(&action_result).await?;
        let result_vec = result_digest.encode_to_vec();
        let temp_path = self.temp_path_for(digest);
        let real_path = self.path_for(digest);
        tokio::fs::write(&temp_path, &result_vec).await?;
        tokio::fs::rename(&temp_path, &real_path).await?;
        Ok(action_result)
    }
}
