//! Memory action cache, stores the action results in a CAS

use std::{collections::HashMap, sync::Arc};

use tokio::sync::Mutex;
use tonic::{async_trait, Status};

use crate::{
    build::bazel::remote::execution::v2::{ActionResult, Digest},
    storage::{StorageBackendExt, StorageBackendInstance},
};

use super::{ActionCacheStorage, ActionCacheStorageInstance, Result};

pub struct MemoryActionStorage {
    storage: StorageBackendInstance,
    mapping: Arc<Mutex<HashMap<Digest, Digest>>>,
}

impl MemoryActionStorage {
    pub fn instantiate(storage: StorageBackendInstance) -> ActionCacheStorageInstance {
        Box::new(Self {
            storage,
            mapping: Arc::new(Mutex::new(HashMap::new())),
        }) as ActionCacheStorageInstance
    }
}

#[async_trait]
impl ActionCacheStorage for MemoryActionStorage {
    async fn get_action_result(&self, digest: &Digest) -> Result<ActionResult> {
        let lock = self.mapping.lock().await;
        let result_digest = lock.get(digest).ok_or_else(|| {
            Status::not_found(format!(
                "ActionResult({}/{}) not found",
                digest.hash, digest.size_bytes
            ))
        })?;
        println!("OK, found result digest");
        self.storage.get_message(result_digest).await
    }

    async fn update_action_result(
        &self,
        digest: &Digest,
        action_result: ActionResult,
    ) -> Result<ActionResult> {
        let result_digest = self.storage.store_message(&action_result).await?;
        self.mapping
            .lock()
            .await
            .insert(digest.clone(), result_digest);
        Ok(action_result)
    }
}
