//! Action cache abstraction for casplay
//!

use tonic::{async_trait, Status};

use crate::build::bazel::remote::execution::v2::{ActionResult, Digest};

type Result<T, E = Status> = std::result::Result<T, E>;

#[async_trait]
pub trait ActionCacheStorage: Send + Sync {
    async fn get_action_result(&self, digest: &Digest) -> Result<ActionResult>;

    async fn update_action_result(
        &self,
        digest: &Digest,
        action_result: ActionResult,
    ) -> Result<ActionResult>;
}

pub type ActionCacheStorageInstance = Box<dyn ActionCacheStorage>;

#[async_trait]
impl ActionCacheStorage for Box<dyn ActionCacheStorage> {
    async fn get_action_result(&self, digest: &Digest) -> Result<ActionResult> {
        self.as_ref().get_action_result(digest).await
    }

    async fn update_action_result(
        &self,
        digest: &Digest,
        action_result: ActionResult,
    ) -> Result<ActionResult> {
        self.as_ref()
            .update_action_result(digest, action_result)
            .await
    }
}

pub mod disk;
pub mod memory;

#[cfg(test)]
mod test {
    use super::*;
    use crate::storage::memory::MemoryStorage;

    #[tokio::test]
    async fn store_retrieve_action() -> Result<()> {
        let storage = MemoryStorage::instantiate();
        let action_storage = memory::MemoryActionStorage::instantiate(storage);
        let action_digest = Digest {
            hash: "hello".into(),
            size_bytes: 5,
        };
        let stored_result = action_storage
            .update_action_result(
                &action_digest,
                ActionResult {
                    output_files: Vec::new(),
                    output_file_symlinks: Vec::new(),
                    output_symlinks: Vec::new(),
                    output_directories: Vec::new(),
                    output_directory_symlinks: Vec::new(),
                    exit_code: 0,
                    stdout_raw: Vec::new(),
                    stdout_digest: None,
                    stderr_raw: Vec::new(),
                    stderr_digest: None,
                    execution_metadata: None,
                },
            )
            .await?;
        let retrieved_result = action_storage.get_action_result(&action_digest).await?;
        assert_eq!(stored_result, retrieved_result);
        Ok(())
    }
}
