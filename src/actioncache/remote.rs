//! Remote action cache, stores the action results in a CAS

use std::sync::Arc;

use tokio::sync::Mutex;
use tonic::{async_trait, transport::Channel, Status};

use crate::build::bazel::remote::execution::v2::{
    action_cache_client::ActionCacheClient, ActionResult, Digest, GetActionResultRequest,
    UpdateActionResultRequest,
};

use super::{ActionCacheStorage, ActionCacheStorageInstance, Result};

pub struct RemoteActionStorage {
    instance_name: String,
    client: Arc<Mutex<ActionCacheClient<Channel>>>,
}

impl RemoteActionStorage {
    pub async fn instantiate(
        instance_name: &str,
        client_base: &str,
    ) -> Result<ActionCacheStorageInstance> {
        let client = ActionCacheClient::connect(client_base.to_owned())
            .await
            .map_err(|e| Status::internal(format!("connect error: {:?}", e)))?;
        Ok(Box::new(Self {
            instance_name: instance_name.to_string(),
            client: Arc::new(Mutex::new(client)),
        }) as ActionCacheStorageInstance)
    }
}

#[async_trait]
impl ActionCacheStorage for RemoteActionStorage {
    async fn get_action_result(&self, digest: &Digest) -> Result<ActionResult> {
        let request = GetActionResultRequest {
            instance_name: self.instance_name.clone(),
            action_digest: Some(digest.clone()),
            inline_stdout: false,
            inline_stderr: false,
            inline_output_files: vec![],
        };
        let response = self.client.lock().await.get_action_result(request).await?;
        Ok(response.into_inner())
    }

    async fn update_action_result(
        &self,
        digest: &Digest,
        action_result: ActionResult,
    ) -> Result<ActionResult> {
        let request = UpdateActionResultRequest {
            instance_name: self.instance_name.clone(),
            action_digest: Some(digest.clone()),
            action_result: Some(action_result),
            results_cache_policy: None,
        };
        let response = self
            .client
            .lock()
            .await
            .update_action_result(request)
            .await?;
        Ok(response.into_inner())
    }
}
