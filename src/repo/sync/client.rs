use iroh::{Endpoint, EndpointId, discovery::mdns::MdnsDiscovery};
use std::collections::VecDeque;
use thiserror::Error;

use crate::repo::{
    Backend, Hash, RepoRefType, ToRepoError,
    backend::KeyType,
    mst::MstNode,
    sync::server::{ALPN, RepoRequest, RepoResponse},
};

#[derive(Error, Debug)]
pub enum PullError {
    #[error("backend error: {0}")]
    Backend(String),
    #[error("sync error: {0}")]
    Sync(String),
    #[error("uncommitted changes")]
    UncommittedChanges,
    #[error("connection error: {0}")]
    Connection(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("postcard error: {0}")]
    Postcard(#[from] postcard::Error),
    #[error("node not found: {0}")]
    NodeNotFound(Hash),
}

pub struct RepoClient<B: Backend> {
    backend: B,
}

impl<B: Backend> RepoClient<B>
where
    B::Error: ToRepoError,
    B: Clone + Send + Sync + std::fmt::Debug + 'static,
{
    pub fn new(backend: B) -> Self {
        Self { backend }
    }

    pub async fn pull(&self, peer_id: EndpointId) -> Result<(), PullError> {
        // 1. Check for uncommitted changes
        let working_hash = self
            .backend
            .get(KeyType::Ref, RepoRefType::Working.as_str().as_bytes())
            .map_err(|e| PullError::Backend(e.to_string()))?;
        let committed_hash = self
            .backend
            .get(KeyType::Ref, RepoRefType::Committed.as_str().as_bytes())
            .map_err(|e| PullError::Backend(e.to_string()))?;

        if working_hash != committed_hash {
            return Err(PullError::UncommittedChanges);
        }

        // 2. Connect to remote peer
        let mdns = MdnsDiscovery::builder();
        let endpoint = Endpoint::builder()
            .discovery(mdns)
            .bind()
            .await
            .map_err(|e| PullError::Connection(e.to_string()))?;

        let connection = endpoint
            .connect(peer_id, ALPN)
            .await
            .map_err(|e| PullError::Connection(e.to_string()))?;

        // 3. Get remote root
        let remote_root = self.get_remote_root(&connection).await?;
        let remote_root = match remote_root {
            Some(h) => h,
            None => return Ok(()), // Empty repo on remote?
        };

        // 4. Fetch missing nodes
        let mut queue = VecDeque::new();
        queue.push_back(remote_root.clone());

        while let Some(hash) = queue.pop_front() {
            if self
                .backend
                .get(KeyType::Node, &hash.0)
                .map_err(|e| PullError::Backend(e.to_string()))?
                .is_none()
            {
                let node_data = self.get_remote_node(&connection, &hash).await?;
                let node_data = node_data.ok_or_else(|| PullError::NodeNotFound(hash.clone()))?;

                // Decompress to find children
                let decompressed = lz4_flex::decompress_size_prepended(&node_data)
                    .map_err(|e| PullError::Sync(format!("decompression error: {}", e)))?;
                let node: MstNode = postcard::from_bytes(&decompressed)?;

                // Add children to queue
                if let Some(h) = node.left {
                    queue.push_back(h);
                }
                for item in node.items {
                    if let Some(h) = item.right {
                        queue.push_back(h);
                    }
                }

                // Save node data (already compressed by server)
                self.backend
                    .set(KeyType::Node, &hash.0, &node_data)
                    .map_err(|e| PullError::Backend(e.to_string()))?;
            }
        }

        // 5. Update working ref
        self.backend
            .set(
                KeyType::Ref,
                RepoRefType::Working.as_str().as_bytes(),
                &remote_root.0,
            )
            .map_err(|e| PullError::Backend(e.to_string()))?;

        Ok(())
    }

    async fn get_remote_root(
        &self,
        connection: &iroh::endpoint::Connection,
    ) -> Result<Option<Hash>, PullError> {
        let (mut send, mut recv) = connection
            .open_bi()
            .await
            .map_err(|e| PullError::Connection(e.to_string()))?;

        let req = RepoRequest::GetRoot;
        let req_bytes = postcard::to_stdvec(&req)?;
        send.write_all(&req_bytes)
            .await
            .map_err(|e| PullError::Connection(e.to_string()))?;
        send.finish()
            .map_err(|e| PullError::Connection(e.to_string()))?;

        let resp_bytes = recv
            .read_to_end(1024)
            .await
            .map_err(|e| PullError::Connection(e.to_string()))?;
        let resp: RepoResponse = postcard::from_bytes(&resp_bytes)?;

        match resp {
            RepoResponse::Root(h) => Ok(h),
            RepoResponse::Error(e) => Err(PullError::Sync(e)),
            _ => Err(PullError::Sync("unexpected response".to_string())),
        }
    }

    async fn get_remote_node(
        &self,
        connection: &iroh::endpoint::Connection,
        hash: &Hash,
    ) -> Result<Option<Vec<u8>>, PullError> {
        let (mut send, mut recv) = connection
            .open_bi()
            .await
            .map_err(|e| PullError::Connection(e.to_string()))?;

        let req = RepoRequest::GetNode(hash.clone());
        let req_bytes = postcard::to_stdvec(&req)?;
        send.write_all(&req_bytes)
            .await
            .map_err(|e| PullError::Connection(e.to_string()))?;
        send.finish()
            .map_err(|e| PullError::Connection(e.to_string()))?;

        let resp_bytes = recv
            .read_to_end(10 * 1024 * 1024)
            .await
            .map_err(|e| PullError::Connection(e.to_string()))?;
        let resp: RepoResponse = postcard::from_bytes(&resp_bytes)?;

        match resp {
            RepoResponse::Node(data) => Ok(data),
            RepoResponse::Error(e) => Err(PullError::Sync(e)),
            _ => Err(PullError::Sync("unexpected response".to_string())),
        }
    }
}
