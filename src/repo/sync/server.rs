use iroh::{
    Endpoint, EndpointId, SecretKey,
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler},
};
use serde::{Deserialize, Serialize};
use std::future::Future;
use thiserror::Error;

use crate::repo::{Backend, Hash, IROH_SECRET, RepoRefType, ToRepoError, backend::KeyType};
use iroh::discovery::mdns::MdnsDiscovery;

pub const ALPN: &[u8] = b"pika/sync/0";

#[derive(Serialize, Deserialize, Debug)]
pub enum RepoRequest {
    GetNode(Hash),
    GetRoot,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum RepoResponse {
    Node(Option<Vec<u8>>),
    Root(Option<Hash>),
    Error(String),
}

#[derive(Error, Debug)]
pub enum SyncError {
    #[error("backend error: {0}")]
    Backend(String),
    #[error("bind error: {0}")]
    Bind(#[from] iroh::endpoint::BindError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("join error: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("secret not found")]
    SecretNotFound,
    #[error("invalid secret")]
    InvalidSecret,
}

pub struct RepoServer<B: Backend> {
    backend: B,
}

impl<B: Backend> RepoServer<B>
where
    B::Error: ToRepoError,
    B: Clone + Send + Sync + std::fmt::Debug + 'static,
{
    pub fn new(backend: B) -> Self {
        Self { backend }
    }

    pub async fn start(&self) -> Result<(EndpointId, iroh::protocol::Router), SyncError> {
        let mdns = MdnsDiscovery::builder();

        let secret_bytes = self
            .backend
            .get(KeyType::Secret, IROH_SECRET)
            .map_err(|e| SyncError::Backend(e.to_string()))?
            .ok_or(SyncError::SecretNotFound)?;

        let secret: [u8; 32] = secret_bytes
            .try_into()
            .map_err(|_| SyncError::InvalidSecret)?;
        let secret_key = SecretKey::from_bytes(&secret);

        let endpoint = Endpoint::builder()
            .discovery(mdns)
            .secret_key(secret_key)
            .bind()
            .await?;

        let endpoint_id = endpoint.id();

        let handler = RepoProtocolHandler {
            backend: self.backend.clone(),
        };

        let router = iroh::protocol::Router::builder(endpoint)
            .accept(ALPN, handler)
            .spawn();

        Ok((endpoint_id, router))
    }
}

#[derive(Debug, Clone)]
struct RepoProtocolHandler<B: Backend> {
    backend: B,
}

impl<B: Backend> ProtocolHandler for RepoProtocolHandler<B>
where
    B: Clone + Send + Sync + std::fmt::Debug + 'static,
    B::Error: ToRepoError,
{
    fn accept(
        &self,
        connection: Connection,
    ) -> impl Future<Output = Result<(), AcceptError>> + std::marker::Send {
        let backend = self.backend.clone();
        Box::pin(async move {
            while let Ok((mut send, mut recv)) = connection.accept_bi().await {
                let backend = backend.clone();
                tokio::spawn(async move {
                    if let Ok(buf) = recv.read_to_end(10 * 1024 * 1024).await {
                        if let Ok(req) = postcard::from_bytes::<RepoRequest>(&buf) {
                            let resp = match req {
                                RepoRequest::GetNode(hash) => RepoResponse::Node(
                                    backend.get(KeyType::Node, &hash.to_string()).ok().flatten(),
                                ),
                                RepoRequest::GetRoot => RepoResponse::Root(
                                    backend
                                        .get(KeyType::Ref, RepoRefType::Committed.as_str())
                                        .ok()
                                        .flatten()
                                        .and_then(|bytes| bytes.try_into().ok())
                                        .map(Hash),
                                ),
                            };
                            if let Ok(resp_bytes) = postcard::to_stdvec(&resp) {
                                let _ = send.write_all(&resp_bytes).await;
                            }
                        }
                    }
                    let _ = send.finish();
                });
            }
            Ok(())
        })
    }
}
