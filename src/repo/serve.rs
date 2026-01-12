use std::future::Future;
use iroh::{Endpoint, EndpointId, SecretKey, endpoint::Connection, protocol::{AcceptError, ProtocolHandler}};
use thiserror::Error;

use crate::repo::{Backend, IROH_SECRET, backend::KeyType, ToRepoError};
use iroh::discovery::mdns::MdnsDiscovery;

pub const ALPN: &[u8] = b"pika/sync/0";

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
    B::Error: ToRepoError
{
    pub fn new(backend: B) -> Self {
        Self { backend }
    }

    pub async fn start(&self) -> Result<(EndpointId, iroh::protocol::Router), SyncError> {
        let mdns = MdnsDiscovery::builder();

        let secret_bytes = self.backend.get(KeyType::Secret, IROH_SECRET)
            .map_err(|e| SyncError::Backend(e.to_string()))?
            .ok_or(SyncError::SecretNotFound)?;
        
        let secret: [u8; 32] = secret_bytes.try_into().map_err(|_| SyncError::InvalidSecret)?;
        let secret_key = SecretKey::from_bytes(&secret);

        let endpoint = Endpoint::builder()
            .discovery(mdns)
            .secret_key(secret_key)
            .bind()
            .await?;

        let endpoint_id = endpoint.id();

        let router = iroh::protocol::Router::builder(endpoint)
            .accept(ALPN, RepoServerHandler)
            .spawn();

        Ok((endpoint_id, router))
    }
}

#[derive(Debug, Clone)]
struct RepoServerHandler;

impl ProtocolHandler for RepoServerHandler {
    /// The `accept` method is called for each incoming connection for our ALPN.
    ///
    /// The returned future runs on a newly spawned tokio task, so it can run as long as
    /// the connection lasts without blocking other connections.
    fn accept(
        &self,
        connection: Connection,
    ) -> impl Future<Output = Result<(), AcceptError>> + std::marker::Send {
        Box::pin(async move {
            let endpoint_id = connection.remote_id();
            println!("accepted connection from {endpoint_id}");

            let (mut send, mut recv) = connection.accept_bi().await?;

            let bytes_sent = tokio::io::copy(&mut recv, &mut send).await?;
            println!("Copied over {bytes_sent} byte(s)");

            send.finish()?;

            connection.closed().await;

            Ok(())
        })
    }
}
