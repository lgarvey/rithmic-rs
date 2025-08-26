use async_trait::async_trait;
use std::time::Duration;
use tracing::{error, info, warn};

use tokio::{
    net::TcpStream,
    time::{Instant, Interval, interval_at, sleep, timeout},
};

use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{Error, Message},
};

/// Number of seconds between heartbeats sent to the server.
pub const HEARTBEAT_SECS: u64 = 60;

/// Connection attempt timeout in seconds.
const CONNECT_TIMEOUT_SECS: u64 = 2;

/// Base backoff in milliseconds multiplied by the attempt number.
const BACKOFF_MS_BASE: u64 = 500;

/// A generic stream over the Rithmic connection exposing a handle for external control.
pub trait RithmicStream {
    type Handle;

    fn get_handle(&self) -> Self::Handle;
}

#[async_trait]
pub trait PlantActor {
    type Command;

    async fn run(&mut self);
    async fn handle_command(&mut self, command: Self::Command);
    async fn handle_rithmic_message(&mut self, message: Result<Message, Error>)
    -> Result<bool, ()>;
}

pub fn get_heartbeat_interval(override_secs: Option<u64>) -> Interval {
    let secs = override_secs.unwrap_or(HEARTBEAT_SECS);
    let heartbeat_interval = Duration::from_secs(secs);
    let start_offset = Instant::now() + heartbeat_interval;

    interval_at(start_offset, heartbeat_interval)
}

/// Sometimes the connection gets stuck and retrying seems to help.
///
/// Arguments:
/// * `primary_url`: Primary URL to connect to first
/// * `secondary_url`: Beta URL to alternate with after the first failure
/// * `max_attempts`: Total number of attempts to connect
///
/// Returns:
/// * `Ok`: A WebSocketStream if the connection is successful.
/// * `Err`: An error if the connection fails after the specified number of attempts.
pub async fn connect_with_retry(
    primary_url: &str,
    secondary_url: &str,
    max_attempts: u32,
) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Error> {
    for attempt in 1..=max_attempts {
        let selected_url = if attempt == 1 {
            primary_url
        } else if attempt % 2 == 0 {
            secondary_url
        } else {
            primary_url
        };

        info!("Attempt {}: connecting to {}", attempt, selected_url);

        match timeout(
            Duration::from_secs(CONNECT_TIMEOUT_SECS),
            connect_async_with_config(selected_url, None, true),
        )
        .await
        {
            Ok(Ok((ws_stream, _))) => return Ok(ws_stream),
            Ok(Err(e)) => warn!("connect_async failed for {}: {:?}", selected_url, e),
            Err(e) => warn!("connect_async to {} timed out: {:?}", selected_url, e),
        }

        if attempt < max_attempts {
            let backoff_ms: u64 = BACKOFF_MS_BASE * attempt as u64;

            info!("Backing off for {}ms before retry", backoff_ms,);

            sleep(Duration::from_millis(backoff_ms)).await;
        }
    }

    error!("max connection attempts reached");

    Err(Error::Io(std::io::Error::new(
        std::io::ErrorKind::Other,
        "max connection attempts reached",
    )))
}
