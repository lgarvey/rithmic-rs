use async_trait::async_trait;
use std::time::Duration;
use tokio::time::{Instant, Interval, interval_at, sleep, timeout};
use tracing::{Level, event};

use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{Error, Message},
};

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

pub fn get_heartbeat_interval() -> Interval {
    let heartbeat_interval = Duration::from_secs(60);
    let start_offset = Instant::now() + heartbeat_interval;

    interval_at(start_offset, heartbeat_interval)
}

/// Sometimes the connection gets stuck and retrying seems to help.
///
/// Arguments:
/// * `url`: The URL to connect to.
/// * `max_attempts`: The number of attempts to connect.
///
/// Returns:
/// * `Ok`: A WebSocketStream if the connection is successful.
/// * `Err`: An error if the connection fails after the specified number of attempts.
pub async fn connect_with_retry(
    url: &str,
    max_attempts: u32,
) -> Result<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>, Error> {
    let mut attempt = 0;

    loop {
        attempt += 1;

        match timeout(
            Duration::from_secs(2),
            connect_async_with_config(url, None, true),
        )
        .await
        {
            Ok(Ok((ws_stream, _))) => {
                return Ok(ws_stream);
            }
            Ok(Err(e)) => {
                event!(Level::WARN, "connect_async failed: {:?}, retrying...", e);
            }
            Err(_) => {
                event!(Level::WARN, "connect_async timed out, retrying...");
            }
        }

        if attempt >= max_attempts {
            event!(Level::ERROR, "max connection attempts reached");

            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "max connection attempts reached",
            )));
        }

        sleep(Duration::from_millis(500)).await;

        event!(
            Level::INFO,
            "Attempting to connect to {} - attempt {}",
            url,
            attempt
        );
    }
}
