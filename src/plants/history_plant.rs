use async_trait::async_trait;
use tracing::{Level, event};

use tokio_tungstenite::{
    MaybeTlsStream,
    tungstenite::{Error, Message},
};

use crate::{
    api::{
        receiver_api::{RithmicReceiverApi, RithmicResponse},
        sender_api::RithmicSenderApi,
    },
    connection_info::{self, AccountInfo},
    request_handler::{RithmicRequest, RithmicRequestHandler},
    rti::request_login::SysInfraType,
    ws::{PlantActor, RithmicStream, connect_with_retry, get_heartbeat_interval},
};

use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};

use tokio::{
    net::TcpStream,
    sync::{broadcast, mpsc, oneshot},
    task::JoinHandle,
    time::Interval,
};

pub enum HistoryPlantCommand {
    Close,
    ListSystemInfo {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    Login {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    SetLogin,
    Logout {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    SendHeartbeat {},
    LoadTicks {
        end_time_sec: i32,
        exchange: String,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
        start_time_sec: i32,
        symbol: String,
    },
}

/// The RithmicHistoryPlant provides access to historical market data through the Rithmic API.
///
/// It allows applications to retrieve historical tick data for specific instruments and time ranges
/// from Rithmic's history database.
///
/// # Example
///
/// ```no_run
/// use rithmic_rs::{connection_info::{AccountInfo, RithmicConnectionSystem}, plants::history_plant::RithmicHistoryPlant};
/// use tokio::time::{sleep, Duration};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     // Step 1: Create connection credentials
///     let account_info = AccountInfo {
///         account_id: "your_account".to_string(),
///         env: RithmicConnectionSystem::Demo,
///         fcm_id: "your_fcm".to_string(),
///         ib_id: "your_ib".to_string(),
///     };
///
///     // Step 2: Create the history plant instance
///     let history_plant = RithmicHistoryPlant::new(&account_info).await;
///
///     // Step 3: Get a handle to interact with the plant
///     let handle = history_plant.get_handle();
///
///     // Step 4: Login to the history plant
///     handle.login().await?;
///
///     // Step 5: Load historical tick data
///     let now = std::time::SystemTime::now()
///         .duration_since(std::time::UNIX_EPOCH)
///         .unwrap()
///         .as_secs() as i32;
///
///     // Get the last hour of data
///     let one_hour_ago = now - 3600;
///
///     let ticks = handle.load_ticks(
///         "ESM1".to_string(),
///         "CME".to_string(),
///         one_hour_ago,
///         now,
///     ).await?;
///
///     println!("Received {} tick responses", ticks.len());
///
///     // Step 6: Disconnect when done
///     handle.disconnect().await?;
///
///     Ok(())
/// }
/// ```
pub struct RithmicHistoryPlant {
    pub connection_handle: JoinHandle<()>,
    sender: mpsc::Sender<HistoryPlantCommand>,
    subscription_sender: broadcast::Sender<RithmicResponse>,
}

impl RithmicHistoryPlant {
    /// Create a new History Plant connection
    ///
    /// # Arguments
    /// * `account_info` - Account credentials and environment settings
    ///
    /// # Returns
    /// A new `RithmicHistoryPlant` instance connected to the Rithmic server
    pub async fn new(account_info: &AccountInfo) -> RithmicHistoryPlant {
        let (req_tx, req_rx) = mpsc::channel::<HistoryPlantCommand>(32);
        let (sub_tx, _sub_rx) = broadcast::channel::<RithmicResponse>(20_000);

        let mut history_plant = HistoryPlant::new(req_rx, sub_tx.clone(), account_info)
            .await
            .unwrap();

        let connection_handle = tokio::spawn(async move {
            history_plant.run().await;
        });

        RithmicHistoryPlant {
            connection_handle,
            sender: req_tx,
            subscription_sender: sub_tx,
        }
    }
}

impl RithmicStream for RithmicHistoryPlant {
    type Handle = RithmicHistoryPlantHandle;

    fn get_handle(&self) -> RithmicHistoryPlantHandle {
        RithmicHistoryPlantHandle {
            sender: self.sender.clone(),
            subscription_receiver: self.subscription_sender.subscribe(),
            subscription_sender: self.subscription_sender.clone(),
        }
    }
}

#[derive(Debug)]
pub struct HistoryPlant {
    config: connection_info::RithmicConnectionInfo,
    interval: Interval,
    logged_in: bool,
    request_handler: RithmicRequestHandler,
    request_receiver: mpsc::Receiver<HistoryPlantCommand>,
    rithmic_reader: SplitStream<tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>>,
    rithmic_receiver_api: RithmicReceiverApi,
    rithmic_sender: SplitSink<
        tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>,
        tokio_tungstenite::tungstenite::Message,
    >,

    rithmic_sender_api: RithmicSenderApi,
    subscription_sender: broadcast::Sender<RithmicResponse>,
}

impl HistoryPlant {
    pub async fn new(
        request_receiver: mpsc::Receiver<HistoryPlantCommand>,
        subscription_sender: broadcast::Sender<RithmicResponse>,
        account_info: &AccountInfo,
    ) -> Result<HistoryPlant, ()> {
        let config = connection_info::get_config(&account_info.env);

        let ws_stream = connect_with_retry(&config.url, 15)
            .await
            .expect("failed to connect to history plant");

        let (rithmic_sender, rithmic_reader) = ws_stream.split();

        let rithmic_sender_api = RithmicSenderApi::new(account_info);
        let rithmic_receiver_api = RithmicReceiverApi {
            source: "history_plant".to_string(),
        };

        let interval = get_heartbeat_interval();

        Ok(HistoryPlant {
            config,
            interval,
            logged_in: false,
            request_handler: RithmicRequestHandler::new(),
            request_receiver,
            rithmic_reader,
            rithmic_receiver_api,
            rithmic_sender,
            rithmic_sender_api,
            subscription_sender,
        })
    }
}

#[async_trait]
impl PlantActor for HistoryPlant {
    type Command = HistoryPlantCommand;

    async fn run(&mut self) {
        loop {
            tokio::select! {
              _ = self.interval.tick() => {
                self.handle_command(HistoryPlantCommand::SendHeartbeat {}).await;
              }
              Some(message) = self.request_receiver.recv() => {
                self.handle_command(message).await;
              }
              Some(message) = self.rithmic_reader.next() => {
                let stop = self.handle_rithmic_message(message).await.unwrap();

                if stop {
                  break;
                }
              }
              else => { break; }
            }
        }
    }

    async fn handle_rithmic_message(
        &mut self,
        message: Result<Message, Error>,
    ) -> Result<bool, ()> {
        let mut stop = false;

        match message {
            Ok(Message::Close(frame)) => {
                event!(
                    Level::INFO,
                    "history_plant: Received close frame: {:?}",
                    frame
                );
                stop = true;
            }
            Ok(Message::Binary(data)) => match self.rithmic_receiver_api.buf_to_message(data) {
                Ok(response) => {
                    if response.is_update {
                        match self.subscription_sender.send(response) {
                            Ok(_) => {}
                            Err(e) => {
                                event!(
                                    Level::ERROR,
                                    "history_plant: failed to send response: {:?}",
                                    e
                                );
                            }
                        };
                    } else {
                        self.request_handler.handle_response(response)
                    }
                }
                Err(err) => {
                    event!(
                        Level::ERROR,
                        "history_plant: received an error message: {:?}",
                        err
                    );

                    self.request_handler.handle_response(err);
                }
            },
            Err(Error::ConnectionClosed) => {
                event!(Level::INFO, "history_plant: Connection closed");
                stop = true;
            }
            _ => {
                event!(
                    Level::WARN,
                    "history_plant: Unhandled message {:?}",
                    message
                );
            }
        }

        Ok(stop)
    }

    async fn handle_command(&mut self, command: HistoryPlantCommand) {
        match command {
            HistoryPlantCommand::Close => {
                self.rithmic_sender
                    .send(Message::Close(None))
                    .await
                    .unwrap();
            }
            HistoryPlantCommand::ListSystemInfo { response_sender } => {
                let (list_system_info_buf, id) =
                    self.rithmic_sender_api.request_rithmic_system_info();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(list_system_info_buf.into()))
                    .await
                    .unwrap();
            }
            HistoryPlantCommand::Login { response_sender } => {
                let (login_buf, id) = self.rithmic_sender_api.request_login(
                    &self.config.system_name,
                    SysInfraType::HistoryPlant,
                    &self.config.user,
                    &self.config.password,
                );

                event!(Level::INFO, "history_plant: sending login request {}", id);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(login_buf.into()))
                    .await
                    .unwrap();
            }
            HistoryPlantCommand::SetLogin => {
                self.logged_in = true;
            }
            HistoryPlantCommand::Logout { response_sender } => {
                let (logout_buf, id) = self.rithmic_sender_api.request_logout();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(logout_buf.into()))
                    .await
                    .unwrap();
            }
            HistoryPlantCommand::SendHeartbeat {} => {
                let (heartbeat_bf, _id) = self.rithmic_sender_api.request_heartbeat();

                let _ = self
                    .rithmic_sender
                    .send(Message::Binary(heartbeat_bf.into()))
                    .await;
            }
            HistoryPlantCommand::LoadTicks {
                exchange,
                symbol,
                start_time_sec,
                end_time_sec,
                response_sender,
            } => {
                let (tick_bar_replay_buf, id) = self.rithmic_sender_api.request_tick_bar_replay(
                    exchange,
                    symbol,
                    start_time_sec,
                    end_time_sec,
                );

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(tick_bar_replay_buf.into()))
                    .await
                    .unwrap();
            }
        }
    }
}

pub struct RithmicHistoryPlantHandle {
    sender: mpsc::Sender<HistoryPlantCommand>,
    subscription_sender: broadcast::Sender<RithmicResponse>,

    pub subscription_receiver: broadcast::Receiver<RithmicResponse>,
}

impl RithmicHistoryPlantHandle {
    /// Get the list of available systems
    ///
    /// # Returns
    /// The list of systems response or an error message
    pub async fn list_system_info(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = HistoryPlantCommand::ListSystemInfo {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    /// Log in to the Rithmic History plant
    ///
    /// This must be called before requesting historical data
    ///
    /// # Returns
    /// The login response or an error message
    pub async fn login(&self) -> Result<RithmicResponse, String> {
        event!(Level::INFO, "history_plant: logging in ");

        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = HistoryPlantCommand::Login {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        let response = rx.await.unwrap().unwrap().remove(0);

        if response.error.is_none() {
            let _ = self.sender.send(HistoryPlantCommand::SetLogin).await;

            event!(Level::INFO, "history_plant: logged in");

            Ok(response)
        } else {
            event!(
                Level::ERROR,
                "history_plant: login failed {:?}",
                response.error
            );

            Err(response.error.unwrap())
        }
    }

    /// Disconnect from the Rithmic History plant
    ///
    /// # Returns
    /// The logout response or an error message
    pub async fn disconnect(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = HistoryPlantCommand::Logout {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        let response = rx.await.unwrap().unwrap().remove(0);
        let _ = self.sender.send(HistoryPlantCommand::Close).await;

        Ok(response)
    }

    /// Load historical tick data for a specific symbol and time range
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESM1")
    /// * `exchange` - The exchange code (e.g., "CME")
    /// * `start_time_sec` - Start time in Unix timestamp (seconds)
    /// * `end_time_sec` - End time in Unix timestamp (seconds)
    ///
    /// # Returns
    /// The historical data responses or an error message
    pub async fn load_ticks(
        &self,
        symbol: String,
        exchange: String,
        start_time_sec: i32,
        end_time_sec: i32,
    ) -> Result<Vec<RithmicResponse>, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = HistoryPlantCommand::LoadTicks {
            exchange,
            symbol,
            start_time_sec,
            end_time_sec,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap())
    }
}

impl Clone for RithmicHistoryPlantHandle {
    fn clone(&self) -> Self {
        RithmicHistoryPlantHandle {
            sender: self.sender.clone(),
            subscription_receiver: self.subscription_sender.subscribe(),
            subscription_sender: self.subscription_sender.clone(),
        }
    }
}
