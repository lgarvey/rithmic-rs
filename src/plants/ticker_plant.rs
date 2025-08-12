use async_trait::async_trait;
use tracing::{Level, event};

use crate::{
    api::{
        receiver_api::{RithmicReceiverApi, RithmicResponse},
        sender_api::RithmicSenderApi,
    },
    connection_info::{self, AccountInfo},
    request_handler::{RithmicRequest, RithmicRequestHandler},
    rti::{
        request_depth_by_order_updates,
        request_login::SysInfraType,
        request_market_data_update::{Request, UpdateBits},
    },
    ws::{PlantActor, RithmicStream, connect_with_retry, get_heartbeat_interval},
};

use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};

use tokio_tungstenite::{
    MaybeTlsStream,
    tungstenite::{Error, Message},
};

use tokio::{
    net::TcpStream,
    sync::{broadcast, mpsc, oneshot},
    time::Interval,
};

pub enum TickerPlantCommand {
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
    Subscribe {
        symbol: String,
        exchange: String,
        fields: Vec<UpdateBits>,
        request_type: Request,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    SubscribeOrderBook {
        symbol: String,
        exchange: String,
        request_type: request_depth_by_order_updates::Request,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    RequestDepthByOrderSnapshot {
        symbol: String,
        exchange: String,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
}

/// The RithmicTickerPlant provides access to real-time market data.
///
/// Currently the following market data updates are supported:
/// - Last trades
/// - Best bid and offer (BBO)
///
/// * NOTE: Feel free to add missing ones and contribute back.
///
/// # Example
///
/// ```no_run
/// use rithmic_rs::{connection_info::AccountInfo, plants::ticker_plant::RithmicTickerPlant};
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
///     // Step 2: Create the ticker plant instance
///     let ticker_plant = RithmicTickerPlant::new(&account_info).await;
///
///     // Step 3: Get a handle to interact with the plant
///     let handle = ticker_plant.get_handle();
///
///     // Step 4: Login to the ticker plant
///     handle.login().await?;
///
///     // Step 5: Subscribe to market data for a symbol
///     let subscription_result = handle.subscribe("ESM1", "CME").await?;
///     println!("Subscription successful: {:?}", subscription_result);
///
///     // Step 6: Process incoming market data updates
///     for _ in 0..10 {
///         match handle.subscription_receiver.recv().await {
///             Ok(update) => {
///                 match update.message {
///                     RithmicMessage::LastTrade(u) => {}
///                     RithmicMessage::BestBidOffer(u) => {}
///                     _ => {}
///                 },
///             }
///             Err(e) => println!("Error receiving update: {}", e),
///         }
///     }
///
///     // Step 7: Disconnect when done
///     handle.disconnect().await?;
///
///     Ok(())
/// }
/// ```
pub struct RithmicTickerPlant {
    pub connection_handle: tokio::task::JoinHandle<()>,
    sender: mpsc::Sender<TickerPlantCommand>,
    subscription_sender: broadcast::Sender<RithmicResponse>,
}

impl RithmicTickerPlant {
    /// Creates a new Ticker Plant connection to access real-time market data.
    ///
    /// # Arguments
    /// * `account_info` - Account credentials and environment settings
    ///
    /// # Returns
    /// A new `RithmicTickerPlant` instance connected to the Rithmic server
    pub async fn new(account_info: &AccountInfo) -> RithmicTickerPlant {
        let (req_tx, req_rx) = mpsc::channel::<TickerPlantCommand>(64);
        let (sub_tx, _sub_rx) = broadcast::channel(10_000);

        let mut ticker_plant = TickerPlant::new(req_rx, sub_tx.clone(), account_info)
            .await
            .unwrap();

        let connection_handle = tokio::spawn(async move {
            ticker_plant.run().await;
        });

        RithmicTickerPlant {
            connection_handle,
            sender: req_tx,
            subscription_sender: sub_tx,
        }
    }
}

impl RithmicStream for RithmicTickerPlant {
    type Handle = RithmicTickerPlantHandle;

    fn get_handle(&self) -> RithmicTickerPlantHandle {
        RithmicTickerPlantHandle {
            sender: self.sender.clone(),
            subscription_sender: self.subscription_sender.clone(),
            subscription_receiver: self.subscription_sender.subscribe(),
        }
    }
}

#[derive(Debug)]
pub struct TickerPlant {
    config: connection_info::RithmicConnectionInfo,
    interval: Interval,
    logged_in: bool,
    request_handler: RithmicRequestHandler,
    request_receiver: mpsc::Receiver<TickerPlantCommand>,
    rithmic_reader: SplitStream<tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>>,
    rithmic_receiver_api: RithmicReceiverApi,
    rithmic_sender: SplitSink<
        tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>,
        tokio_tungstenite::tungstenite::Message,
    >,

    rithmic_sender_api: RithmicSenderApi,
    subscription_sender: broadcast::Sender<RithmicResponse>,
}

impl TickerPlant {
    async fn new(
        request_receiver: mpsc::Receiver<TickerPlantCommand>,
        subscription_sender: broadcast::Sender<RithmicResponse>,
        account_info: &AccountInfo,
    ) -> Result<TickerPlant, ()> {
        let config = connection_info::get_config(&account_info.env);

        let ws_stream = connect_with_retry(&config.url, &config.beta_url, 15)
            .await
            .expect("failed to connect to ticker plant");

        let (rithmic_sender, rithmic_reader) = ws_stream.split();

        let rithmic_sender_api = RithmicSenderApi::new(account_info);
        let rithmic_receiver_api = RithmicReceiverApi {
            source: "ticker_plant".to_string(),
        };

        let interval = get_heartbeat_interval();

        Ok(TickerPlant {
            config,
            interval,
            logged_in: false,
            request_handler: RithmicRequestHandler::new(),
            request_receiver,
            rithmic_reader,
            rithmic_receiver_api,
            rithmic_sender_api,
            rithmic_sender,
            subscription_sender,
        })
    }
}

#[async_trait]
impl PlantActor for TickerPlant {
    type Command = TickerPlantCommand;

    /// Execute the ticker plant in its own thread
    /// We will listen for messages from request_receiver and forward them to Rithmic
    /// while also listening for messages from Rithmic and forwarding them to subscription_sender
    /// or request handler
    async fn run(&mut self) {
        loop {
            tokio::select! {
                _ = self.interval.tick() => {
                    if self.logged_in {
                        self.handle_command(TickerPlantCommand::SendHeartbeat {}).await;
                    }
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
                else => { break }
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
                    "ticker_plant received close frame: {:?}",
                    frame
                );

                stop = true;
            }
            Ok(Message::Binary(data)) => {
                let response = self.rithmic_receiver_api.buf_to_message(data).unwrap();

                if response.is_update {
                    self.subscription_sender.send(response).unwrap();
                } else {
                    self.request_handler.handle_response(response);
                }
            }
            Err(Error::ConnectionClosed) => {
                event!(Level::INFO, "ticker_plant connection closed");

                stop = true;
            }
            _ => {
                event!(
                    Level::WARN,
                    "ticker_plant received unknown message {:?}",
                    message
                );
            }
        }

        Ok(stop)
    }

    async fn handle_command(&mut self, command: TickerPlantCommand) {
        match command {
            TickerPlantCommand::Close => {
                self.rithmic_sender
                    .send(Message::Close(None))
                    .await
                    .unwrap();
            }
            TickerPlantCommand::ListSystemInfo { response_sender } => {
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
            TickerPlantCommand::Login { response_sender } => {
                let (login_buf, id) = self.rithmic_sender_api.request_login(
                    &self.config.system_name,
                    SysInfraType::TickerPlant,
                    &self.config.user,
                    &self.config.password,
                );

                event!(Level::INFO, "ticker_plant: sending login request {}", id);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(login_buf.into()))
                    .await
                    .unwrap();
            }
            TickerPlantCommand::SetLogin => {
                self.logged_in = true;
            }
            TickerPlantCommand::Logout { response_sender } => {
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
            TickerPlantCommand::SendHeartbeat {} => {
                let (heartbeat_buf, _id) = self.rithmic_sender_api.request_heartbeat();

                let _ = self
                    .rithmic_sender
                    .send(Message::Binary(heartbeat_buf.into()))
                    .await;
            }
            TickerPlantCommand::Subscribe {
                symbol,
                exchange,
                fields,
                request_type,
                response_sender,
            } => {
                let (sub_buf, id) = self.rithmic_sender_api.request_market_data_update(
                    &symbol,
                    &exchange,
                    fields,
                    request_type,
                );

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(sub_buf.into()))
                    .await
                    .unwrap();
            }
            TickerPlantCommand::SubscribeOrderBook {
                symbol,
                exchange,
                request_type,
                response_sender,
            } => {
                let (sub_buf, id) = self.rithmic_sender_api.request_depth_by_order_update(
                    &symbol,
                    &exchange,
                    request_type,
                );

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(sub_buf.into()))
                    .await
                    .unwrap();
            }
            TickerPlantCommand::RequestDepthByOrderSnapshot {
                symbol,
                exchange,
                response_sender,
            } => {
                let (snapshot_buf, id) = self
                    .rithmic_sender_api
                    .request_depth_by_order_snapshot(&symbol, &exchange);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(snapshot_buf.into()))
                    .await
                    .unwrap();
            }
        }
    }
}

pub struct RithmicTickerPlantHandle {
    sender: mpsc::Sender<TickerPlantCommand>,
    subscription_sender: broadcast::Sender<RithmicResponse>,

    /// Receiver for subscription updates
    pub subscription_receiver: broadcast::Receiver<RithmicResponse>,
}

impl RithmicTickerPlantHandle {
    pub async fn list_system_info(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = TickerPlantCommand::ListSystemInfo {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        let response = rx.await.unwrap().unwrap().remove(0);

        Ok(response)
    }

    /// Log in to the Rithmic ticker plant
    ///
    /// This must be called before subscribing to any market data
    ///
    /// # Returns
    /// The login response or an error message
    pub async fn login(&self) -> Result<RithmicResponse, String> {
        event!(Level::INFO, "ticker_plant: logging in");

        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = TickerPlantCommand::Login {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        let response = rx.await.unwrap().unwrap().remove(0);

        if response.error.is_none() {
            let _ = self.sender.send(TickerPlantCommand::SetLogin).await;

            event!(Level::INFO, "ticker_plant: logged in");

            Ok(response)
        } else {
            event!(
                Level::ERROR,
                "ticker_plant: login failed {:?}",
                response.error
            );

            Err(response.error.unwrap())
        }
    }

    /// Disconnect from the Rithmic ticker plant
    ///
    /// # Returns
    /// The logout response or an error message
    pub async fn disconnect(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = TickerPlantCommand::Logout {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        let mut r = rx.await.unwrap().unwrap();
        let _ = self.sender.send(TickerPlantCommand::Close).await;
        let response = r.remove(0);

        self.subscription_sender.send(response.clone()).unwrap();

        Ok(response)
    }

    /// Subscribe to market data for a specific symbol
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESM1")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The subscription response or an error message
    pub async fn subscribe(&self, symbol: &str, exchange: &str) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = TickerPlantCommand::Subscribe {
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            fields: vec![UpdateBits::LastTrade, UpdateBits::Bbo],
            request_type: Request::Subscribe,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    /// Subscribe to order book depth-by-order updates for a specific symbol
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESM1")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The subscription response or an error message
    pub async fn subscribe_order_book(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = TickerPlantCommand::SubscribeOrderBook {
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            request_type: request_depth_by_order_updates::Request::Subscribe,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    /// Request a snapshot of the order book depth-by-order for a specific symbol
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESM1")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// A vector of responses containing the order book snapshot data or an error message
    pub async fn request_depth_by_order_snapshot(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<Vec<RithmicResponse>, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = TickerPlantCommand::RequestDepthByOrderSnapshot {
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.unwrap()
    }
}

impl Clone for RithmicTickerPlantHandle {
    fn clone(&self) -> Self {
        RithmicTickerPlantHandle {
            sender: self.sender.clone(),
            subscription_receiver: self.subscription_sender.subscribe(),
            subscription_sender: self.subscription_sender.clone(),
        }
    }
}
