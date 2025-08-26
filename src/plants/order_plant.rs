use async_trait::async_trait;
use tracing::{Level, event};

use tokio_tungstenite::{
    MaybeTlsStream,
    tungstenite::{Error, Message},
};

use crate::{
    api::{
        receiver_api::{RithmicReceiverApi, RithmicResponse},
        rithmic_command_types::{RithmicBracketOrder, RithmicCancelOrder, RithmicModifyOrder},
        sender_api::RithmicSenderApi,
    },
    connection_info::{self, AccountInfo},
    request_handler::{RithmicRequest, RithmicRequestHandler},
    rti::{messages::RithmicMessage, request_login::SysInfraType},
    ws::{HEARTBEAT_SECS, PlantActor, RithmicStream, connect_with_retry, get_heartbeat_interval},
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

pub enum OrderPlantCommand {
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
    UpdateHeartbeat {
        seconds: u64,
    },
    AccountList {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    SubscribeOrderUpdates {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    SubscribeBracketUpdates {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    SubscribePnlUpdates {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    PlaceBracketOrder {
        bracket_order: RithmicBracketOrder,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    ModifyOrder {
        order: RithmicModifyOrder,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    ModifyStop {
        order_id: String,
        ticks: i32,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    ModifyProfit {
        order_id: String,
        ticks: i32,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    CancelOrder {
        order_id: String,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    ShowOrders {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
}

/// The RithmicOrderPlant provides functionality to manage trading orders through the Rithmic API.
///
/// It allows applications to:
/// - Place, modify and cancel orders
/// - Work with bracket orders (entry orders with profit targets and stop losses)
/// - Receive real-time order status updates
/// - Track positions and execution reports
///
/// # Example
///
/// ```no_run
/// use rithmic_rs::{
///     connection_info::{AccountInfo, RithmicConnectionSystem},
///     plants::order_plant::RithmicOrderPlant,
///     api::rithmic_command_types::{RithmicBracketOrder, OrderType, Side}
/// };
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
///     // Step 2: Create the order plant instance
///     let order_plant = RithmicOrderPlant::new(&account_info).await;
///
///     // Step 3: Get a handle to interact with the plant
///     let handle = order_plant.get_handle();
///
///     // Step 4: Login to the order plant
///     handle.login().await?;
///
///     // Step 5: Subscribe to order updates
///     handle.subscribe_order_updates().await?;
///     handle.subscribe_bracket_updates().await?;
///
///     // Step 6: Get account information
///     let accounts = handle.get_account_list().await?;
///     println!("Available accounts: {:?}", accounts);
///
///     // Step 7: Place a bracket order
///     let bracket_order = RithmicBracketOrder {
///         account: "your_account".to_string(),
///         symbol: "ESM1".to_string(),
///         exchange: "CME".to_string(),
///         quantity: 1,
///         side: Side::Buy,
///         order_type: OrderType::Limit,
///         price: 4500.00,
///         stop_ticks: 4,    // 4 ticks for stop loss
///         target_ticks: 8,  // 8 ticks for profit target
///         tif: "Day".to_string(),
///     };
///
///     let order_result = handle.place_bracket_order(bracket_order).await?;
///     println!("Order placement result: {:?}", order_result);
///
///     // Step 8: Monitor order updates
///     for _ in 0..5 {
///         match handle.subscription_receiver.recv().await {
///             Ok(update) => {
///                 match update.message {
///                     RithmicMessage::RithmicOrderNotification(u) => {}
///                     RithmicMessage::ExchangeOrderNotification(u) => {}
///                     _ => {}
///                 },
///             }
///     }
///
///     // Step 9: Disconnect when done
///     handle.disconnect().await?;
///
///     Ok(())
/// }
/// ```
pub struct RithmicOrderPlant {
    pub connection_handle: JoinHandle<()>,
    sender: mpsc::Sender<OrderPlantCommand>,
    subscription_sender: broadcast::Sender<RithmicResponse>,
}

impl RithmicOrderPlant {
    /// Create a new Order Plant connection
    ///
    /// # Arguments
    /// * `account_info` - Account credentials and environment settings
    ///
    /// # Returns
    /// A new `RithmicOrderPlant` instance connected to the Rithmic server
    pub async fn new(account_info: &AccountInfo) -> RithmicOrderPlant {
        let (req_tx, req_rx) = mpsc::channel::<OrderPlantCommand>(64);
        let (sub_tx, _sub_rx) = broadcast::channel(10_000);

        let mut order_plant = OrderPlant::new(req_rx, sub_tx.clone(), account_info)
            .await
            .unwrap();

        let connection_handle = tokio::spawn(async move {
            order_plant.run().await;
        });

        RithmicOrderPlant {
            connection_handle,
            sender: req_tx,
            subscription_sender: sub_tx,
        }
    }
}

impl RithmicStream for RithmicOrderPlant {
    type Handle = RithmicOrderPlantHandle;

    fn get_handle(&self) -> RithmicOrderPlantHandle {
        RithmicOrderPlantHandle {
            sender: self.sender.clone(),
            subscription_receiver: self.subscription_sender.subscribe(),
        }
    }
}

pub struct OrderPlant {
    config: connection_info::RithmicConnectionInfo,
    interval: Interval,
    logged_in: bool,
    request_handler: RithmicRequestHandler,
    request_receiver: mpsc::Receiver<OrderPlantCommand>,
    rithmic_reader: SplitStream<tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>>,
    rithmic_receiver_api: RithmicReceiverApi,
    rithmic_sender: SplitSink<
        tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>,
        tokio_tungstenite::tungstenite::Message,
    >,
    rithmic_sender_api: RithmicSenderApi,
    subscription_sender: broadcast::Sender<RithmicResponse>,
}

impl OrderPlant {
    pub async fn new(
        request_receiver: mpsc::Receiver<OrderPlantCommand>,
        subscription_sender: broadcast::Sender<RithmicResponse>,
        account_info: &AccountInfo,
    ) -> Result<OrderPlant, String> {
        let config = connection_info::get_config(&account_info.env);

        let ws_stream = connect_with_retry(&config.url, &config.beta_url, 15)
            .await
            .expect("failed to connect to order plant");

        let (rithmic_sender, rithmic_reader) = ws_stream.split();

        let rithmic_sender_api = RithmicSenderApi::new(account_info);
        let rithmic_receiver_api = RithmicReceiverApi {
            source: "order_plant".to_string(),
        };

        let interval = get_heartbeat_interval(None);

        Ok(OrderPlant {
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
impl PlantActor for OrderPlant {
    type Command = OrderPlantCommand;

    async fn run(&mut self) {
        loop {
            tokio::select! {
                _ = self.interval.tick() => {
                    if self.logged_in {
                        self.handle_command(OrderPlantCommand::SendHeartbeat {}).await;
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
                else => { break; }
            }
        }
    }

    async fn handle_rithmic_message(
        &mut self,
        message: Result<Message, Error>,
    ) -> Result<bool, ()> {
        let mut stop: bool = false;

        match message {
            Ok(Message::Close(frame)) => {
                event!(
                    Level::INFO,
                    "order_plant: Received close frame: {:?}",
                    frame
                );

                stop = true;
            }
            Ok(Message::Binary(data)) => match self.rithmic_receiver_api.buf_to_message(data) {
                Ok(response) => {
                    if response.is_update {
                        self.subscription_sender.send(response).unwrap();
                    } else {
                        self.request_handler.handle_response(response);
                    }
                }
                Err(err) => {
                    event!(Level::ERROR, "order_plant: response from server: {:?}", err);

                    self.request_handler.handle_response(err);
                }
            },
            Err(Error::ConnectionClosed) => {
                event!(Level::ERROR, "order_plant: Connection closed");

                stop = true;
            }
            Err(Error::Protocol(
                tokio_tungstenite::tungstenite::error::ProtocolError::ResetWithoutClosingHandshake,
            )) => {
                event!(
                    Level::ERROR,
                    "order_plant: connection reset without closing handshake"
                );

                stop = true;
            }
            _ => {
                event!(Level::WARN, "order_plant: Unhandled message: {:?}", message);
            }
        }

        Ok(stop)
    }

    async fn handle_command(&mut self, command: OrderPlantCommand) {
        match command {
            OrderPlantCommand::Close => {
                self.rithmic_sender
                    .send(Message::Close(None))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::ListSystemInfo { response_sender } => {
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
            OrderPlantCommand::Login { response_sender } => {
                let (login_buf, id) = self.rithmic_sender_api.request_login(
                    &self.config.system_name,
                    SysInfraType::OrderPlant,
                    &self.config.user,
                    &self.config.password,
                );

                event!(Level::INFO, "order_plant: sending login request {}", id);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(login_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::SetLogin => {
                self.logged_in = true;
            }
            OrderPlantCommand::Logout { response_sender } => {
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
            OrderPlantCommand::SendHeartbeat {} => {
                let (heartbeat_buf, _id) = self.rithmic_sender_api.request_heartbeat();

                let _ = self
                    .rithmic_sender
                    .send(Message::Binary(heartbeat_buf.into()))
                    .await;
            }
            OrderPlantCommand::UpdateHeartbeat { seconds } => {
                self.interval = get_heartbeat_interval(Some(seconds));
            }
            OrderPlantCommand::AccountList { response_sender } => {
                let (req_buf, id) = self.rithmic_sender_api.request_account_list();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::SubscribeOrderUpdates { response_sender } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_subscribe_for_order_updates();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::SubscribeBracketUpdates { response_sender } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_subscribe_to_bracket_updates();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::PlaceBracketOrder {
                bracket_order,
                response_sender,
            } => {
                let (req_buf, id) = self.rithmic_sender_api.request_bracket_order(bracket_order);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::ModifyOrder {
                order,
                response_sender,
            } => {
                let (req_buf, id) = self.rithmic_sender_api.request_modify_order(
                    &order.id,
                    &order.exchange,
                    &order.symbol,
                    order.qty,
                    order.price,
                    order.ordertype,
                );

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::CancelOrder {
                order_id,
                response_sender,
            } => {
                let (req_buf, id) = self.rithmic_sender_api.request_cancel_order(&order_id);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::ModifyStop {
                order_id,
                ticks,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_update_stop_bracket_level(&order_id, ticks);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::ModifyProfit {
                order_id,
                ticks,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_update_target_bracket_level(&order_id, ticks);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::ShowOrders { response_sender } => {
                let (req_buf, id) = self.rithmic_sender_api.request_show_orders();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            _ => {}
        };
    }
}

pub struct RithmicOrderPlantHandle {
    sender: mpsc::Sender<OrderPlantCommand>,
    pub subscription_receiver: broadcast::Receiver<RithmicResponse>,
}

impl RithmicOrderPlantHandle {
    /// Get the list of available systems
    ///
    /// # Returns
    /// The list of systems response or an error message
    pub async fn list_system_info(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::ListSystemInfo {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    /// Log in to the Rithmic Order plant
    ///
    /// This must be called before sending orders or subscriptions
    ///
    /// # Returns
    /// The login response or an error message
    pub async fn login(&self) -> Result<RithmicResponse, String> {
        event!(Level::INFO, "order_plant: logging in");

        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::Login {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        let response = rx.await.unwrap().unwrap().remove(0);

        if response.error.is_none() {
            let _ = self.sender.send(OrderPlantCommand::SetLogin).await;

            if let RithmicMessage::ResponseLogin(resp) = &response.message {
                if let Some(hb) = resp.heartbeat_interval {
                    let secs = hb.max(HEARTBEAT_SECS as f64) as u64;
                    self.update_heartbeat(secs).await;
                }

                if let Some(session_id) = &resp.unique_user_id {
                    event!(Level::INFO, "order_plant: session id: {}", session_id);
                }
            }

            event!(Level::INFO, "order_plant: logged in");

            Ok(response)
        } else {
            event!(
                Level::ERROR,
                "order_plant: login failed {:?}",
                response.error
            );

            Err(response.error.unwrap())
        }
    }

    /// Disconnect from the Rithmic Order plant
    ///
    /// # Returns
    /// The logout response or an error message
    pub async fn disconnect(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::Logout {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        let mut r = rx.await.unwrap().unwrap();
        let _ = self.sender.send(OrderPlantCommand::Close).await;

        Ok(r.remove(0))
    }

    /// Get a list of available trading accounts
    ///
    /// # Returns
    /// The account list response or an error message
    pub async fn get_account_list(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::AccountList {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    /// Subscribe to order status updates
    ///
    /// # Returns
    /// The subscription response or an error message
    pub async fn subscribe_order_updates(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::SubscribeOrderUpdates {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    /// Subscribe to bracket order status updates
    ///
    /// # Returns
    /// The subscription response or an error message
    pub async fn subscribe_bracket_updates(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::SubscribeBracketUpdates {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    /// Place a bracket order (entry order with profit target and stop loss)
    ///
    /// # Arguments
    /// * `bracket_order` - The bracket order parameters
    ///
    /// # Returns
    /// The order placement responses or an error message
    pub async fn place_bracket_order(
        &self,
        bracket_order: RithmicBracketOrder,
    ) -> Result<Vec<RithmicResponse>, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::PlaceBracketOrder {
            bracket_order,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.unwrap()
    }

    /// Modify an existing order
    ///
    /// # Arguments
    /// * `order` - The order parameters to modify
    ///
    /// # Returns
    /// The order modification response or an error message
    pub async fn modify_order(&self, order: RithmicModifyOrder) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::ModifyOrder {
            order,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    /// Cancel an order
    ///
    /// # Arguments
    /// * `order` - The cancel order parameters
    ///
    /// # Returns
    /// The cancellation response or an error message
    pub async fn cancel_order(&self, order: RithmicCancelOrder) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::CancelOrder {
            order_id: order.id,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    /// Adjust the profit target level of a bracket order
    ///
    /// # Arguments
    /// * `id` - The order ID
    /// * `ticks` - Number of ticks to adjust the profit target
    ///
    /// # Returns
    /// The adjustment response or an error message
    pub async fn adjust_profit(&self, id: &str, ticks: i32) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::ModifyProfit {
            order_id: id.to_string(),
            ticks,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    /// Adjust the stop loss level of a bracket order
    ///
    /// # Arguments
    /// * `id` - The order ID
    /// * `ticks` - Number of ticks to adjust the stop loss
    ///
    /// # Returns
    /// The adjustment response or an error message
    pub async fn adjust_stop(&self, id: &str, ticks: i32) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::ModifyStop {
            order_id: id.to_string(),
            ticks,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    /// Request a list of all open orders
    ///
    /// # Returns
    /// The order list response or an error message
    pub async fn show_orders(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::ShowOrders {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    async fn update_heartbeat(&self, seconds: u64) {
        let command = OrderPlantCommand::UpdateHeartbeat { seconds };

        let _ = self.sender.send(command).await;
    }
}
