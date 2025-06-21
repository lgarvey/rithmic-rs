use std::env;
use tracing::{Level, event};

use rithmic_rs::{
    RithmicHistoryPlant,
    connection_info::{AccountInfo, RithmicConnectionSystem},
    rti::{messages::RithmicMessage, request_time_bar_replay::BarType},
    ws::RithmicStream,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Before running this example, copy .env.blank to .env
    // and fill in RITHMIC_ACCOUNT_ID, FCM_ID, and IB_ID
    dotenv::dotenv().ok();

    tracing_subscriber::fmt().init();

    let account_id = env::var("RITHMIC_ACCOUNT_ID")
        .expect("RITHMIC_ACCOUNT_ID must be set in environment variables");

    let fcm_id = env::var("FCM_ID").expect("RITHMIC_FCM_ID must be set in environment variables");
    let ib_id = env::var("IB_ID").expect("RITHMIC_IB_ID must be set in environment variables");

    let account_info = AccountInfo {
        account_id,
        env: RithmicConnectionSystem::Demo,
        fcm_id,
        ib_id,
    };

    let history_plant = RithmicHistoryPlant::new(&account_info).await;
    let handle = history_plant.get_handle();

    handle.login().await?;

    // Adjust symbol and time range to match
    let symbol = "ESU5".to_string(); // Example: ES December 2024 contract
    let exchange = "CME".to_string();
    let start_time = 1750370400;
    let end_time = 1750453200;

    event!(
        Level::INFO,
        "Loading 5-minute bars for {} from {} to {}",
        symbol,
        start_time,
        end_time
    );

    // Load 5-minute time bars
    let five_min_bars = handle
        .load_time_bars(
            symbol.clone(),
            exchange.clone(),
            BarType::MinuteBar,
            5, // 5-minute bars
            start_time,
            end_time,
        )
        .await?;

    event!(
        Level::INFO,
        "Received {} 5-minute bar responses",
        five_min_bars.len()
    );

    // Process the 5-minute bar responses
    for r in five_min_bars.iter() {
        match &r.message {
            RithmicMessage::ResponseTimeBarReplay(bar_message) => {
                event!(Level::INFO, "5-minute bar: {:#?}", bar_message);
            }
            _ => {
                event!(Level::WARN, "Received unexpected message type");
            }
        }
    }

    let _ = handle.disconnect().await;

    event!(Level::INFO, "Disconnected from Rithmic History Plant");

    Ok(())
}
