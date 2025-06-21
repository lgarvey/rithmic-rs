use std::env;
use tracing::{Level, event};

use rithmic_rs::{
    RithmicHistoryPlant,
    connection_info::{AccountInfo, RithmicConnectionSystem},
    rti::messages::RithmicMessage,
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
        "Loading ticks for {} from {} to {}",
        symbol,
        start_time,
        end_time
    );

    // Rithmic only returns 10_000 ticks at a time, and note that there can be several ticks sharing the same timestamp
    let tick_responses = handle
        .load_ticks(symbol.clone(), exchange, start_time, end_time)
        .await?;

    event!(
        Level::INFO,
        "Received {} tick responses",
        tick_responses.len()
    );

    // Process the tick responses
    for r in tick_responses.iter() {
        match &r.message {
            RithmicMessage::ResponseTickBarReplay(tick_message) => {
                event!(Level::INFO, "Tick: {:#?}", tick_message);
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
