# Rust Rithmic R | Protocol API client

Unofficial rust client for connecting to Rithmic's R | Protocol API.

[Documentation](https://docs.rs/rithmic-rs/latest/rithmic_rs/) | [Rithmic APIs](https://www.rithmic.com/apis)

_rithmic protocol version: 0.84.0.0_

Not all functionality has been implemented, but this is currently being used to trade live capital through Rithmic.

Only `order_plant`, `ticker_plant`, `pnl_plant`, `history_plant` are provided. Each plant uses the actor pattern so you'll want to start a plant, and communicate / call commands with it using it's handle. The crate is setup to be used with tokio channels.

The `history_plant` supports loading both historical tick data and time bar data (1-second, 1-minute, 5-minute, daily, and weekly bars).

## Installation

You can install it from crates.io

```
$ cargo add rithmic-rs
```

Or manually add it to your `Cargo.toml` file.


```
[dependencies]
rithmic-rs = "0.4.0"
```

## Usage

Store your credentials in a `.env` file.

```sh
# .env
RITHMIC_TEST_USER=<USER_NAME>
RITHMIC_TEST_PW=<PASSWORD>

RITHMIC_DEMO_USER=<USER_NAME>
RITHMIC_DEMO_PW=<PASSWORD>

RITHMIC_LIVE_USER=<USER_NAME>
RITHMIC_LIVE_PW=<PASSWORD>
```

Rithmic supports three types of account environments, `RithmicConnectionSystem::Demo` is used for paper trading, `RithmicConnectionSystem::Live` will connect to your funded account, and `RithmicConnectionSystem::Test` connects to the test environment before your app is approved.

To use this crate, pass in your account information to one of the plants. Doing so will spawn an actor in a new thread that listens to commands that you send via a handle. Some plants like the ticker plant will also include a broadcast channel that you can listen to for wire level updates.

```rust
pub async fn stream_live_ticks(
    &self,
    env: &RithmicConnectionSystem,
    account_info: &AccountInfo
) -> Result<(), Box<dyn std::error::Error>> {
    event!(Level::INFO, "market-data streaming ticks");

    let ticker_plant = RithmicTickerPlant::new(env, account_info).await;
    let ticker_plant_handle = ticker_plant.get_handle();

    let mut min_backoff_wait = 1;

    while let Err(err) = ticker_plant_handle.login().await {
        event!(Level::ERROR, "market-data: login failed: {}", err);

        sleep(Duration::from_secs(min_backoff_wait)).await;

        min_backoff_wait *= 2;

        if min_backoff_wait > 60 {
            event!(Level::ERROR, "market-data: login exceeded max backoff");

            panic!("market-data: login exceeded max backoff")
        }
    }

    handle.subscribe("ESM5", "CME").await?;

    loop {
        let message = ticker_plant_handle.subscription_receiver.recv().await;

        match message {
            Ok(update) => {
                match update.message {
                    RithmicMessage::LastTrade(u) => {
                        let tick = Tick {
                            dir: u.aggressor.unwrap(),
                            price: u.trade_price.unwrap(),
                            vol: u.trade_size.unwrap(),
                            utime: u.ssboe.unwrap() as i64 * 1_000_000 + u.usecs.unwrap() as i64
                        };

                        if let Some(s) = self.stream_map.get(&u.symbol.unwrap()) {
                            if let Err(e) = s.send(tick) {
                                event!(Level::ERROR, "market-data: failed to send tick: {}", e);
                            };
                        }
                    }
                    _ => {}
                }
            }
            Err(RecvError::Lagged(count)) => {
                event!(Level::WARN, "{} messages lagged", count);
            }
            Err(err) => {
                event!(Level::ERROR, "received error {:?}", err);

                break;
            }
        }
    }
}
```

## Examples

The repository includes several examples to help you get started:

**Environment Variables**

Before running examples, copy `.env.blank` to `.env` and fill in your credentials:
```bash
cp examples/.env.blank .env
# Edit .env with your Rithmic credentials
```

### Basic Connection
```bash
cargo run --example connect
```

### Historical Data
```bash
# Load historical tick data
cargo run --example load_historical_ticks

# Load historical time bars (1-minute, 5-minute, daily)
cargo run --example load_historical_bars
```

## Contribution

Contributions encouraged and welcomed!

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
