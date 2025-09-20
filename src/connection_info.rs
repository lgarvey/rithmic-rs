use dotenv::dotenv;
use std::{env, fmt, str::FromStr};

#[derive(Clone, Debug)]
pub struct RithmicConnectionInfo {
    pub url: String,
    pub beta_url: String,
    pub user: String,
    pub password: String,
    pub system_name: String,
}

#[derive(Clone, Debug)]
pub enum RithmicConnectionSystem {
    Demo,
    Live,
    Test,
}

impl fmt::Display for RithmicConnectionSystem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RithmicConnectionSystem::Demo => write!(f, "demo"),
            RithmicConnectionSystem::Live => write!(f, "live"),
            RithmicConnectionSystem::Test => write!(f, "test"),
        }
    }
}

impl FromStr for RithmicConnectionSystem {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "demo" | "development" => Ok(RithmicConnectionSystem::Demo),
            "live" | "production" => Ok(RithmicConnectionSystem::Live),
            "test" => Ok(RithmicConnectionSystem::Test),
            _ => Err(()),
        }
    }
}

pub fn get_config(env: &RithmicConnectionSystem) -> RithmicConnectionInfo {
    dotenv().ok();

    match env {
        RithmicConnectionSystem::Demo => RithmicConnectionInfo {
            url: "wss://rprotocol.rithmic.com:443".into(),
            beta_url: "wss://rprotocol-beta.rithmic.com:443".into(),
            user: env::var("RITHMIC_DEMO_USER").unwrap(),
            password: env::var("RITHMIC_DEMO_PW").unwrap(),
            system_name: "Rithmic Paper Trading".into(),
        },
        RithmicConnectionSystem::Live => RithmicConnectionInfo {
            url: "wss://rprotocol.rithmic.com:443".into(),
            beta_url: "wss://rprotocol-beta.rithmic.com:443".into(),
            user: env::var("RITHMIC_LIVE_USER").unwrap(),
            password: env::var("RITHMIC_LIVE_PW").unwrap(),
            system_name: "Rithmic 01".into(),
        },
        RithmicConnectionSystem::Test => RithmicConnectionInfo {
            url: "wss://rituz00100.rithmic.com:443".into(),
            beta_url: "wss://rprotocol-beta.rithmic.com:443".into(),
            user: env::var("RITHMIC_TEST_USER").unwrap(),
            password: env::var("RITHMIC_TEST_PW").unwrap(),
            system_name: "Rithmic Test".into(),
        },
    }
}

#[derive(Clone, Debug)]
pub struct AccountInfo {
    pub account_id: String,
    pub env: RithmicConnectionSystem,
    pub fcm_id: String,
    pub ib_id: String,
}
