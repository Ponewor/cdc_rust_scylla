use std::env;
use std::thread::sleep;
use std::time::Duration;
use std::time::SystemTime;

use chrono;
use hex::decode;
use scylla::frame::value::Timestamp;
use scylla::IntoTypedRows;
use scylla::{Session, SessionBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let stream_id = match &args[1].strip_prefix("0x") {
        Some(s) => *s,
        None => &args[1],
    };
    let decoded_stream_id = decode(stream_id)?;

    const URI: &str = "127.0.0.1:9042";
    let session: Session = SessionBuilder::new().known_node(URI).build().await?;

    let y = Duration::new(3, 0);
    let confidence_interval = chrono::Duration::seconds(1);
    let past_depth = chrono::Duration::from_std(y)? + confidence_interval + confidence_interval;
    let mut maximal_timestamp = chrono::Duration::seconds(0);

    loop {
        let now =
            chrono::Duration::from_std(SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?)?;

        if let Some(rows) = session.query("SELECT pk, ck, v, system.totimestamp(\"cdc$time\") FROM ks.t_scylla_cdc_log WHERE \"cdc$stream_id\" = ? AND \"cdc$time\" >= maxTimeuuid(?) AND \"cdc$time\" < minTimeuuid(?)", (&decoded_stream_id, Timestamp(now - past_depth), Timestamp(now - confidence_interval))).await?.rows {
            for row in rows.into_typed::<(i32, i32, i32, chrono::Duration)>() {
                let (a, b, c, d) = row?;
                if d > maximal_timestamp {
                    maximal_timestamp = d;
                    println!("pk, ck, v: {}, {}, {}", a, b, c);
                }
            }
        }
        sleep(y);
    }
}
