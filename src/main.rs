use std::cmp::{max, min};
use std::env;
use std::time::{Duration, SystemTime};

use chrono;
use futures::stream::StreamExt;
use hex::decode;
use scylla::frame::value::Timestamp;
use scylla::{Session, SessionBuilder};
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let stream_id = args[1].strip_prefix("0x").unwrap_or(&args[1]);
    let decoded_stream_id = decode(stream_id)?;

    const URI: &str = "127.0.0.1:9042";
    let session: Session = SessionBuilder::new().known_node(URI).build().await?;

    let mut t =
        chrono::Duration::from_std(SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?)?;
    let sleep_interval = Duration::new(1, 0);
    let window_period = chrono::Duration::seconds(2);
    let confidence_interval = chrono::Duration::seconds(1);

    let query_base = session.prepare("SELECT pk, ck, v FROM ks.t_scylla_cdc_log WHERE \"cdc$stream_id\" = ? AND \"cdc$time\" >= maxTimeuuid(?) AND \"cdc$time\" < minTimeuuid(?)").await?;

    loop {
        let now =
            chrono::Duration::from_std(SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?)?;

        let u = max(t, min(t + window_period, now - confidence_interval));

        let mut rows_stream = session
            .execute_iter(
                query_base.clone(),
                (&decoded_stream_id, Timestamp(t), Timestamp(u)),
            )
            .await?
            .into_typed::<(i32, i32, i32)>();
        while let Some(row) = rows_stream.next().await {
            let (a, b, c) = row?;
            println!("pk, ck, v: {}, {}, {}", a, b, c);
        }
        t = u;
        sleep(sleep_interval).await;
    }
}
