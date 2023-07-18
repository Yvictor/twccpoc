use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use rsolace::solclient::{SessionProps, SolClient, SolClientError};
use rsolace::types::{SolClientLogLevel, SolClientSubscribeFlags};
use dotenv::dotenv;
use clap::Parser;


#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    #[arg(short, long, default_value_t=1)]
    compression_level: u32,
    #[arg(short='n', long)]
    client_name: Option<String>,
    #[arg(short, long, default_value_t=30)]
    execute_time: u32,
}

fn main() -> Result<(), SolClientError> {
    dotenv().ok();
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();
    let mut solclient = SolClient::new(SolClientLogLevel::Notice)?;
    let event_recv = solclient.get_event_receiver();
    static STOP: AtomicBool = AtomicBool::new(false);
    static MSG_COUNT: AtomicU64 = AtomicU64::new(0);
    static MSG_DELTA: AtomicU64 = AtomicU64::new(0);
    static MSG_BODY_SIZE: AtomicUsize = AtomicUsize::new(0);
    static MSG_BODY_SIZE_DELTA: AtomicUsize = AtomicUsize::new(0);
    std::thread::spawn(move || loop {
        match event_recv.recv() {
            Ok(event) => {
                tracing::info!("{:?}", event);
            }
            Err(e) => {
                tracing::warn!("recv msg error: {}", e);
            }
        }
        if STOP.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }
    });
    let msg_recv = solclient.get_msg_receiver();
    std::thread::spawn(move || loop {
        // solclient1.get_event_receiver();
        match msg_recv.recv() {
            Ok(msg) => {
                MSG_COUNT.fetch_add(1, Ordering::Relaxed);
                MSG_DELTA.fetch_add(1, Ordering::Relaxed);
                let buf = msg.get_binary_attachment().unwrap();
                let buf_size = buf.len();
                MSG_BODY_SIZE.fetch_add(buf_size, Ordering::Relaxed);
                MSG_BODY_SIZE_DELTA.fetch_add(buf_size, Ordering::Relaxed);
                // tracing::info!("{}", buf.len());
                // tracing::info!(
                //     "{} {} {:?}",
                //     msg.get_topic().unwrap(),
                //     msg.get_sender_time()
                //         .unwrap_or(chrono::prelude::Utc::now())
                //         .to_rfc3339(),
                //     msg.get_binary_attachment().unwrap()
                // );
            }
            Err(e) => {
                tracing::warn!("recv msg error: {}", e);
            }
        }
        if STOP.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }
    });
    let host = std::env::var("SOL_HOST").unwrap_or("210.59.255.161:80".to_string());
    let password = std::env::var("SOL_PASS").unwrap_or("shioaji".to_string());
    let clien_name = args.client_name.unwrap_or("twccpoc".to_string());
    let props = SessionProps::default()
        .vpn("sinopac")
        .username("shioaji")
        .host(&host)
        .password(&password)
        .client_name(&clien_name)
        .reapply_subscriptions(true)
        .connect_retries(1)
        .connect_timeout_ms(3000)
        .compression_level(args.compression_level);
        // .compression_level(args.compression_level);
    let r = solclient.connect(props);
    tracing::info!("connect: {}", r);
    solclient.subscribe_ext("TIC/v1/*/*/*/*", SolClientSubscribeFlags::RequestConfirm);
    solclient.subscribe_ext("QUO/v1/*/*/*/*", SolClientSubscribeFlags::RequestConfirm);

    // solclient.subscribe_ext(
    //     "TIC/v1/STK/*/TSE/*",
    //     SolClientSubscribeFlags::RequestConfirm,
    // );
    // solclient.subscribe_ext(
    //     "QUO/v1/STK/*/TSE/*",
    //     SolClientSubscribeFlags::RequestConfirm,
    // );
    for _ in 0..args.execute_time {
        std::thread::sleep(std::time::Duration::from_secs(60));
        tracing::info!(
            "msg count: {}, delta: {}, size: {}, delta: {}",
            MSG_COUNT.load(Ordering::Relaxed),
            MSG_DELTA.load(Ordering::Relaxed),
            MSG_BODY_SIZE.load(Ordering::Relaxed),
            MSG_BODY_SIZE_DELTA.load(Ordering::Relaxed),
        );
        MSG_DELTA.store(0, Ordering::Relaxed);
        MSG_BODY_SIZE_DELTA.store(0, Ordering::Relaxed);
    }
    STOP.store(true, std::sync::atomic::Ordering::Relaxed);
    Ok(())
}
