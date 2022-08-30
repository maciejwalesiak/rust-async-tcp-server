/*
** Copyright 2022 Maciej Walesiak
**
** Licensed under the Apache License, Version 2.0 (the "License");
** you may not use this file except in compliance with the License.
** You may obtain a copy of the License at
**
**     http://www.apache.org/licenses/LICENSE-2.0

** Unless required by applicable law or agreed to in writing, software
** distributed under the License is distributed on an "AS IS" BASIS,
** WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
** See the License for the specific language governing permissions and
** limitations under the License.
*/

mod client;
mod registry;

use client::Client;

use tokio::net::TcpListener;

use registry::{Message, Registry};

use std::env;
use std::error::Error;
use std::net::{Ipv4Addr, SocketAddrV4};

use tokio::sync::broadcast;

use log::info;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "info");
    }

    env_logger::init();

    let (tx, _): (broadcast::Sender<Message>, broadcast::Receiver<Message>) =
        broadcast::channel(1024);

    let reg_tx = tx.clone();
    let reg_rx = reg_tx.subscribe();
    let mut registry = Registry::new(reg_tx, reg_rx);
    tokio::spawn(async move {
        registry.worker().await;
    });

    let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 1234);
    let listener = TcpListener::bind(&addr).await?;
    info!("server running on port {}", addr.port());

    loop {
        let (socket, socket_addr) = listener.accept().await?;

        let client_id: u16 = socket.peer_addr()?.port();
        let client_tx = tx.clone();
        let client_rx = client_tx.subscribe();
        let mut client = Client::new(client_id, socket, client_tx, client_rx);
        info!(
            "new client connected (ip: {} id: {})",
            socket_addr.ip(),
            client_id
        );

        tokio::spawn(async move {
            client.worker().await;
        });
    }
}
