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

use crate::registry::{MSG_BROADCAST_ID, MSG_REGISTRY_ID, Message};

use log::{error, info};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::broadcast;

pub struct Client {
    id: u16,
    socket: TcpStream,
    tx: broadcast::Sender<Message>,
    rx: broadcast::Receiver<Message>,
}

impl Client {
    pub fn new(
        id: u16,
        socket: TcpStream,
        tx: broadcast::Sender<Message>,
        rx: broadcast::Receiver<Message>,
    ) -> Client {
        Client { id, socket, tx, rx }
    }

    pub async fn worker(&mut self) {
        let mut data = vec![0; 1024];

        loop {
            let mut incoming_msg: Option<Message> = None;
            let mut outgoing_msg: Option<Message> = None;

            tokio::select! {
                n = self.socket.read(&mut data) => {
                    match n {
                        Ok(0) => {
                            info!("closed connection ({})", self.id);
                            return;
                        }
                        Ok(n) => {
                            incoming_msg = Some(Message::new(self.id, MSG_REGISTRY_ID, &data[0..n]));
                        }
                        Err(err) => {
                            error!("broken connection ({}): {err}", self.id);
                            return;
                        }
                    }
                }
                msg = self.rx.recv() => {
                    match msg {
                        Ok(msg) => {
                            if msg.dst_id() == self.id || msg.dst_id() == MSG_BROADCAST_ID {
                                outgoing_msg = Some(msg);
                            }
                        }
                        Err(_) => {
                            error!("internal processing error ({})", self.id);
                            return;
                        }
                    }
                }
            }

            if let Some(incoming_msg) = incoming_msg {
                if let Err(err) = self.tx.send(incoming_msg) {
                    error!("failed to register message ({}): {err}", self.id);
                }
            }

            if let Some(outgoing_msg) = outgoing_msg {
                let res_str = outgoing_msg.to_string() + "\n";
                self.socket
                    .write_all(res_str.as_bytes())
                    .await
                    .expect("failed to write data to socket");
            }
        }
    }
}
