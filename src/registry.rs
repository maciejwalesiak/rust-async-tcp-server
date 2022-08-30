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

use log::{error, info};

use std::fmt;

use tokio::sync::broadcast;

pub const MSG_BROADCAST_ID: u16 = 0xFFFF;
pub const MSG_REGISTRY_ID: u16 = 0;

#[derive(Clone)]
pub struct Message {
    src_id: u16,
    dst_id: u16,
    data: Vec<u8>,
}

impl Message {
    pub fn new(src_id: u16, dst_id: u16, data: &[u8]) -> Option<Message> {
        Some(Message {
            src_id,
            dst_id,
            data: data.to_vec(),
        })
    }

    pub fn src_id(&self) -> u16 {
        self.src_id
    }

    pub fn dst_id(&self) -> u16 {
        self.dst_id
    }
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}:{} {}",
            self.src_id,
            self.dst_id,
            String::from_utf8_lossy(&self.data[..])
        )
    }
}

pub struct Registry {
    tx: broadcast::Sender<Message>,
    rx: broadcast::Receiver<Message>,
}

impl Registry {
    pub fn new(tx: broadcast::Sender<Message>, rx: broadcast::Receiver<Message>) -> Registry {
        Registry { tx, rx }
    }

    fn ack_msg(&mut self, msg: &Message) {
        let res = Message {
            src_id: MSG_REGISTRY_ID,
            dst_id: msg.src_id(),
            data: "ACK".as_bytes().to_vec(),
        };
        if self.tx.send(res).is_err() {
            error!("failed to send message");
        }
    }

    fn broadcast(&mut self, msg: &Message) {
        let res = Message {
            src_id: MSG_REGISTRY_ID,
            dst_id: MSG_BROADCAST_ID,
            data: format!("received message from {}", msg.src_id)
                .as_bytes()
                .to_vec(),
        };
        if self.tx.send(res).is_err() {
            error!("failed to send message");
        }
    }

    fn process_msg(&mut self, msg: &Message) {
        info!("processing msg {}", msg);

        //TODO: implement message processing logic here
        self.ack_msg(msg);
        self.broadcast(msg);
    }

    pub async fn worker(&mut self) {
        loop {
            let msg = self.rx.recv().await.unwrap();
            if msg.dst_id() == MSG_REGISTRY_ID {
                self.process_msg(&msg);
            }
        }
    }
}
