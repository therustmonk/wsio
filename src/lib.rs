#![recursion_limit = "512"]

use failure::{format_err, Error};
use futures::channel::{mpsc, oneshot};
use futures::compat::{Future01CompatExt, Sink01CompatExt, Stream01CompatExt};
use futures::{select, SinkExt, StreamExt};
use futures_legacy::{Sink as LegacySink, Stream as LegacyStream};
use rand::Rng;
use runtime::time::Interval;
use std::fmt::Debug;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio_tungstenite::{accept_async, connect_async};
use tungstenite::{Error as WsError, Message};
use url::Url;

pub trait Protocol: Debug + Clone + Send + 'static {
    type In: Send;
    type Out: Send;

    fn do_ping(&self) -> bool;
    fn force_text(&self) -> bool;
    fn encode(&self, item: &Self::In) -> Result<Vec<u8>, Error>;
    fn decode(&self, data: &[u8]) -> Result<Self::Out, Error>;
}

#[derive(Debug)]
pub struct WsIo<P: Protocol> {
    to_ws_tx: mpsc::Sender<P::In>,
    from_ws_rx: mpsc::Receiver<P::Out>,
}

impl<P: Protocol> WsIo<P> {
    pub fn client(protocol: P, uri: &str) -> oneshot::Receiver<WsIo<P>> {
        log::debug!("Connect WebSocket client: {}.", uri);
        let (client_tx, client_rx) = oneshot::channel();
        let uri = uri.to_owned();
        runtime::spawn(async move {
            if let Err(err) = WsIo::client_impl(protocol, uri, client_tx).await {
                log::error!("WsIo client failed: {}.", err);
            }
        });
        client_rx
    }

    async fn client_impl(
        protocol: P,
        uri: String,
        client_tx: oneshot::Sender<WsIo<P>>,
    ) -> Result<(), Error> {
        let url = Url::parse(&uri)?;
        let (ws_stream, _) = connect_async(url).compat().await?;
        let wsio = WsIoInteraction::from_stream(protocol, ws_stream);
        client_tx
            .send(wsio)
            .map_err(|_| format_err!("Can't send WsIo client"))?;
        Ok(())
    }

    pub fn server(protocol: P, addr: &str) -> mpsc::Receiver<WsIo<P>> {
        log::debug!("Bind WebSocket server to: {}.", addr);
        let (clients_tx, clients_rx) = mpsc::channel(8);
        let addr = addr.to_owned();
        runtime::spawn(async move {
            if let Err(err) = WsIo::server_impl(protocol, addr, clients_tx).await {
                log::error!("WsIo server failed: {}.", err);
            }
        });
        clients_rx
    }

    async fn server_impl(
        protocol: P,
        addr: String,
        mut clients_tx: mpsc::Sender<WsIo<P>>,
    ) -> Result<(), Error> {
        let addr = addr.parse()?;
        let socket = TcpListener::bind(&addr)?;
        let mut incoming = socket.incoming().compat();
        while let Some(tcp_stream) = incoming.next().await.transpose()? {
            let ws_stream = accept_async(tcp_stream).compat().await?;
            let wsio = WsIoInteraction::from_stream(protocol.clone(), ws_stream);
            clients_tx.send(wsio).await?;
        }
        Ok(())
    }

    pub fn tx(&mut self) -> &mut mpsc::Sender<P::In> {
        &mut self.to_ws_tx
    }

    pub fn rx(&mut self) -> &mut mpsc::Receiver<P::Out> {
        &mut self.from_ws_rx
    }
}

struct WsIoInteraction<T, P: Protocol> {
    protocol: P,
    ws_stream: T,
    to_ws_rx: mpsc::Receiver<P::In>,
    from_ws_tx: mpsc::Sender<P::Out>,
}

impl<T, P: Protocol> WsIoInteraction<T, P>
where
    T: Send + 'static,
    T: LegacyStream<Item = Message, Error = WsError>,
    T: LegacySink<SinkItem = Message, SinkError = WsError>,
{
    fn from_stream(protocol: P, ws_stream: T) -> WsIo<P> {
        let (from_ws_tx, from_ws_rx) = mpsc::channel(8);
        let (to_ws_tx, to_ws_rx) = mpsc::channel(8);
        let wsio = WsIo {
            to_ws_tx,
            from_ws_rx,
        };
        let this = Self {
            protocol,
            ws_stream,
            to_ws_rx,
            from_ws_tx,
        };
        runtime::spawn(this.routine());
        wsio
    }

    async fn routine(mut self) -> Result<(), Error> {
        // Streams
        let (sink, stream) = self.ws_stream.split();
        let (mut sink, mut stream) = (sink.sink_compat(), stream.compat().fuse());

        // Ping data
        let mut interval = Interval::new(Duration::from_secs(5)).fuse();
        let mut last_ping = None;
        let mut ping_data = [0u8; 4];

        loop {
            select! {
                heartbeat = interval.next() => {
                    if self.protocol.do_ping() {
                        ping_data = rand::thread_rng().gen();
                        let msg = Message::Ping(ping_data.to_vec());
                        last_ping = Some(Instant::now());
                        sink.send(msg).await?;
                    }
                }
                msg = stream.select_next_some() => {
                    let msg = msg?;
                    match msg {
                        Message::Text(txt) => {
                            let res = self.protocol.decode(txt.as_bytes());
                            match res {
                                Ok(out) => {
                                    self.from_ws_tx.send(out).await?;
                                }
                                Err(err) => {
                                    log::error!("Can't get value from text: {}.", err);
                                }
                            }
                        }
                        Message::Binary(bin) => {
                            let res = self.protocol.decode(&bin);
                            match res {
                                Ok(out) => {
                                    self.from_ws_tx.send(out).await?;
                                }
                                Err(err) => {
                                    log::error!("Can't get value from binary: {}.", err);
                                }
                            }
                        }
                        Message::Ping(_) => {
                            // tungstenite reacts this automatically
                        }
                        Message::Pong(vec) => {
                            if vec == ping_data {
                                if let Some(from) = last_ping.take() {
                                    let duration = from.elapsed();
                                    log::debug!("Round-trip: {:?}", duration);
                                } else {
                                    log::warn!("Pong message duplication: {:?}", ping_data);
                                }
                            }
                        }
                        Message::Close(_) => {
                            break;
                        }
                    }
                }
                msg = self.to_ws_rx.next() => {
                    if let Some(msg) = msg {
                        let res = {
                            let bin = self.protocol.encode(&msg);
                            if self.protocol.force_text() {
                                bin.map(|data| {
                                    let s = unsafe { String::from_utf8_unchecked(data) };
                                    Message::Text(s)
                                })
                            } else {
                                bin.map(Message::Binary)
                            }
                        };
                        match res {
                            Ok(msg) => {
                                sink.send(msg).await?;
                            }
                            Err(err) => {
                                log::error!("Can't serialize message: {}.", err);
                            }
                        }
                    } else {
                        let msg = Message::Close(None);
                        sink.send(msg).await?;
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_interaction() {}
}
