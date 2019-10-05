#![recursion_limit = "512"]

use failure::{format_err, Error};
use futures::channel::{mpsc, oneshot};
use futures::compat::{Future01CompatExt, Sink01CompatExt, Stream01CompatExt};
use futures::{select, SinkExt, StreamExt};
use futures_legacy::{Sink as LegacySink, Stream as LegacyStream};
use rand::Rng;
use runtime::time::Interval;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio_tungstenite::{accept_async, connect_async};
use tungstenite::{Error as WsError, Message};
use url::Url;

pub trait Protocol: Default + Sync + Send + 'static {
    type In: Sync + Send;
    type Out: Sync + Send;

    fn do_ping(&self) -> bool;
    fn force_text(&self) -> bool;
    fn decode(&self, data: &[u8]) -> Result<Self::In, Error>;
    fn encode(&self, item: &Self::Out) -> Result<Vec<u8>, Error>;
}

#[derive(Debug)]
pub struct WsIo<P: Protocol> {
    to_ws_tx: mpsc::Sender<P::Out>,
    from_ws_rx: mpsc::Receiver<P::In>,
}

impl<P: Protocol> WsIo<P> {
    pub fn client(uri: &str) -> oneshot::Receiver<WsIo<P>> {
        log::debug!("Connect WebSocket client: {}.", uri);
        let (client_tx, client_rx) = oneshot::channel();
        let uri = uri.to_owned();
        runtime::spawn(async move {
            if let Err(err) = WsIo::client_impl(uri, client_tx).await {
                log::error!("WsIo client failed: {}.", err);
            }
        });
        client_rx
    }

    async fn client_impl(uri: String, client_tx: oneshot::Sender<WsIo<P>>) -> Result<(), Error> {
        let url = Url::parse(&uri)?;
        let (ws_stream, _) = connect_async(url).compat().await?;
        let wsio = WsIoInteraction::from_stream(ws_stream);
        client_tx
            .send(wsio)
            .map_err(|_| format_err!("Can't send WsIo client"))?;
        Ok(())
    }

    pub fn server(addr: &str) -> mpsc::Receiver<WsIo<P>> {
        log::debug!("Bind WebSocket server to: {}.", addr);
        let (clients_tx, clients_rx) = mpsc::channel(8);
        let addr = addr.to_owned();
        runtime::spawn(async move {
            if let Err(err) = WsIo::server_impl(addr, clients_tx).await {
                log::error!("WsIo server failed: {}.", err);
            }
        });
        clients_rx
    }

    async fn server_impl(addr: String, mut clients_tx: mpsc::Sender<WsIo<P>>) -> Result<(), Error> {
        let addr = addr.parse()?;
        let socket = TcpListener::bind(&addr)?;
        let mut incoming = socket.incoming().compat();
        while let Some(tcp_stream) = incoming.next().await.transpose()? {
            let ws_stream = accept_async(tcp_stream).compat().await?;
            let wsio = WsIoInteraction::from_stream(ws_stream);
            clients_tx.send(wsio).await?;
        }
        Ok(())
    }

    pub fn tx(&mut self) -> &mut mpsc::Sender<P::Out> {
        &mut self.to_ws_tx
    }

    pub fn rx(&mut self) -> &mut mpsc::Receiver<P::In> {
        &mut self.from_ws_rx
    }
}

struct WsIoInteraction<T, P: Protocol> {
    protocol: P,
    ws_stream: T,
    to_ws_rx: mpsc::Receiver<P::Out>,
    from_ws_tx: mpsc::Sender<P::In>,
}

impl<T, P: Protocol> WsIoInteraction<T, P>
where
    T: Send + 'static,
    T: LegacyStream<Item = Message, Error = WsError>,
    T: LegacySink<SinkItem = Message, SinkError = WsError>,
{
    fn from_stream(ws_stream: T) -> WsIo<P> {
        let (from_ws_tx, from_ws_rx) = mpsc::channel(8);
        let (to_ws_tx, to_ws_rx) = mpsc::channel(8);
        let wsio = WsIo {
            to_ws_tx,
            from_ws_rx,
        };
        let this = Self {
            protocol: P::default(),
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
    use super::*;
    use failure::Error;
    use futures::{SinkExt, StreamExt};
    use serde::{Deserialize, Serialize};

    #[derive(Deserialize, Serialize, Debug, Clone)]
    enum Request {
        Action,
    }

    #[derive(Deserialize, Serialize, Debug, Clone)]
    enum Response {
        Reaction,
    }

    #[derive(Default)]
    struct ServerProtocol;

    impl Protocol for ServerProtocol {
        type In = Request;
        type Out = Response;
        fn do_ping(&self) -> bool {
            false
        }
        fn force_text(&self) -> bool {
            true
        }
        fn decode(&self, data: &[u8]) -> Result<Self::In, Error> {
            serde_json::from_slice(data).map_err(Error::from)
        }
        fn encode(&self, item: &Self::Out) -> Result<Vec<u8>, Error> {
            serde_json::to_vec(item).map_err(Error::from)
        }
    }

    #[derive(Default)]
    struct ClientProtocol;

    impl Protocol for ClientProtocol {
        type In = Response;
        type Out = Request;
        fn do_ping(&self) -> bool {
            true
        }
        fn force_text(&self) -> bool {
            true
        }
        fn decode(&self, data: &[u8]) -> Result<Self::In, Error> {
            serde_json::from_slice(data).map_err(Error::from)
        }
        fn encode(&self, item: &Self::Out) -> Result<Vec<u8>, Error> {
            serde_json::to_vec(item).map_err(Error::from)
        }
    }

    #[runtime::test(runtime_tokio::Tokio)]
    async fn test_interaction() -> Result<(), Error> {
        let mut server = WsIo::<ServerProtocol>::server("127.0.0.1:6776");
        let mut client = WsIo::<ClientProtocol>::client("ws://127.0.0.1:6776").await?;
        let mut client_on_server = server
            .next()
            .await
            .ok_or_else(|| format_err!("No client connected"))?;
        client.tx().send(Request::Action).await?;
        let Request::Action = client_on_server
            .rx()
            .next()
            .await
            .ok_or_else(|| format_err!("No request received"))?;
        client_on_server.tx().send(Response::Reaction).await?;
        let Response::Reaction = client
            .rx()
            .next()
            .await
            .ok_or_else(|| format_err!("No response received"))?;
        Ok(())
    }
}
