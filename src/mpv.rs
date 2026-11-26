//! mpv JSON IPC client: one actor task per socket path that owns the
//! connection, reconnects, and keeps property observations alive.

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::io;
use std::time::Duration;

use serde_json::{Map, Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, ReadHalf, WriteHalf};
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::time::{Instant, sleep_until, timeout};

pub const COMMAND_TIMEOUT: Duration = Duration::from_secs(2);
pub const RECONNECT_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, PartialEq)]
pub enum MpvError {
    NotConnected,
    Disconnected,
    Timeout,
    Mpv(String),
}

impl fmt::Display for MpvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MpvError::NotConnected => f.write_str("mpv not connected"),
            MpvError::Disconnected => f.write_str("mpv disconnected"),
            MpvError::Timeout => f.write_str("mpv did not reply in time"),
            MpvError::Mpv(e) => write!(f, "mpv error: {e}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PropertyChange {
    pub name: String,
    /// `None` when mpv reports the property as unavailable.
    pub value: Option<Value>,
}

type Reply = oneshot::Sender<Result<Value, MpvError>>;

enum Request {
    Command {
        args: Vec<Value>,
        reply: Reply,
    },
    Text {
        line: String,
        done: oneshot::Sender<Result<(), MpvError>>,
    },
    SetObserved(BTreeSet<String>),
}

#[derive(Clone)]
pub struct MpvHandle {
    tx: mpsc::UnboundedSender<Request>,
    status: watch::Receiver<bool>,
}

impl MpvHandle {
    /// Starts the connection actor for `path`. The actor stops once every
    /// handle clone is dropped.
    pub fn spawn(path: String) -> (MpvHandle, broadcast::Receiver<PropertyChange>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let (changes_tx, changes_rx) = broadcast::channel(256);
        let (status_tx, status_rx) = watch::channel(false);
        tokio::spawn(run(path, rx, changes_tx, status_tx));
        (
            MpvHandle {
                tx,
                status: status_rx,
            },
            changes_rx,
        )
    }

    /// Sends a JSON command and waits for mpv's reply `data`.
    pub async fn command(&self, args: Vec<Value>) -> Result<Value, MpvError> {
        let (reply, reply_rx) = oneshot::channel();
        self.tx
            .send(Request::Command { args, reply })
            .map_err(|_| MpvError::NotConnected)?;
        match timeout(COMMAND_TIMEOUT, reply_rx).await {
            Err(_) => Err(MpvError::Timeout),
            Ok(Err(_)) => Err(MpvError::Disconnected),
            Ok(Ok(result)) => result,
        }
    }

    /// Writes an input.conf text command. mpv sends no reply, so success only
    /// means the line was written.
    pub async fn send_text(&self, line: String) -> Result<(), MpvError> {
        let (done, done_rx) = oneshot::channel();
        self.tx
            .send(Request::Text { line, done })
            .map_err(|_| MpvError::NotConnected)?;
        done_rx.await.unwrap_or(Err(MpvError::Disconnected))
    }

    /// Replaces the set of observed properties; survives reconnects.
    pub fn set_observed(&self, names: BTreeSet<String>) {
        let _ = self.tx.send(Request::SetObserved(names));
    }

    pub fn is_connected(&self) -> bool {
        *self.status.borrow()
    }

    pub fn status(&self) -> watch::Receiver<bool> {
        self.status.clone()
    }
}

#[cfg(unix)]
type Stream = tokio::net::UnixStream;

#[cfg(unix)]
async fn connect(path: &str) -> io::Result<Stream> {
    tokio::net::UnixStream::connect(path).await
}

#[cfg(windows)]
type Stream = tokio::net::windows::named_pipe::NamedPipeClient;

#[cfg(windows)]
async fn connect(path: &str) -> io::Result<Stream> {
    const ERROR_PIPE_BUSY: i32 = 231;
    loop {
        match tokio::net::windows::named_pipe::ClientOptions::new().open(path) {
            Err(e) if e.raw_os_error() == Some(ERROR_PIPE_BUSY) => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            result => return result,
        }
    }
}

async fn run(
    path: String,
    mut rx: mpsc::UnboundedReceiver<Request>,
    changes: broadcast::Sender<PropertyChange>,
    status: watch::Sender<bool>,
) {
    let mut desired = BTreeSet::new();
    // Starts at 1: mpv answers requests without a request_id using id 0.
    let mut next_request_id: u64 = 1;
    loop {
        let stream = match connect(&path).await {
            Ok(stream) => stream,
            Err(e) => {
                log::debug!("mpv connect {path} failed: {e}");
                let deadline = Instant::now() + RECONNECT_DELAY;
                loop {
                    tokio::select! {
                        _ = sleep_until(deadline) => break,
                        req = rx.recv() => match req {
                            None => return,
                            Some(Request::Command { reply, .. }) => {
                                let _ = reply.send(Err(MpvError::NotConnected));
                            }
                            Some(Request::Text { done, .. }) => {
                                let _ = done.send(Err(MpvError::NotConnected));
                            }
                            Some(Request::SetObserved(names)) => desired = names,
                        },
                    }
                }
                continue;
            }
        };
        log::info!("mpv connected: {path}");
        let mut session = Session::new(stream, &changes, &mut desired, &mut next_request_id);
        status.send_replace(true);
        let outcome = session.serve(&mut rx).await;
        session.close();
        status.send_replace(false);
        match outcome {
            Outcome::Disconnected => log::info!("mpv disconnected: {path}"),
            Outcome::Shutdown => return,
        }
    }
}

enum Outcome {
    Disconnected,
    Shutdown,
}

/// State for one live connection.
struct Session<'a> {
    writer: WriteHalf<Stream>,
    msgs: mpsc::UnboundedReceiver<Value>,
    reader: tokio::task::JoinHandle<()>,
    pending: HashMap<u64, Reply>,
    observed: HashMap<String, u64>,
    next_observe_id: u64,
    changes: &'a broadcast::Sender<PropertyChange>,
    desired: &'a mut BTreeSet<String>,
    next_request_id: &'a mut u64,
}

impl<'a> Session<'a> {
    fn new(
        stream: Stream,
        changes: &'a broadcast::Sender<PropertyChange>,
        desired: &'a mut BTreeSet<String>,
        next_request_id: &'a mut u64,
    ) -> Self {
        let (read, writer) = tokio::io::split(stream);
        let (msg_tx, msgs) = mpsc::unbounded_channel();
        // A separate reader task keeps the select! loop cancel-safe.
        let reader = tokio::spawn(read_loop(read, msg_tx));
        Session {
            writer,
            msgs,
            reader,
            pending: HashMap::new(),
            observed: HashMap::new(),
            next_observe_id: 1,
            changes,
            desired,
            next_request_id,
        }
    }

    async fn serve(&mut self, rx: &mut mpsc::UnboundedReceiver<Request>) -> Outcome {
        let initial: Vec<String> = self.desired.iter().cloned().collect();
        for name in initial {
            if self.observe(name).await.is_err() {
                return Outcome::Disconnected;
            }
        }
        loop {
            tokio::select! {
                msg = self.msgs.recv() => match msg {
                    None => return Outcome::Disconnected,
                    Some(msg) => self.handle_message(msg),
                },
                req = rx.recv() => match req {
                    None => return Outcome::Shutdown,
                    Some(req) => {
                        if self.handle_request(req).await.is_err() {
                            return Outcome::Disconnected;
                        }
                    }
                },
            }
        }
    }

    fn close(self) {
        self.reader.abort();
        for (_, reply) in self.pending {
            let _ = reply.send(Err(MpvError::Disconnected));
        }
    }

    async fn handle_request(&mut self, req: Request) -> io::Result<()> {
        match req {
            Request::Command { args, reply } => {
                let id = *self.next_request_id;
                *self.next_request_id += 1;
                if let Err(e) = self
                    .write(&json!({ "command": args, "request_id": id }))
                    .await
                {
                    let _ = reply.send(Err(MpvError::Disconnected));
                    return Err(e);
                }
                self.pending.insert(id, reply);
            }
            Request::Text { line, done } => {
                let mut line = line.replace(['\r', '\n'], " ");
                line.push('\n');
                match self.writer.write_all(line.as_bytes()).await {
                    Ok(()) => {
                        let _ = done.send(Ok(()));
                    }
                    Err(e) => {
                        let _ = done.send(Err(MpvError::Disconnected));
                        return Err(e);
                    }
                }
            }
            Request::SetObserved(new) => {
                let removed: Vec<String> = self
                    .observed
                    .keys()
                    .filter(|name| !new.contains(*name))
                    .cloned()
                    .collect();
                let added: Vec<String> = new
                    .iter()
                    .filter(|name| !self.observed.contains_key(*name))
                    .cloned()
                    .collect();
                *self.desired = new;
                for name in removed {
                    let id = self.observed.remove(&name).expect("name from observed");
                    self.write(&json!({ "command": ["unobserve_property", id] }))
                        .await?;
                }
                for name in added {
                    self.observe(name).await?;
                }
            }
        }
        Ok(())
    }

    async fn observe(&mut self, name: String) -> io::Result<()> {
        let id = self.next_observe_id;
        self.next_observe_id += 1;
        let msg = json!({ "command": ["observe_property", id, name] });
        self.observed.insert(name, id);
        self.write(&msg).await
    }

    async fn write(&mut self, msg: &Value) -> io::Result<()> {
        let mut line = msg.to_string();
        line.push('\n');
        self.writer.write_all(line.as_bytes()).await
    }

    fn handle_message(&mut self, msg: Value) {
        let Value::Object(mut msg) = msg else {
            return;
        };
        if msg.get("event").and_then(Value::as_str) == Some("property-change") {
            if let Some(Value::String(name)) = msg.remove("name") {
                let value = msg.remove("data").filter(|v| !v.is_null());
                let _ = self.changes.send(PropertyChange { name, value });
            }
            return;
        }
        let Some(id) = msg.get("request_id").and_then(Value::as_u64) else {
            return;
        };
        if let Some(reply) = self.pending.remove(&id) {
            let _ = reply.send(reply_result(msg));
        }
    }
}

fn reply_result(mut msg: Map<String, Value>) -> Result<Value, MpvError> {
    match msg.get("error").and_then(Value::as_str) {
        Some("success") => Ok(msg.remove("data").unwrap_or(Value::Null)),
        Some(error) => Err(MpvError::Mpv(error.to_owned())),
        None => Err(MpvError::Mpv("reply without error field".to_owned())),
    }
}

async fn read_loop(read: ReadHalf<Stream>, tx: mpsc::UnboundedSender<Value>) {
    let mut reader = BufReader::new(read);
    let mut buf = Vec::new();
    loop {
        buf.clear();
        match reader.read_until(b'\n', &mut buf).await {
            Ok(0) => return,
            Ok(_) => {}
            Err(e) => {
                log::debug!("mpv read failed: {e}");
                return;
            }
        }
        // mpv may emit invalid UTF-8 (e.g. file names).
        let line = String::from_utf8_lossy(&buf);
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<Value>(line) {
            Ok(msg) => {
                if tx.send(msg).is_err() {
                    return;
                }
            }
            Err(e) => log::warn!("unparseable mpv message {line:?}: {e}"),
        }
    }
}

#[cfg(all(test, unix))]
pub(crate) mod fake {
    use std::path::PathBuf;
    use std::time::Duration;

    use serde_json::Value;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::UnixListener;
    use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
    use tokio::time::timeout;

    const WAIT: Duration = Duration::from_secs(5);

    /// A scripted stand-in for mpv's IPC socket.
    pub struct FakeMpv {
        _dir: tempfile::TempDir,
        pub path: PathBuf,
        listener: Option<UnixListener>,
    }

    impl FakeMpv {
        pub fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("mpv.sock");
            let listener = Some(UnixListener::bind(&path).unwrap());
            FakeMpv {
                _dir: dir,
                path,
                listener,
            }
        }

        pub fn path_string(&self) -> String {
            self.path.to_str().unwrap().to_owned()
        }

        /// Stops listening so reconnect attempts fail.
        pub fn stop(&mut self) {
            self.listener = None;
            std::fs::remove_file(&self.path).unwrap();
        }

        pub fn start(&mut self) {
            self.listener = Some(UnixListener::bind(&self.path).unwrap());
        }

        pub async fn accept(&self) -> FakeConn {
            let listener = self.listener.as_ref().expect("fake mpv not listening");
            let (stream, _) = timeout(WAIT, listener.accept())
                .await
                .expect("accept timed out")
                .unwrap();
            let (read, writer) = stream.into_split();
            FakeConn {
                reader: BufReader::new(read),
                writer,
            }
        }
    }

    pub struct FakeConn {
        reader: BufReader<OwnedReadHalf>,
        writer: OwnedWriteHalf,
    }

    impl FakeConn {
        pub async fn read_line(&mut self) -> String {
            let mut line = String::new();
            let n = timeout(WAIT, self.reader.read_line(&mut line))
                .await
                .expect("read timed out")
                .unwrap();
            assert!(n > 0, "client closed the connection");
            line.trim_end_matches('\n').to_owned()
        }

        pub async fn read_json(&mut self) -> Value {
            serde_json::from_str(&self.read_line().await).unwrap()
        }

        pub async fn send(&mut self, msg: Value) {
            let mut line = msg.to_string();
            line.push('\n');
            self.writer.write_all(line.as_bytes()).await.unwrap();
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::fake::FakeMpv;
    use super::*;

    const WAIT: Duration = Duration::from_secs(5);

    #[tokio::test]
    async fn command_round_trip() {
        let fake = FakeMpv::new();
        let (mpv, _changes) = MpvHandle::spawn(fake.path_string());
        let mut conn = fake.accept().await;
        let reply = tokio::spawn(async move {
            mpv.command(vec![json!("get_property"), json!("volume")])
                .await
        });
        let req = conn.read_json().await;
        assert_eq!(req["command"], json!(["get_property", "volume"]));
        conn.send(json!({ "request_id": req["request_id"], "error": "success", "data": 42 }))
            .await;
        assert_eq!(reply.await.unwrap(), Ok(json!(42)));
    }

    #[tokio::test]
    async fn mpv_error_reply() {
        let fake = FakeMpv::new();
        let (mpv, _changes) = MpvHandle::spawn(fake.path_string());
        let mut conn = fake.accept().await;
        let reply = tokio::spawn(async move { mpv.command(vec![json!("bogus")]).await });
        let req = conn.read_json().await;
        conn.send(json!({ "request_id": req["request_id"], "error": "invalid parameter" }))
            .await;
        assert_eq!(
            reply.await.unwrap(),
            Err(MpvError::Mpv("invalid parameter".to_owned()))
        );
    }

    #[tokio::test]
    async fn not_connected_when_no_socket() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing.sock");
        let (mpv, _changes) = MpvHandle::spawn(path.to_str().unwrap().to_owned());
        let result = timeout(Duration::from_millis(500), mpv.command(vec![json!("stop")]))
            .await
            .expect("command should fail fast");
        assert_eq!(result, Err(MpvError::NotConnected));
    }

    #[tokio::test]
    async fn text_command_written_verbatim() {
        let fake = FakeMpv::new();
        let (mpv, _changes) = MpvHandle::spawn(fake.path_string());
        let mut conn = fake.accept().await;
        assert_eq!(mpv.send_text("cycle pause".to_owned()).await, Ok(()));
        assert_eq!(conn.read_line().await, "cycle pause");
    }

    #[tokio::test]
    async fn property_change_broadcast() {
        let fake = FakeMpv::new();
        let (_mpv, mut changes) = MpvHandle::spawn(fake.path_string());
        let mut conn = fake.accept().await;
        conn.send(json!({ "event": "property-change", "id": 1, "name": "time-pos", "data": 1.0 }))
            .await;
        conn.send(json!({ "event": "property-change", "id": 1, "name": "time-pos" }))
            .await;
        let first = timeout(WAIT, changes.recv()).await.unwrap().unwrap();
        assert_eq!(
            first,
            PropertyChange {
                name: "time-pos".to_owned(),
                value: Some(json!(1.0))
            }
        );
        let second = timeout(WAIT, changes.recv()).await.unwrap().unwrap();
        assert_eq!(
            second,
            PropertyChange {
                name: "time-pos".to_owned(),
                value: None
            }
        );
    }

    #[tokio::test]
    async fn reobserves_after_reconnect() {
        let mut fake = FakeMpv::new();
        let (mpv, _changes) = MpvHandle::spawn(fake.path_string());
        let mut status = mpv.status();
        mpv.set_observed(BTreeSet::from(["time-pos".to_owned()]));

        let mut conn = fake.accept().await;
        let observe = conn.read_json().await;
        assert_eq!(observe["command"][0], "observe_property");
        assert_eq!(observe["command"][2], "time-pos");
        timeout(WAIT, status.wait_for(|up| *up))
            .await
            .unwrap()
            .unwrap();

        fake.stop();
        drop(conn);
        timeout(WAIT, status.wait_for(|up| !*up))
            .await
            .unwrap()
            .unwrap();

        fake.start();
        let mut conn = fake.accept().await;
        let observe = conn.read_json().await;
        assert_eq!(observe["command"][0], "observe_property");
        assert_eq!(observe["command"][2], "time-pos");
        timeout(WAIT, status.wait_for(|up| *up))
            .await
            .unwrap()
            .unwrap();
    }
}
