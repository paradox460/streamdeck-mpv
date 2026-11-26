//! Drives the real plugin binary against a fake Stream Deck WebSocket server
//! and a real mpv.
#![cfg(unix)]

use std::path::Path;
use std::process::{Child, Command};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream, UnixStream};
use tokio::time::{Instant, sleep, timeout};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;

const WAIT: Duration = Duration::from_secs(5);
const PLUGIN_UUID: &str = "su.pdx.streamdeck.mpv";
const COMMAND_UUID: &str = "su.pdx.streamdeck.mpv.command";
const DISPLAY_UUID: &str = "su.pdx.streamdeck.mpv.display";

struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

type Ws = WebSocketStream<TcpStream>;

async fn next_json(ws: &mut Ws, deadline: Instant) -> Value {
    loop {
        let msg = tokio::time::timeout_at(deadline, ws.next())
            .await
            .expect("timed out waiting for plugin message")
            .expect("plugin closed the WebSocket")
            .unwrap();
        if let Message::Text(text) = msg {
            return serde_json::from_str(&text).unwrap();
        }
    }
}

/// Reads plugin messages until one satisfies `pred`.
async fn wait_for(ws: &mut Ws, what: &str, pred: impl Fn(&Value) -> bool) -> Value {
    let deadline = Instant::now() + WAIT;
    loop {
        let msg = tokio::time::timeout_at(deadline, next_json(ws, deadline))
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
        if pred(&msg) {
            return msg;
        }
    }
}

async fn send(ws: &mut Ws, msg: Value) {
    ws.send(Message::Text(msg.to_string().into()))
        .await
        .unwrap();
}

fn instance_event(event: &str, action: &str, context: &str, settings: Value) -> Value {
    json!({
        "event": event,
        "action": action,
        "context": context,
        "device": "dev1",
        "payload": {
            "settings": settings,
            "coordinates": { "column": 0, "row": 0 },
            "controller": "Keypad",
            "state": 0,
            "isInMultiAction": false,
        },
    })
}

fn is_title(msg: &Value, context: &str, pred: impl Fn(&str) -> bool) -> bool {
    msg["event"] == "setTitle"
        && msg["context"] == context
        && msg["payload"]["title"].as_str().is_some_and(pred)
}

fn is_m_ss(title: &str) -> bool {
    let parts: Vec<&str> = title.split(':').collect();
    parts.len() == 2
        && !parts[0].is_empty()
        && parts[0].bytes().all(|b| b.is_ascii_digit())
        && parts[1].len() == 2
        && parts[1].bytes().all(|b| b.is_ascii_digit())
}

async fn mpv_get_property(sock: &Path, name: &str) -> Value {
    let stream = UnixStream::connect(sock).await.unwrap();
    let (read, mut write) = stream.into_split();
    let request = json!({ "command": ["get_property", name] }).to_string() + "\n";
    write.write_all(request.as_bytes()).await.unwrap();
    let mut lines = BufReader::new(read).lines();
    loop {
        let line = timeout(WAIT, lines.next_line())
            .await
            .expect("mpv reply timed out")
            .unwrap()
            .expect("mpv closed the socket");
        let msg: Value = serde_json::from_str(&line).unwrap();
        if msg.get("error").is_some() {
            return msg["data"].clone();
        }
    }
}

#[tokio::test]
#[ignore = "requires mpv on PATH"]
async fn plugin_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("mpv.sock");
    let sock_str = sock.to_str().unwrap().to_owned();

    // 1. Real mpv playing a synthetic source.
    let mut mpv = KillOnDrop(
        Command::new("mpv")
            .args([
                "--no-config",
                "--idle=yes",
                "--vo=null",
                "--ao=null",
                &format!("--input-ipc-server={sock_str}"),
                "av://lavfi:testsrc=duration=600:size=160x120:rate=30",
            ])
            .spawn()
            .expect("spawn mpv"),
    );
    let deadline = Instant::now() + WAIT;
    while !sock.exists() {
        assert!(Instant::now() < deadline, "mpv socket never appeared");
        sleep(Duration::from_millis(50)).await;
    }

    // 2. Fake Stream Deck app; launch the plugin against it.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let _plugin = KillOnDrop(
        Command::new(env!("CARGO_BIN_EXE_streamdeck-mpv"))
            .args([
                "-port",
                &port.to_string(),
                "-pluginUUID",
                PLUGIN_UUID,
                "-registerEvent",
                "registerPlugin",
                "-info",
                r#"{"devices":[{"id":"dev1","name":"Test","size":{"columns":5,"rows":3},"type":0}]}"#,
            ])
            .spawn()
            .expect("spawn plugin"),
    );
    let (tcp, _) = timeout(WAIT, listener.accept())
        .await
        .expect("plugin never connected")
        .unwrap();
    let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();

    // 3. Registration.
    let register = next_json(&mut ws, Instant::now() + WAIT).await;
    assert_eq!(
        register,
        json!({ "event": "registerPlugin", "uuid": PLUGIN_UUID })
    );

    // 5. Display key renders live time-pos.
    send(
        &mut ws,
        instance_event(
            "willAppear",
            DISPLAY_UUID,
            "disp",
            json!({ "template": "{{ time_pos | time }}", "socketPath": sock_str }),
        ),
    )
    .await;
    wait_for(&mut ws, "M:SS display title", |m| {
        is_title(m, "disp", is_m_ss)
    })
    .await;

    // 6. Text command pauses mpv.
    let settings = json!({ "command": "set pause yes", "socketPath": sock_str });
    send(
        &mut ws,
        instance_event("willAppear", COMMAND_UUID, "cmd", settings.clone()),
    )
    .await;
    send(
        &mut ws,
        instance_event("keyDown", COMMAND_UUID, "cmd", settings),
    )
    .await;
    let deadline = Instant::now() + WAIT;
    while mpv_get_property(&sock, "pause").await != json!(true) {
        assert!(Instant::now() < deadline, "mpv never paused");
        sleep(Duration::from_millis(100)).await;
    }

    // 7. A JSON command mpv rejects shows an alert.
    let settings = json!({ "command": "[\"bogus\"]", "socketPath": sock_str });
    send(
        &mut ws,
        instance_event("willAppear", COMMAND_UUID, "bad", settings.clone()),
    )
    .await;
    send(
        &mut ws,
        instance_event("keyDown", COMMAND_UUID, "bad", settings),
    )
    .await;
    wait_for(&mut ws, "showAlert for bad", |m| {
        *m == json!({ "event": "showAlert", "context": "bad" })
    })
    .await;

    // 8. mpv exit turns the display offline.
    mpv.0.kill().unwrap();
    mpv.0.wait().unwrap();
    wait_for(&mut ws, "offline title", |m| {
        is_title(m, "disp", |t| t == "mpv\noffline")
    })
    .await;
}
