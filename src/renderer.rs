//! Renders Display key titles from observed mpv properties.
//!
//! One renderer task runs per mpv session. Rendering is local and throttled
//! to `RENDER_INTERVAL`; titles are only sent when their text changes.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Duration;

use serde_json::Value;
use tokio::sync::{broadcast, mpsc};
use tokio::time::{Instant, sleep_until};

use crate::mpv::{MpvHandle, PropertyChange};
use crate::template::TitleTemplate;

pub const OFFLINE_TITLE: &str = "mpv\noffline";
pub const TEMPLATE_ERROR_TITLE: &str = "template\nerror";
pub const UNAVAILABLE_TITLE: &str = "--";
pub const RENDER_INTERVAL: Duration = Duration::from_millis(250);

pub enum RenderMsg {
    Upsert { id: String, template: String },
    Remove { id: String },
}

pub trait TitleSink: Send + Sync + 'static {
    fn set_title(&self, id: &str, title: &str) -> impl Future<Output = ()> + Send;
}

/// Sends titles to the Stream Deck app through openaction.
pub struct StreamDeckTitles;

impl TitleSink for StreamDeckTitles {
    async fn set_title(&self, id: &str, title: &str) {
        let Some(instance) = openaction::get_instance(id.to_owned()).await else {
            return;
        };
        if let Err(e) = instance.set_title(Some(title.to_owned()), None).await {
            log::warn!("display {id}: setTitle failed: {e}");
        }
    }
}

struct Entry {
    template: Result<TitleTemplate, String>,
    last: Option<String>,
    last_error: Option<String>,
}

pub async fn run<S: TitleSink>(
    mpv: MpvHandle,
    mut changes: broadcast::Receiver<PropertyChange>,
    mut rx: mpsc::UnboundedReceiver<RenderMsg>,
    sink: S,
) {
    let mut entries: HashMap<String, Entry> = HashMap::new();
    let mut values: HashMap<String, Value> = HashMap::new();
    let mut dirty: HashSet<String> = HashSet::new();
    let mut next_render = Instant::now();
    let mut status = mpv.status();

    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    None => return,
                    Some(RenderMsg::Upsert { id, template }) => {
                        let template = TitleTemplate::compile(&template)
                            .inspect_err(|e| log::warn!("display {id}: invalid template: {e}"));
                        entries.insert(id.clone(), Entry { template, last: None, last_error: None });
                        dirty.insert(id);
                    }
                    Some(RenderMsg::Remove { id }) => {
                        entries.remove(&id);
                        dirty.remove(&id);
                    }
                }
                let union = observed_union(&entries);
                values.retain(|name, _| union.contains(name));
                mpv.set_observed(union);
            }
            change = changes.recv() => match change {
                Ok(PropertyChange { name, value }) => {
                    for (id, entry) in &entries {
                        if entry.template.as_ref().is_ok_and(|t| t.properties().any(|p| p == name)) {
                            dirty.insert(id.clone());
                        }
                    }
                    match value {
                        Some(v) => {
                            values.insert(name, v);
                        }
                        None => {
                            values.remove(&name);
                        }
                    }
                }
                // A stale value is fine: mpv sends the next change soon.
                Err(broadcast::error::RecvError::Lagged(_)) => dirty.extend(entries.keys().cloned()),
                Err(broadcast::error::RecvError::Closed) => return,
            },
            changed = status.changed() => {
                if changed.is_err() {
                    return;
                }
                if !*status.borrow_and_update() {
                    values.clear();
                }
                dirty.extend(entries.keys().cloned());
            }
            _ = sleep_until(next_render), if !dirty.is_empty() => {
                let connected = mpv.is_connected();
                for id in dirty.drain() {
                    let Some(entry) = entries.get_mut(&id) else { continue };
                    let title = match &entry.template {
                        Err(_) => TEMPLATE_ERROR_TITLE.to_owned(),
                        Ok(_) if !connected => OFFLINE_TITLE.to_owned(),
                        Ok(template) => match template.render(&values) {
                            Ok(title) => {
                                entry.last_error = None;
                                title
                            }
                            Err(e) => {
                                if entry.last_error.as_ref() != Some(&e) {
                                    log::warn!("display {id}: render failed: {e}");
                                    entry.last_error = Some(e);
                                }
                                UNAVAILABLE_TITLE.to_owned()
                            }
                        },
                    };
                    if entry.last.as_ref() != Some(&title) {
                        sink.set_title(&id, &title).await;
                        entry.last = Some(title);
                    }
                }
                next_render = Instant::now() + RENDER_INTERVAL;
            }
        }
    }
}

fn observed_union(entries: &HashMap<String, Entry>) -> BTreeSet<String> {
    entries
        .values()
        .filter_map(|e| e.template.as_ref().ok())
        .flat_map(|t| t.properties().map(str::to_owned))
        .collect()
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::mpv::fake::{FakeConn, FakeMpv};
    use serde_json::json;
    use tokio::time::timeout;

    const WAIT: Duration = Duration::from_secs(5);

    struct ChannelSink(mpsc::UnboundedSender<(String, String)>);

    impl TitleSink for ChannelSink {
        async fn set_title(&self, id: &str, title: &str) {
            let _ = self.0.send((id.to_owned(), title.to_owned()));
        }
    }

    struct Harness {
        render_tx: mpsc::UnboundedSender<RenderMsg>,
        titles: mpsc::UnboundedReceiver<(String, String)>,
    }

    impl Harness {
        fn start(path: String) -> Self {
            let (mpv, changes) = MpvHandle::spawn(path);
            let (render_tx, render_rx) = mpsc::unbounded_channel();
            let (title_tx, titles) = mpsc::unbounded_channel();
            tokio::spawn(run(mpv, changes, render_rx, ChannelSink(title_tx)));
            Harness { render_tx, titles }
        }

        fn upsert(&self, id: &str, template: &str) {
            self.render_tx
                .send(RenderMsg::Upsert {
                    id: id.to_owned(),
                    template: template.to_owned(),
                })
                .unwrap();
        }

        async fn next_title(&mut self) -> (String, String) {
            timeout(WAIT, self.titles.recv())
                .await
                .expect("no title within timeout")
                .unwrap()
        }

        /// Skips titles until `want` arrives for `id`.
        async fn expect_title(&mut self, id: &str, want: &str) {
            loop {
                let (got_id, title) = self.next_title().await;
                if got_id == id && title == want {
                    return;
                }
            }
        }
    }

    async fn time_pos(conn: &mut FakeConn, data: Option<f64>) {
        let mut msg = json!({ "event": "property-change", "id": 1, "name": "time-pos" });
        if let Some(d) = data {
            msg["data"] = json!(d);
        }
        conn.send(msg).await;
    }

    async fn expect_observe(conn: &mut FakeConn, name: &str) {
        let msg = conn.read_json().await;
        assert_eq!(msg["command"][0], "observe_property");
        assert_eq!(msg["command"][2], name);
    }

    #[tokio::test]
    async fn renders_on_property_change_and_dedupes() {
        let fake = FakeMpv::new();
        let mut h = Harness::start(fake.path_string());
        h.upsert("k", "{{ time_pos | time }}");
        let mut conn = fake.accept().await;
        expect_observe(&mut conn, "time-pos").await;

        time_pos(&mut conn, Some(1.2)).await;
        h.expect_title("k", "0:01").await;

        time_pos(&mut conn, Some(1.5)).await;
        time_pos(&mut conn, Some(1.8)).await;
        assert!(
            timeout(Duration::from_millis(600), h.titles.recv())
                .await
                .is_err(),
            "unchanged text must not be resent"
        );

        time_pos(&mut conn, Some(2.1)).await;
        assert_eq!(h.next_title().await, ("k".to_owned(), "0:02".to_owned()));
    }

    #[tokio::test]
    async fn unavailable_property_renders_placeholder() {
        let fake = FakeMpv::new();
        let mut h = Harness::start(fake.path_string());
        h.upsert("k", "{{ time_pos | time }}");
        let mut conn = fake.accept().await;
        expect_observe(&mut conn, "time-pos").await;
        time_pos(&mut conn, Some(7.0)).await;
        h.expect_title("k", "0:07").await;
        time_pos(&mut conn, None).await;
        h.expect_title("k", UNAVAILABLE_TITLE).await;
    }

    #[tokio::test]
    async fn offline_title_when_disconnected() {
        let mut fake = FakeMpv::new();
        let mut h = Harness::start(fake.path_string());
        h.upsert("k", "{{ time_pos | time }}");
        let mut conn = fake.accept().await;
        expect_observe(&mut conn, "time-pos").await;
        time_pos(&mut conn, Some(3.0)).await;
        h.expect_title("k", "0:03").await;

        fake.stop();
        drop(conn);
        h.expect_title("k", OFFLINE_TITLE).await;
    }

    #[tokio::test]
    async fn invalid_template_title() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("none.sock");
        let mut h = Harness::start(path.to_str().unwrap().to_owned());
        h.upsert("k", "{{ x");
        assert_eq!(
            h.next_title().await,
            ("k".to_owned(), TEMPLATE_ERROR_TITLE.to_owned())
        );
    }
}
