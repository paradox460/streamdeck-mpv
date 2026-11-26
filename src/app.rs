//! Pool of mpv sessions shared by all action instances that use the same
//! socket path.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;

use crate::mpv::MpvHandle;
use crate::renderer::{self, RenderMsg, StreamDeckTitles};
use crate::socket_path;

pub struct App {
    /// Never held across `.await`.
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    sessions: HashMap<String, Session>,
    /// Instance id → resolved socket path.
    bindings: HashMap<String, String>,
}

struct Session {
    mpv: MpvHandle,
    render_tx: mpsc::UnboundedSender<RenderMsg>,
    users: HashSet<String>,
}

impl Session {
    fn start(path: String) -> Self {
        let (mpv, changes) = MpvHandle::spawn(path);
        let (render_tx, render_rx) = mpsc::unbounded_channel();
        tokio::spawn(renderer::run(
            mpv.clone(),
            changes,
            render_rx,
            StreamDeckTitles,
        ));
        Session {
            mpv,
            render_tx,
            users: HashSet::new(),
        }
    }
}

impl Inner {
    /// Removes `instance` from its session; the session (renderer, mpv actor,
    /// socket) shuts down once its last user leaves.
    fn detach(&mut self, instance: &str) {
        let Some(path) = self.bindings.remove(instance) else {
            return;
        };
        let Some(session) = self.sessions.get_mut(&path) else {
            return;
        };
        let _ = session.render_tx.send(RenderMsg::Remove {
            id: instance.to_owned(),
        });
        session.users.remove(instance);
        if session.users.is_empty() {
            self.sessions.remove(&path);
        }
    }
}

impl App {
    pub fn new() -> Arc<App> {
        Arc::new(App {
            inner: Mutex::new(Inner::default()),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn bind(&self, instance: &str, raw_path: &str) -> MpvHandle {
        let path = socket_path::resolve(raw_path);
        let mut inner = self.lock();
        if inner.bindings.get(instance) == Some(&path) {
            return inner.sessions[&path].mpv.clone();
        }
        inner.detach(instance);
        let session = inner
            .sessions
            .entry(path.clone())
            .or_insert_with(|| Session::start(path.clone()));
        session.users.insert(instance.to_owned());
        let mpv = session.mpv.clone();
        inner.bindings.insert(instance.to_owned(), path);
        mpv
    }

    pub fn unbind(&self, instance: &str) {
        self.lock().detach(instance);
    }

    pub fn show(&self, instance: &str, template: String) {
        let inner = self.lock();
        match inner
            .bindings
            .get(instance)
            .and_then(|path| inner.sessions.get(path))
        {
            Some(session) => {
                let _ = session.render_tx.send(RenderMsg::Upsert {
                    id: instance.to_owned(),
                    template,
                });
            }
            None => log::warn!("show for unbound instance {instance}"),
        }
    }

    pub fn mpv(&self, instance: &str) -> Option<MpvHandle> {
        let inner = self.lock();
        let path = inner.bindings.get(instance)?;
        inner.sessions.get(path).map(|s| s.mpv.clone())
    }
}
