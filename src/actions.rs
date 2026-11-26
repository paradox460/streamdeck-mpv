//! Stream Deck action handlers.

use std::sync::Arc;

use openaction::{Action, Instance, OpenActionResult, async_trait};
use serde::{Deserialize, Serialize};

use crate::app::App;
use crate::command::{self, CommandInput};
use crate::mpv::MpvHandle;

pub const COMMAND_UUID: &str = "su.pdx.streamdeck.mpv.command";
pub const DISPLAY_UUID: &str = "su.pdx.streamdeck.mpv.display";
pub const DEFAULT_TEMPLATE: &str = "{{ time_pos | time }}";

#[derive(Serialize, Deserialize, Default, Clone)]
#[serde(default, rename_all = "camelCase")]
pub struct CommandSettings {
    pub command: String,
    pub socket_path: String,
}

#[derive(Serialize, Deserialize, Default, Clone)]
#[serde(default, rename_all = "camelCase")]
pub struct DisplaySettings {
    pub template: String,
    pub socket_path: String,
}

pub struct CommandAction {
    pub app: Arc<App>,
}

pub struct DisplayAction {
    pub app: Arc<App>,
}

#[async_trait]
impl Action for CommandAction {
    const UUID: &'static str = COMMAND_UUID;
    type Settings = CommandSettings;

    async fn will_appear(
        &self,
        instance: &Instance,
        settings: &CommandSettings,
    ) -> OpenActionResult<()> {
        self.app.bind(&instance.instance_id, &settings.socket_path);
        Ok(())
    }

    async fn did_receive_settings(
        &self,
        instance: &Instance,
        settings: &CommandSettings,
    ) -> OpenActionResult<()> {
        self.app.bind(&instance.instance_id, &settings.socket_path);
        Ok(())
    }

    async fn will_disappear(
        &self,
        instance: &Instance,
        _: &CommandSettings,
    ) -> OpenActionResult<()> {
        self.app.unbind(&instance.instance_id);
        Ok(())
    }

    async fn key_down(
        &self,
        instance: &Instance,
        settings: &CommandSettings,
    ) -> OpenActionResult<()> {
        let Some(mpv) = self.app.mpv(&instance.instance_id) else {
            return instance.show_alert().await;
        };
        let id = instance.instance_id.clone();
        let input = settings.command.clone();
        // openaction handles events sequentially; never wait on mpv inline.
        tokio::spawn(async move {
            if let Err(err) = execute(&mpv, &input).await {
                log::warn!("command {input:?} failed: {err}");
                if let Some(instance) = openaction::get_instance(id).await {
                    let _ = instance.show_alert().await;
                }
            }
        });
        Ok(())
    }
}

async fn execute(mpv: &MpvHandle, input: &str) -> Result<(), String> {
    match command::parse(input)? {
        CommandInput::Json(args) => mpv.command(args).await.map(drop).map_err(|e| e.to_string()),
        CommandInput::Text(lines) => {
            for line in lines {
                mpv.send_text(line).await.map_err(|e| e.to_string())?;
            }
            Ok(())
        }
    }
}

impl DisplayAction {
    fn show(&self, instance: &Instance, settings: &DisplaySettings) {
        self.app.bind(&instance.instance_id, &settings.socket_path);
        let template = if settings.template.trim().is_empty() {
            DEFAULT_TEMPLATE
        } else {
            &settings.template
        };
        self.app.show(&instance.instance_id, template.to_owned());
    }
}

#[async_trait]
impl Action for DisplayAction {
    const UUID: &'static str = DISPLAY_UUID;
    type Settings = DisplaySettings;

    async fn will_appear(
        &self,
        instance: &Instance,
        settings: &DisplaySettings,
    ) -> OpenActionResult<()> {
        self.show(instance, settings);
        Ok(())
    }

    async fn did_receive_settings(
        &self,
        instance: &Instance,
        settings: &DisplaySettings,
    ) -> OpenActionResult<()> {
        self.show(instance, settings);
        Ok(())
    }

    async fn will_disappear(
        &self,
        instance: &Instance,
        _: &DisplaySettings,
    ) -> OpenActionResult<()> {
        self.app.unbind(&instance.instance_id);
        Ok(())
    }
}
