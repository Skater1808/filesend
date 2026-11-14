//! Grafische Oberfläche: eine schmale Ablage-Leiste am Bildschirmrand.

use eframe::egui;
use egui::{
    Align, Align2, Color32, FontId, Frame, Label, Layout, Margin, RichText, Sense, Stroke,
    StrokeKind, Vec2, ViewportCommand,
};
use std::fs;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use filesend::config::{Config, Side, default_download_dir};
use filesend::discovery::{self, Peer};
use filesend::transfer::{self, ReceiveEvent, ReceiverConfig, ReceiverHandle, SendEvent};
use filesend::util;
use filesend::{APP_NAME, VERSION};

const BG: Color32 = Color32::from_rgb(14, 16, 22);
const CARD: Color32 = Color32::from_rgb(23, 26, 34);
const CARD_HOVER: Color32 = Color32::from_rgb(30, 34, 44);
const INPUT_BG: Color32 = Color32::from_rgb(10, 12, 17);
const BORDER: Color32 = Color32::from_rgb(42, 47, 60);
const TEXT: Color32 = Color32::from_rgb(232, 236, 245);
const MUTED: Color32 = Color32::from_rgb(132, 141, 160);
const ACCENT: Color32 = Color32::from_rgb(64, 220, 160);
const ACCENT_DARK: Color32 = Color32::from_rgb(22, 92, 70);
const DANGER: Color32 = Color32::from_rgb(255, 112, 112);
const WARN: Color32 = Color32::from_rgb(255, 192, 92);

// ---------------------------------------------------------------------------
// Zustand
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum OutState {
    Sending,
    Done,
    Failed(String),
    Cancelled,
}

struct OutFile {
    name: String,
    size: u64,
    sent: u64,
    saved_as: Option<String>,
}

struct Outgoing {
    id: u64,
    target: String,
    files: Vec<OutFile>,
    state: OutState,
    cancel: Arc<AtomicBool>,
    started: Instant,
}

impl Outgoing {
    fn total_bytes(&self) -> u64 {
        self.files.iter().map(|file| file.size).sum()
    }

    fn sent_bytes(&self) -> u64 {
        self.files.iter().map(|file| file.sent.min(file.size)).sum()
    }

    fn fraction(&self) -> f32 {
        let total = self.total_bytes();
        if total == 0 {
            if matches!(self.state, OutState::Sending) {
                0.35
            } else {
                1.0
            }
        } else {
            (self.sent_bytes() as f64 / total as f64) as f32
        }
    }

    fn speed(&self) -> f64 {
        let elapsed = self.started.elapsed().as_secs_f64();
        if elapsed <= 0.0 {
            0.0
        } else {
            self.sent_bytes() as f64 / elapsed
        }
    }

    fn title(&self) -> String {
        match self.files.as_slice() {
            [] => "Datei".into(),
            [single] => single.name.clone(),
            [first, ..] => format!("{} + {} weitere", first.name, self.files.len() - 1),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InState {
    Receiving,
    Saved,
    Failed,
}

struct Incoming {
    id: u64,
    name: String,
    saved_as: Option<String>,
    path: Option<PathBuf>,
    size: u64,
    received: u64,
    sha256: Option<String>,
    from: String,
    state: InState,
    error: Option<String>,
}

#[derive(Default)]
struct AppModel {
    target: String,
    peers: Vec<Peer>,
    outgoing: Vec<Outgoing>,
    incoming: Vec<Incoming>,
}

impl AppModel {
    fn apply_send(&mut self, event: SendEvent) {
        match event {
            SendEvent::Started { batch, target, .. } => {
                if let Some(item) = self.outgoing.iter_mut().find(|item| item.id == batch) {
                    item.target = target;
                }
            }
            SendEvent::FileStarted { batch, .. } => {
                if let Some(item) = self.outgoing.iter_mut().find(|item| item.id == batch) {
                    item.state = OutState::Sending;
                }
            }
            SendEvent::Progress {
                batch,
                index,
                sent,
                total,
            } => {
                if let Some(item) = self.outgoing.iter_mut().find(|item| item.id == batch)
                    && let Some(file) = item.files.get_mut(index)
                {
                    file.sent = sent;
                    file.size = total;
                }
            }
            SendEvent::FileSaved {
                batch,
                index,
                saved_as,
                ..
            } => {
                if let Some(item) = self.outgoing.iter_mut().find(|item| item.id == batch)
                    && let Some(file) = item.files.get_mut(index)
                {
                    file.sent = file.size;
                    file.saved_as = Some(saved_as);
                }
            }
            SendEvent::Finished { batch, .. } => {
                if let Some(item) = self.outgoing.iter_mut().find(|item| item.id == batch) {
                    item.state = OutState::Done;
                }
            }
            SendEvent::Failed {
                batch,
                error,
                cancelled,
            } => {
                if let Some(item) = self.outgoing.iter_mut().find(|item| item.id == batch) {
                    item.state = if cancelled {
                        OutState::Cancelled
                    } else {
                        OutState::Failed(error)
                    };
                }
            }
        }
    }

    fn apply_receive(&mut self, event: ReceiveEvent) {
        match event {
            ReceiveEvent::Started {
                id,
                name,
                size,
                from,
            } => {
                self.incoming.insert(
                    0,
                    Incoming {
                        id,
                        name,
                        saved_as: None,
                        path: None,
                        size,
                        received: 0,
                        sha256: None,
                        from,
                        state: InState::Receiving,
                        error: None,
                    },
                );
                self.incoming.truncate(40);
            }
            ReceiveEvent::Progress {
                id,
                received,
                total,
            } => {
                if let Some(item) = self.incoming.iter_mut().find(|item| item.id == id) {
                    item.received = received;
                    item.size = total;
                }
            }
            ReceiveEvent::Saved {
                id,
                name,
                saved_as,
                path,
                size,
                sha256,
                from,
            } => {
                let item = self.ensure_incoming(id, name, size, from);
                item.state = InState::Saved;
                item.received = size;
                item.saved_as = Some(saved_as);
                item.path = Some(path);
                item.sha256 = Some(sha256);
            }
            ReceiveEvent::Failed { id, name, error } => {
                let item = self.ensure_incoming(id, name, 0, String::new());
                item.state = InState::Failed;
                item.error = Some(error);
            }
        }
    }

    fn ensure_incoming(&mut self, id: u64, name: String, size: u64, from: String) -> &mut Incoming {
        if let Some(index) = self.incoming.iter().position(|item| item.id == id) {
            return &mut self.incoming[index];
        }
        self.incoming.insert(
            0,
            Incoming {
                id,
                name,
                saved_as: None,
                path: None,
                size,
                received: 0,
                sha256: None,
                from,
                state: InState::Receiving,
                error: None,
            },
        );
        self.incoming.truncate(40);
        &mut self.incoming[0]
    }
}

struct Notice {
    text: String,
    error: bool,
    at: Instant,
}

struct SettingsDraft {
    port: u16,
    token: String,
    dir: String,
    edge: Side,
}

impl SettingsDraft {
    fn from_config(config: &Config) -> Self {
        Self {
            port: config.port,
            token: config.token.clone(),
            dir: config.download_dir.display().to_string(),
            edge: config.edge,
        }
    }
}

pub struct FilesendApp {
    ctx: egui::Context,
    cfg: Config,
    shared: Arc<Mutex<AppModel>>,
    receiver: Option<ReceiverHandle>,
    local_ip: Option<IpAddr>,
    discovery_stop: Arc<AtomicBool>,
    next_batch: Arc<AtomicU64>,
    settings: SettingsDraft,
    show_settings: bool,
    hover_files: usize,
    snap_requested: bool,
    snapped: bool,
    /// Letzte beobachtete Fensterposition, um Ziehen zu erkennen.
    last_window_pos: Option<egui::Pos2>,
    last_window_move: Instant,
    /// Nach dem Andocken kurz nicht auf Positionsänderungen reagieren.
    snap_settle_until: Instant,
    notice: Option<Notice>,
}

impl FilesendApp {
    pub fn new(cc: &eframe::CreationContext<'_>) -> Self {
        install_style(&cc.egui_ctx);
        let cfg = Config::load();
        let shared = Arc::new(Mutex::new(AppModel {
            target: cfg.peer.clone(),
            ..Default::default()
        }));
        let mut app = Self {
            ctx: cc.egui_ctx.clone(),
            settings: SettingsDraft::from_config(&cfg),
            cfg,
            shared,
            receiver: None,
            local_ip: util::local_ip(),
            discovery_stop: Arc::new(AtomicBool::new(false)),
            next_batch: Arc::new(AtomicU64::new(1)),
            show_settings: false,
            hover_files: 0,
            snap_requested: true,
            snapped: false,
            last_window_pos: None,
            last_window_move: Instant::now(),
            snap_settle_until: Instant::now(),
            notice: None,
        };
        app.restart_receiver();
        app.restart_discovery();
        app
    }

    fn restart_receiver(&mut self) {
        if let Some(mut old) = self.receiver.take() {
            old.shutdown();
        }
        let shared = self.shared.clone();
        let ctx = self.ctx.clone();
        let edge = self.cfg.edge;
        let result = transfer::start_receiver(
            std::net::SocketAddr::from(([0, 0, 0, 0], self.cfg.port)),
            ReceiverConfig {
                dir: self.cfg.download_dir.clone(),
                token: self.cfg.token.clone(),
                name: self.cfg.device_name.clone(),
                edge,
            },
            move |event| {
                if let Ok(mut model) = shared.lock() {
                    model.apply_receive(event);
                }
                ctx.request_repaint();
            },
        );
        match result {
            Ok(handle) => self.receiver = Some(handle),
            Err(err) => {
                self.receiver = None;
                self.set_notice(format!("Empfang konnte nicht starten: {err:#}"), true);
            }
        }
    }

    fn restart_discovery(&mut self) {
        self.discovery_stop.store(true, Ordering::Relaxed);
        let stop = Arc::new(AtomicBool::new(false));
        self.discovery_stop = stop.clone();
        let shared = self.shared.clone();
        let ctx = self.ctx.clone();
        let port = self.cfg.port;
        let local_edge = self.cfg.edge;
        thread::spawn(move || {
            loop {
                let peers: Vec<Peer> = discovery::discover(port, Duration::from_millis(500))
                    .into_iter()
                    .filter(|peer| local_edge.can_send_to(peer.edge))
                    .collect();
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                if let Ok(mut model) = shared.lock() {
                    let target_is_empty = model.target.trim().is_empty();
                    model.peers = peers;
                    if target_is_empty && let Some(peer) = model.peers.first() {
                        model.target = peer.addr.to_string();
                    }
                }
                ctx.request_repaint();
                for _ in 0..25 {
                    if stop.load(Ordering::Relaxed) {
                        return;
                    }
                    thread::sleep(Duration::from_millis(100));
                }
            }
        });
    }

    fn set_notice(&mut self, text: impl Into<String>, error: bool) {
        self.notice = Some(Notice {
            text: text.into(),
            error,
            at: Instant::now(),
        });
    }

    fn try_snap(&self, ctx: &egui::Context) -> bool {
        let Some(monitor) = ctx.input(|input| input.viewport().monitor_size) else {
            return false;
        };
        let size = ctx.input(|input| {
            input
                .viewport()
                .outer_rect
                .map(|rect| rect.size())
                .unwrap_or(Vec2::new(330.0, 660.0))
        });
        let pos = snap_position(self.cfg.edge, monitor, size);
        ctx.send_viewport_cmd(ViewportCommand::OuterPosition(pos));
        true
    }

    /// Erkennt, an welcher Kante das Fenster steht, und wechselt bei Bedarf
    /// die Seite (inkl. Senderichtung). Läuft nur, wenn das Fenster kurz
    /// ruhig steht und niemand mehr zieht.
    fn follow_window_edge(&mut self, ctx: &egui::Context) {
        if !self.snapped || Instant::now() < self.snap_settle_until {
            return;
        }
        if ctx.input(|input| input.pointer.any_down()) {
            self.last_window_pos = None;
            return;
        }
        let Some(monitor) = ctx.input(|input| input.viewport().monitor_size) else {
            return;
        };
        let Some(rect) = ctx.input(|input| input.viewport().outer_rect) else {
            return;
        };
        if monitor.x <= 1.0 {
            return;
        }

        let pos = rect.min;
        let settled = match self.last_window_pos {
            Some(previous) if previous.distance(pos) <= 0.5 => {
                self.last_window_move.elapsed() >= Duration::from_millis(300)
            }
            _ => {
                self.last_window_pos = Some(pos);
                self.last_window_move = Instant::now();
                false
            }
        };
        if !settled {
            return;
        }

        let detected = side_for_position(monitor, rect.center());
        if detected != self.cfg.edge {
            self.switch_edge(detected);
            return;
        }

        // Innerhalb derselben Seite: nach dem Ziehen wieder andocken, wenn das
        // Fenster frei auf dem Bildschirm steht.
        let near_left = pos.x <= 40.0;
        let near_right = pos.x + rect.width() >= monitor.x - 40.0;
        if !near_left && !near_right {
            self.snap_requested = true;
            self.snapped = false;
            self.last_window_pos = None;
        }
    }

    /// Wechselt die Bildschirmseite, startet Empfänger/Erkennung neu und
    /// verwirft ein Ziel, das nicht mehr zur neuen Seite passt.
    fn switch_edge(&mut self, edge: Side) {
        if self.cfg.edge == edge {
            return;
        }
        self.cfg.edge = edge;
        self.settings.edge = edge;

        let target = self
            .shared
            .lock()
            .map(|model| model.target.clone())
            .unwrap_or_default();
        let incompatible = self.shared.lock().ok().and_then(|model| {
            model
                .peers
                .iter()
                .find(|peer| peer.addr.to_string() == target)
                .filter(|peer| !edge.can_send_to(peer.edge))
                .map(|peer| peer.name.clone())
        });

        let cleared = if let Some(name) = incompatible {
            self.cfg.peer.clear();
            if let Ok(mut model) = self.shared.lock() {
                model.target.clear();
                model.peers.clear();
            }
            Some(name)
        } else {
            self.cfg.peer = target;
            None
        };

        let _ = self.cfg.save();
        self.restart_receiver();
        self.restart_discovery();
        self.snap_requested = true;
        self.snapped = false;
        self.last_window_pos = None;
        self.snap_settle_until = Instant::now() + Duration::from_millis(500);

        if self.receiver.is_none() {
            return;
        }
        if let Some(name) = cleared {
            self.set_notice(
                format!(
                    "Seite gewechselt: Ziel {name} entfernt, weil diese Leiste jetzt {} ist und \
                     nur an PCs {} sendet.",
                    edge.short(),
                    edge.opposite().short()
                ),
                true,
            );
        } else {
            self.set_notice(
                format!(
                    "Seite gewechselt: Leiste {}, sendet an PCs {}.",
                    edge.short(),
                    edge.opposite().short()
                ),
                false,
            );
        }
    }

    fn on_drop(&mut self, paths: Vec<PathBuf>) {
        let mut files = Vec::new();
        let mut rejected = Vec::new();
        for path in paths {
            match fs::metadata(&path) {
                Ok(meta) if meta.is_file() => files.push((path, meta.len())),
                Ok(_) => rejected.push(format!(
                    "{}: Ordner werden nicht unterstützt",
                    path.display()
                )),
                Err(err) => rejected.push(format!("{}: {err}", path.display())),
            }
        }
        if !rejected.is_empty() {
            self.set_notice(rejected.join(" · "), true);
        }
        if files.is_empty() {
            return;
        }

        let target = {
            let model = self.shared.lock().expect("Modell-Mutex vergiftet");
            util::normalize_target(&model.target, self.cfg.port)
        };
        if target.is_empty() {
            self.set_notice(
                "Kein Ziel-PC: Adresse eintragen oder auf einen gefundenen PC warten.",
                true,
            );
            return;
        }

        // Nur PCs auf der Gegenseite dürfen angesprochen werden.
        let incompatible = {
            let model = self.shared.lock().expect("Modell-Mutex vergiftet");
            model
                .peers
                .iter()
                .find(|peer| peer.addr.to_string() == target)
                .filter(|peer| !self.cfg.edge.can_send_to(peer.edge))
                .map(|peer| (peer.name.clone(), peer.edge))
        };
        if let Some((name, remote_edge)) = incompatible {
            self.set_notice(
                format!(
                    "{name} ist auf der {} Seite. Diese Leiste ist auf der {} Seite und sendet \
                     nur an PCs auf der {} Seite.",
                    remote_edge.label(),
                    self.cfg.edge.label(),
                    self.cfg.edge.opposite().label()
                ),
                true,
            );
            return;
        }

        {
            let mut model = self.shared.lock().expect("Modell-Mutex vergiftet");
            model.target = target.clone();
        }
        if self.cfg.peer != target {
            self.cfg.peer = target.clone();
            let _ = self.cfg.save();
        }

        let batch = self.next_batch.fetch_add(1, Ordering::Relaxed);
        let cancel = Arc::new(AtomicBool::new(false));
        let entry = Outgoing {
            id: batch,
            target: target.clone(),
            files: files
                .iter()
                .map(|(path, size)| OutFile {
                    name: path
                        .file_name()
                        .map(|name| name.to_string_lossy().into_owned())
                        .unwrap_or_else(|| "Datei".into()),
                    size: *size,
                    sent: 0,
                    saved_as: None,
                })
                .collect(),
            state: OutState::Sending,
            cancel: cancel.clone(),
            started: Instant::now(),
        };
        {
            let mut model = self.shared.lock().expect("Modell-Mutex vergiftet");
            model.outgoing.insert(0, entry);
            model.outgoing.truncate(12);
        }

        let paths: Vec<PathBuf> = files.into_iter().map(|(path, _)| path).collect();
        let shared = self.shared.clone();
        let ctx = self.ctx.clone();
        let token = self.cfg.token.clone();
        let sender = self.cfg.device_name.clone();
        let local_edge = self.cfg.edge;
        thread::spawn(move || {
            let _ = transfer::send_files(
                batch,
                &paths,
                &target,
                &token,
                &sender,
                local_edge,
                cancel,
                |event| {
                    if let Ok(mut model) = shared.lock() {
                        model.apply_send(event);
                    }
                    ctx.request_repaint();
                },
            );
        });
    }

    fn apply_settings(&mut self) {
        let target = self
            .shared
            .lock()
            .map(|model| model.target.clone())
            .unwrap_or_default();
        let mut cfg = self.cfg.clone();
        cfg.port = self.settings.port.max(1);
        cfg.token = self.settings.token.trim().to_string();
        cfg.download_dir = if self.settings.dir.trim().is_empty() {
            default_download_dir()
        } else {
            PathBuf::from(self.settings.dir.trim())
        };
        cfg.edge = self.settings.edge;
        // Ein Ziel, das nicht mehr zur neuen Kante passt, wird verworfen.
        let incompatible_target = self.shared.lock().ok().and_then(|model| {
            model
                .peers
                .iter()
                .find(|peer| peer.addr.to_string() == target)
                .filter(|peer| !cfg.edge.can_send_to(peer.edge))
                .map(|peer| peer.name.clone())
        });
        if let Some(name) = incompatible_target {
            cfg.peer = String::new();
            if let Ok(mut model) = self.shared.lock() {
                model.target.clear();
                model.peers.clear();
            }
            self.set_notice(
                format!(
                    "Ziel {name} passt nicht mehr: Diese Leiste sendet nur an PCs auf der {} Seite.",
                    cfg.edge.opposite().label()
                ),
                true,
            );
        } else {
            cfg.peer = target;
        }
        match cfg.save() {
            Ok(()) => {
                self.cfg = cfg.clone();
                self.settings = SettingsDraft::from_config(&cfg);
                self.restart_receiver();
                self.restart_discovery();
                self.snap_requested = true;
                self.snapped = false;
                self.last_window_pos = None;
                self.snap_settle_until = Instant::now() + Duration::from_millis(500);
                self.set_notice("Einstellungen gespeichert", false);
            }
            Err(err) => self.set_notice(format!("Speichern fehlgeschlagen: {err:#}"), true),
        }
    }

    fn header(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            let (rect, _) = ui.allocate_exact_size(Vec2::splat(10.0), Sense::hover());
            let status_color = if self.receiver.is_some() {
                ACCENT
            } else {
                WARN
            };
            ui.painter().circle_filled(rect.center(), 4.5, status_color);
            ui.label(RichText::new(APP_NAME).strong().size(17.0).color(TEXT));
            ui.label(RichText::new(format!("v{VERSION}")).size(10.0).color(MUTED));
            ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                if ui
                    .add(egui::Button::new(RichText::new("×").size(18.0).color(MUTED)).frame(false))
                    .on_hover_text("Beenden")
                    .clicked()
                {
                    ui.ctx().send_viewport_cmd(ViewportCommand::Close);
                }
                if ui
                    .add(
                        egui::Button::new(RichText::new("Rand").size(10.0).color(MUTED))
                            .frame(false),
                    )
                    .on_hover_text("An die Bildschirmkante andocken")
                    .clicked()
                {
                    self.snap_requested = true;
                    self.snapped = false;
                }
            });
        });
    }

    fn notice_banner(&self, ui: &mut egui::Ui) {
        let Some(notice) = &self.notice else {
            return;
        };
        let color = if notice.error { DANGER } else { ACCENT };
        let fill = if notice.error {
            Color32::from_rgb(48, 24, 30)
        } else {
            Color32::from_rgb(20, 42, 34)
        };
        ui.add_space(6.0);
        Frame::new()
            .fill(fill)
            .corner_radius(8.0)
            .inner_margin(Margin::symmetric(9, 7))
            .show(ui, |ui| {
                ui.add(Label::new(RichText::new(&notice.text).size(11.0).color(color)).wrap());
            });
    }

    fn drop_zone(&mut self, ui: &mut egui::Ui) {
        let hovered = self.hover_files > 0;
        let height = if hovered { 142.0 } else { 128.0 };
        let (rect, _) = ui.allocate_exact_size(
            Vec2::new(ui.available_width().max(1.0), height),
            Sense::hover(),
        );
        let painter = ui.painter();
        let fill = if hovered {
            ACCENT.gamma_multiply(0.12)
        } else {
            CARD
        };
        let stroke = Stroke::new(
            if hovered { 2.0 } else { 1.0 },
            if hovered { ACCENT } else { BORDER },
        );
        painter.rect(rect, 14.0, fill, stroke, StrokeKind::Inside);

        let accent = if hovered { ACCENT } else { MUTED };
        let center = rect.center();
        painter.arrow(
            center + Vec2::new(0.0, -36.0),
            Vec2::new(0.0, 28.0),
            Stroke::new(2.5, accent),
        );
        let tray_y = center.y + 8.0;
        painter.line_segment(
            [
                egui::pos2(center.x - 22.0, tray_y),
                egui::pos2(center.x + 22.0, tray_y),
            ],
            Stroke::new(2.0, accent),
        );
        painter.text(
            egui::pos2(center.x, center.y + 31.0),
            Align2::CENTER_CENTER,
            if hovered {
                "Loslassen zum Senden"
            } else {
                "Datei hier ablegen"
            },
            FontId::proportional(15.5),
            if hovered { ACCENT } else { TEXT },
        );
        painter.text(
            egui::pos2(center.x, center.y + 51.0),
            Align2::CENTER_CENTER,
            "ziehen & fallen lassen",
            FontId::proportional(10.5),
            MUTED,
        );
    }

    fn target_section(&mut self, ui: &mut egui::Ui) {
        let mut save_target = false;
        let expected = self.cfg.edge.opposite().label();
        Frame::new()
            .fill(CARD)
            .corner_radius(12.0)
            .inner_margin(Margin::same(10))
            .show(ui, |ui| {
                let mut model = match self.shared.lock() {
                    Ok(model) => model,
                    Err(_) => return,
                };
                ui.horizontal(|ui| {
                    ui.label(RichText::new("ZIEL-PC").size(10.0).color(MUTED).strong());
                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        let count = model.peers.len();
                        let (text, color) = if count > 0 {
                            (format!("{count} auf der {expected} Seite"), ACCENT)
                        } else {
                            (format!("suche auf der {expected} Seite …"), MUTED)
                        };
                        ui.label(RichText::new(text).size(9.5).color(color));
                    });
                });
                ui.add_space(2.0);

                if !model.peers.is_empty() {
                    let peers = model.peers.clone();
                    let current = model.target.clone();
                    let selected_text = peers
                        .iter()
                        .find(|peer| peer.addr.to_string() == current)
                        .map(|peer| format!("{} · {}", peer.name, peer.addr))
                        .unwrap_or_else(|| "PC auswählen …".to_string());
                    egui::ComboBox::from_id_salt("peer-select")
                        .selected_text(RichText::new(selected_text).size(11.5))
                        .width(ui.available_width())
                        .show_ui(ui, |ui| {
                            for peer in &peers {
                                let selected = peer.addr.to_string() == model.target;
                                if ui
                                    .selectable_label(
                                        selected,
                                        format!("{} · {}", peer.name, peer.addr),
                                    )
                                    .clicked()
                                {
                                    model.target = peer.addr.to_string();
                                    save_target = true;
                                }
                            }
                        });
                    ui.add_space(2.0);
                }

                let response = ui.add(
                    egui::TextEdit::singleline(&mut model.target)
                        .hint_text("192.168.1.42:45871")
                        .font(FontId::monospace(12.0))
                        .desired_width(f32::INFINITY),
                );
                if response.lost_focus() {
                    save_target = true;
                }
                ui.label(
                    RichText::new(format!("IP oder Hostname des PCs auf der {expected} Seite"))
                        .size(9.5)
                        .color(MUTED),
                );
            });

        if save_target {
            let target = self
                .shared
                .lock()
                .map(|model| model.target.clone())
                .unwrap_or_default();
            let normalized = util::normalize_target(&target, self.cfg.port);
            if !normalized.is_empty()
                && normalized != target
                && let Ok(mut model) = self.shared.lock()
            {
                model.target = normalized.clone();
            }
            if self.cfg.peer != normalized {
                self.cfg.peer = normalized;
                let _ = self.cfg.save();
            }
        }
    }

    fn settings_section(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            if ui
                .add(
                    egui::Button::new(
                        RichText::new("Einstellungen")
                            .size(11.0)
                            .color(if self.show_settings { ACCENT } else { MUTED }),
                    )
                    .frame(false),
                )
                .clicked()
            {
                self.show_settings = !self.show_settings;
            }
            ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                if ui
                    .add(
                        egui::Button::new(RichText::new("Empfangsordner").size(10.5).color(MUTED))
                            .frame(false),
                    )
                    .on_hover_text(self.cfg.download_dir.display().to_string())
                    .clicked()
                {
                    let _ = util::open_path(&self.cfg.download_dir);
                }
            });
        });

        if !self.show_settings {
            return;
        }

        ui.add_space(4.0);
        Frame::new()
            .fill(CARD)
            .corner_radius(12.0)
            .inner_margin(Margin::same(10))
            .show(ui, |ui| {
                egui::Grid::new("settings-grid")
                    .num_columns(2)
                    .spacing([8.0, 7.0])
                    .show(ui, |ui| {
                        ui.label(RichText::new("Port").size(11.0).color(MUTED));
                        ui.add(
                            egui::DragValue::new(&mut self.settings.port)
                                .range(1024..=65535)
                                .speed(1.0),
                        );
                        ui.end_row();

                        ui.label(RichText::new("Token").size(11.0).color(MUTED));
                        ui.add(
                            egui::TextEdit::singleline(&mut self.settings.token)
                                .hint_text("optional")
                                .password(true)
                                .desired_width(f32::INFINITY),
                        );
                        ui.end_row();

                        ui.label(RichText::new("Ordner").size(11.0).color(MUTED));
                        ui.add(
                            egui::TextEdit::singleline(&mut self.settings.dir)
                                .desired_width(f32::INFINITY),
                        );
                        ui.end_row();

                        ui.label(RichText::new("Kante").size(11.0).color(MUTED));
                        egui::ComboBox::from_id_salt("edge-select")
                            .selected_text(self.settings.edge.label())
                            .show_ui(ui, |ui| {
                                ui.selectable_value(
                                    &mut self.settings.edge,
                                    Side::Right,
                                    Side::Right.label(),
                                );
                                ui.selectable_value(
                                    &mut self.settings.edge,
                                    Side::Left,
                                    Side::Left.label(),
                                );
                            });
                        ui.end_row();
                    });
                ui.add_space(2.0);
                ui.add(
                    Label::new(
                        RichText::new(
                            "Tipp: Fenster auf die andere Bildschirmseite ziehen – die Leiste \
                             wechselt automatisch die Seite.",
                        )
                        .size(9.5)
                        .color(MUTED),
                    )
                    .wrap(),
                );
                ui.add_space(4.0);
                if ui
                    .add_sized(
                        [ui.available_width(), 26.0],
                        egui::Button::new(RichText::new("Speichern & neu starten").size(11.5)),
                    )
                    .clicked()
                {
                    self.apply_settings();
                }
            });
    }

    fn transfers(&mut self, ui: &mut egui::Ui) {
        let mut open_after: Option<PathBuf> = None;
        let mut cancel_after: Option<Arc<AtomicBool>> = None;
        {
            let model = match self.shared.lock() {
                Ok(model) => model,
                Err(_) => return,
            };

            if model.outgoing.is_empty() && model.incoming.is_empty() {
                ui.add_space(2.0);
                ui.label(
                    RichText::new(format!("Empfang bereit · Port {}", self.cfg.port))
                        .size(10.5)
                        .color(MUTED),
                );
                return;
            }

            let height = (ui.available_height() - 34.0).max(120.0);
            egui::ScrollArea::vertical()
                .auto_shrink([false, false])
                .max_height(height)
                .show(ui, |ui| {
                    if !model.outgoing.is_empty() {
                        ui.label(RichText::new("GESENDET").size(9.5).color(MUTED).strong());
                        ui.add_space(2.0);
                        for item in &model.outgoing {
                            outgoing_card(ui, item, &mut cancel_after);
                            ui.add_space(5.0);
                        }
                    }
                    if !model.incoming.is_empty() {
                        ui.add_space(4.0);
                        ui.label(RichText::new("EMPFANGEN").size(9.5).color(MUTED).strong());
                        ui.add_space(2.0);
                        for item in &model.incoming {
                            incoming_card(ui, item, &mut open_after);
                            ui.add_space(5.0);
                        }
                    }
                });
        }

        if let Some(cancel) = cancel_after {
            cancel.store(true, Ordering::Relaxed);
        }
        if let Some(path) = open_after {
            let _ = util::open_path(&path);
        }
    }

    fn footer(&mut self, ui: &mut egui::Ui) {
        ui.add_space(4.0);
        ui.horizontal(|ui| {
            let address = match self.local_ip {
                Some(ip) => format!("{ip}:{}", self.cfg.port),
                None => format!("Port {}", self.cfg.port),
            };
            ui.add(
                Label::new(
                    RichText::new(format!(
                        "Dieser PC · {address} · {} → {}",
                        self.cfg.edge.label(),
                        self.cfg.edge.opposite().label()
                    ))
                    .size(9.5)
                    .color(MUTED),
                )
                .truncate(),
            );
            ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                let (text, color) = if self.receiver.is_some() {
                    ("Empfang aktiv", ACCENT)
                } else {
                    ("Empfang inaktiv", WARN)
                };
                ui.label(RichText::new(text).size(9.5).color(color));
            });
        });
    }
}

impl eframe::App for FilesendApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx = ui.ctx().clone();

        let dropped: Vec<PathBuf> = ctx.input(|input| {
            input
                .raw
                .dropped_files
                .iter()
                .map(|file| file.path().to_path_buf())
                .collect()
        });
        if !dropped.is_empty() {
            self.on_drop(dropped);
        }
        self.hover_files = ctx.input(|input| input.raw.hovered_files.len());

        if (self.snap_requested || !self.snapped) && self.try_snap(&ctx) {
            self.snap_requested = false;
            self.snapped = true;
            self.last_window_pos = None;
            self.snap_settle_until = Instant::now() + Duration::from_millis(500);
        }
        self.follow_window_edge(&ctx);

        if self
            .notice
            .as_ref()
            .is_some_and(|notice| notice.at.elapsed() > Duration::from_secs(9))
        {
            self.notice = None;
        }

        egui::CentralPanel::default()
            .frame(Frame::new().fill(BG).inner_margin(Margin::same(13)))
            .show(ui, |ui| {
                self.header(ui);
                self.notice_banner(ui);
                ui.add_space(10.0);
                self.drop_zone(ui);
                ui.add_space(10.0);
                self.target_section(ui);
                ui.add_space(5.0);
                self.settings_section(ui);
                ui.add_space(7.0);
                self.transfers(ui);
                self.footer(ui);
            });

        let busy = self
            .shared
            .lock()
            .map(|model| {
                model
                    .outgoing
                    .iter()
                    .any(|item| matches!(item.state, OutState::Sending))
                    || model
                        .incoming
                        .iter()
                        .any(|item| item.state == InState::Receiving)
            })
            .unwrap_or(false);
        if busy || self.notice.is_some() {
            ctx.request_repaint_after(Duration::from_millis(250));
        }
    }
}

impl Drop for FilesendApp {
    fn drop(&mut self) {
        self.discovery_stop.store(true, Ordering::Relaxed);
        if let Some(mut receiver) = self.receiver.take() {
            receiver.shutdown();
        }
    }
}

fn outgoing_card(ui: &mut egui::Ui, item: &Outgoing, cancel_after: &mut Option<Arc<AtomicBool>>) {
    let (color, status) = match &item.state {
        OutState::Sending => (
            ACCENT,
            format!(
                "{:.0}% · {}/s",
                item.fraction() * 100.0,
                util::format_bytes(item.speed() as u64)
            ),
        ),
        OutState::Done => (ACCENT, "Gesendet".to_string()),
        OutState::Cancelled => (MUTED, "Abgebrochen".to_string()),
        OutState::Failed(_) => (DANGER, "Fehlgeschlagen".to_string()),
    };

    Frame::new()
        .fill(CARD)
        .corner_radius(10.0)
        .inner_margin(Margin::same(9))
        .show(ui, |ui| {
            ui.horizontal(|ui| {
                let (dot, _) = ui.allocate_exact_size(Vec2::splat(8.0), Sense::hover());
                ui.painter().circle_filled(dot.center(), 3.5, color);
                ui.add(
                    Label::new(RichText::new(item.title()).size(12.0).color(TEXT).strong())
                        .truncate(),
                );
                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                    if matches!(item.state, OutState::Sending)
                        && ui.small_button("×").on_hover_text("Abbrechen").clicked()
                    {
                        *cancel_after = Some(item.cancel.clone());
                    }
                    ui.label(RichText::new(status).size(9.5).color(color));
                });
            });
            if matches!(item.state, OutState::Sending) {
                ui.add_space(2.0);
                ui.add(
                    egui::ProgressBar::new(item.fraction())
                        .desired_height(5.0)
                        .fill(ACCENT),
                );
            }
            let detail = match &item.state {
                OutState::Done => {
                    if item.files.len() == 1 {
                        match &item.files[0].saved_as {
                            Some(saved) if saved != &item.files[0].name => {
                                format!("Gespeichert als {saved} · {}", item.target)
                            }
                            _ => format!(
                                "{} · {}",
                                util::format_bytes(item.total_bytes()),
                                item.target
                            ),
                        }
                    } else {
                        format!(
                            "{} Dateien · {} · {}",
                            item.files.len(),
                            util::format_bytes(item.total_bytes()),
                            item.target
                        )
                    }
                }
                OutState::Failed(error) => error.clone(),
                OutState::Cancelled => format!(
                    "{} · {}",
                    util::format_bytes(item.sent_bytes()),
                    item.target
                ),
                OutState::Sending => {
                    if item.files.len() > 1 {
                        let done = item
                            .files
                            .iter()
                            .filter(|file| file.sent >= file.size)
                            .count();
                        format!(
                            "{done}/{} Dateien · {}",
                            item.files.len(),
                            util::format_bytes(item.total_bytes())
                        )
                    } else {
                        format!(
                            "{} von {}",
                            util::format_bytes(item.sent_bytes()),
                            util::format_bytes(item.total_bytes())
                        )
                    }
                }
            };
            ui.add(Label::new(RichText::new(detail).size(9.5).color(MUTED)).wrap());
        });
}

fn incoming_card(ui: &mut egui::Ui, item: &Incoming, open_after: &mut Option<PathBuf>) {
    let (color, status) = match item.state {
        InState::Receiving => (ACCENT, "Empfange …".to_string()),
        InState::Saved => (ACCENT, "Gespeichert".to_string()),
        InState::Failed => (DANGER, "Fehlgeschlagen".to_string()),
    };
    let fraction = if item.size == 0 {
        if item.state == InState::Receiving {
            0.35
        } else {
            1.0
        }
    } else {
        (item.received as f64 / item.size as f64) as f32
    };

    Frame::new()
        .fill(CARD_HOVER)
        .corner_radius(10.0)
        .inner_margin(Margin::same(9))
        .show(ui, |ui| {
            ui.horizontal(|ui| {
                let (dot, _) = ui.allocate_exact_size(Vec2::splat(8.0), Sense::hover());
                ui.painter().circle_filled(dot.center(), 3.5, color);
                ui.add(
                    Label::new(RichText::new(&item.name).size(12.0).color(TEXT).strong())
                        .truncate(),
                );
                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                    if item.state == InState::Saved && ui.small_button("Öffnen").clicked() {
                        *open_after = item.path.clone();
                    }
                    ui.label(RichText::new(status).size(9.5).color(color));
                });
            });
            if item.state == InState::Receiving {
                ui.add_space(2.0);
                ui.add(
                    egui::ProgressBar::new(fraction)
                        .desired_height(5.0)
                        .fill(ACCENT),
                );
            }
            let detail = match item.state {
                InState::Saved => {
                    format!("{} · von {}", util::format_bytes(item.size), item.from)
                }
                InState::Failed => item
                    .error
                    .clone()
                    .unwrap_or_else(|| "Unbekannter Fehler".into()),
                InState::Receiving => format!(
                    "{} von {} · von {}",
                    util::format_bytes(item.received),
                    util::format_bytes(item.size),
                    item.from
                ),
            };
            ui.add(Label::new(RichText::new(detail).size(9.5).color(MUTED)).wrap());
        });

    // Der Hash ist nützlich zur Kontrolle, soll aber nicht stören.
    let _ = &item.sha256;
}

fn install_style(ctx: &egui::Context) {
    ctx.set_theme(egui::ThemePreference::Dark);
    let mut visuals = egui::Visuals::dark();
    visuals.panel_fill = BG;
    visuals.window_fill = BG;
    visuals.extreme_bg_color = INPUT_BG;
    visuals.faint_bg_color = CARD;
    visuals.override_text_color = Some(TEXT);
    visuals.widgets.noninteractive.bg_fill = CARD;
    visuals.widgets.noninteractive.bg_stroke = Stroke::new(1.0, BORDER);
    visuals.widgets.inactive.bg_fill = Color32::from_rgb(35, 39, 49);
    visuals.widgets.inactive.weak_bg_fill = Color32::from_rgb(29, 33, 42);
    visuals.widgets.inactive.fg_stroke = Stroke::new(1.0, TEXT);
    visuals.widgets.hovered.bg_fill = Color32::from_rgb(45, 51, 64);
    visuals.widgets.hovered.weak_bg_fill = Color32::from_rgb(39, 44, 56);
    visuals.widgets.hovered.fg_stroke = Stroke::new(1.0, ACCENT);
    visuals.widgets.active.bg_fill = ACCENT_DARK;
    visuals.widgets.active.weak_bg_fill = ACCENT_DARK;
    visuals.widgets.active.fg_stroke = Stroke::new(1.0, TEXT);
    visuals.selection.bg_fill = ACCENT_DARK;
    visuals.hyperlink_color = ACCENT;
    ctx.set_visuals(visuals);

    ctx.all_styles_mut(|style| {
        style.spacing.item_spacing = egui::vec2(8.0, 6.0);
        style.spacing.button_padding = egui::vec2(8.0, 4.0);
        style.spacing.interact_size.y = 26.0;
    });
}

/// Fensterposition für eine Bildschirmkante (10 px Abstand, vertikal zentriert).
fn snap_position(edge: Side, monitor: Vec2, size: Vec2) -> egui::Pos2 {
    let margin = 10.0;
    let x = match edge {
        Side::Right => (monitor.x - size.x - margin).max(margin),
        Side::Left => margin,
    };
    let y = ((monitor.y - size.y) * 0.5).max(margin);
    egui::pos2(x, y)
}

/// Auf welcher Seite das Fenster anhand seiner Mitte steht.
fn side_for_position(monitor: Vec2, center: egui::Pos2) -> Side {
    if center.x < monitor.x * 0.5 {
        Side::Left
    } else {
        Side::Right
    }
}

/// Fensteroptionen: schmale, rahmenlose Leiste, die immer oben liegt.
pub fn native_options() -> eframe::NativeOptions {
    eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_title(APP_NAME)
            .with_inner_size(egui::vec2(330.0, 660.0))
            .with_min_inner_size(egui::vec2(290.0, 440.0))
            .with_max_inner_size(egui::vec2(440.0, 1000.0))
            .with_decorations(false)
            .with_resizable(true)
            .with_always_on_top()
            .with_movable_by_background(true),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outgoing(id: u64, size: u64) -> Outgoing {
        Outgoing {
            id,
            target: "192.168.1.20:45871".into(),
            files: vec![OutFile {
                name: "urlaub.jpg".into(),
                size,
                sent: 0,
                saved_as: None,
            }],
            state: OutState::Sending,
            cancel: Arc::new(AtomicBool::new(false)),
            started: Instant::now(),
        }
    }

    #[test]
    fn detects_side_from_window_center() {
        let monitor = egui::vec2(1920.0, 1080.0);
        assert_eq!(
            side_for_position(monitor, egui::pos2(200.0, 500.0)),
            Side::Left
        );
        assert_eq!(
            side_for_position(monitor, egui::pos2(1700.0, 500.0)),
            Side::Right
        );
        assert_eq!(
            side_for_position(monitor, egui::pos2(960.0, 500.0)),
            Side::Right
        );
    }

    #[test]
    fn snap_position_sticks_to_edges() {
        let monitor = egui::vec2(1920.0, 1080.0);
        let size = egui::vec2(330.0, 660.0);
        assert_eq!(snap_position(Side::Left, monitor, size).x, 10.0);
        assert_eq!(snap_position(Side::Right, monitor, size).x, 1580.0);
        assert_eq!(snap_position(Side::Left, monitor, size).y, 210.0);
    }

    #[test]
    fn send_progress_reaches_done() {
        let mut model = AppModel::default();
        model.outgoing.push(outgoing(1, 1000));
        model.apply_send(SendEvent::Progress {
            batch: 1,
            index: 0,
            sent: 500,
            total: 1000,
        });
        assert!((model.outgoing[0].fraction() - 0.5).abs() < f32::EPSILON);
        model.apply_send(SendEvent::FileSaved {
            batch: 1,
            index: 0,
            name: "urlaub.jpg".into(),
            saved_as: "urlaub (1).jpg".into(),
            sha256: "abc".into(),
        });
        assert_eq!(
            model.outgoing[0].files[0].saved_as.as_deref(),
            Some("urlaub (1).jpg")
        );
        model.apply_send(SendEvent::Finished { batch: 1, count: 1 });
        assert!(matches!(model.outgoing[0].state, OutState::Done));
        assert!((model.outgoing[0].fraction() - 1.0).abs() < f32::EPSILON);
    }

    #[test]
    fn cancelled_and_failed_are_distinguished() {
        let mut model = AppModel::default();
        model.outgoing.push(outgoing(1, 10));
        model.outgoing.push(outgoing(2, 10));
        model.apply_send(SendEvent::Failed {
            batch: 1,
            error: "abgebrochen".into(),
            cancelled: true,
        });
        model.apply_send(SendEvent::Failed {
            batch: 2,
            error: "keine Verbindung".into(),
            cancelled: false,
        });
        assert!(matches!(model.outgoing[0].state, OutState::Cancelled));
        assert!(matches!(
            &model.outgoing[1].state,
            OutState::Failed(message) if message == "keine Verbindung"
        ));
    }

    #[test]
    fn receive_events_update_existing_entry() {
        let mut model = AppModel::default();
        model.apply_receive(ReceiveEvent::Started {
            id: 9,
            name: "foto.png".into(),
            size: 200,
            from: "192.168.1.20:50000".into(),
        });
        model.apply_receive(ReceiveEvent::Progress {
            id: 9,
            received: 200,
            total: 200,
        });
        model.apply_receive(ReceiveEvent::Saved {
            id: 9,
            name: "foto.png".into(),
            saved_as: "foto (1).png".into(),
            path: PathBuf::from("/tmp/foto (1).png"),
            size: 200,
            sha256: "deadbeef".into(),
            from: "192.168.1.20:50000".into(),
        });
        assert_eq!(model.incoming.len(), 1);
        assert_eq!(model.incoming[0].state, InState::Saved);
        assert_eq!(model.incoming[0].saved_as.as_deref(), Some("foto (1).png"));
        assert_eq!(model.incoming[0].received, 200);
    }

    #[test]
    fn receive_failure_creates_entry() {
        let mut model = AppModel::default();
        model.apply_receive(ReceiveEvent::Failed {
            id: 4,
            name: "kaputt.bin".into(),
            error: "Prüfsumme falsch".into(),
        });
        assert_eq!(model.incoming.len(), 1);
        assert_eq!(model.incoming[0].state, InState::Failed);
        assert_eq!(model.incoming[0].error.as_deref(), Some("Prüfsumme falsch"));
    }
}
