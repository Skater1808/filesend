//! FileSend – Dateien per Drag & Drop an den PC daneben senden.
//!
//! Die Bibliothek enthält alles außer der grafischen Oberfläche:
//! Konfiguration, das Netzwerkprotokoll, Sender/Empfänger und die
//! automatische Peer-Erkennung im lokalen Netzwerk.

pub mod config;
pub mod discovery;
pub mod protocol;
pub mod transfer;
pub mod util;

pub const APP_NAME: &str = "FileSend";
pub const DEFAULT_PORT: u16 = 45_871;
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
