//! Konfiguration, gespeichert unter dem plattformüblichen Config-Verzeichnis.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

use crate::util;
use crate::{APP_NAME, DEFAULT_PORT};

/// An welcher Bildschirmkante das Fenster andockt.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    #[default]
    Right,
    Left,
}

impl Side {
    pub fn label(self) -> &'static str {
        match self {
            Self::Right => "Rechts",
            Self::Left => "Links",
        }
    }

    /// Kleinschreibung für Sätze („auf der linken Seite“).
    pub fn short(self) -> &'static str {
        match self {
            Self::Right => "rechts",
            Self::Left => "links",
        }
    }

    /// Die Seite, auf der der PC daneben steht.
    pub fn opposite(self) -> Self {
        match self {
            Self::Right => Self::Left,
            Self::Left => Self::Right,
        }
    }

    /// Nur PCs auf der Gegenseite werden angesprochen.
    pub fn can_send_to(self, remote: Self) -> bool {
        remote == self.opposite()
    }

    /// `--edge left|rechts|…` für die Kommandozeile.
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "left" | "links" | "l" => Some(Self::Left),
            "right" | "rechts" | "r" => Some(Self::Right),
            _ => None,
        }
    }
}

/// Persistente Einstellungen der App.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// TCP-Port für Übertragungen (UDP-Port für die Erkennung ist identisch).
    pub port: u16,
    /// Ziel-PC als `host:port`. Leer = automatisch den ersten gefundenen PC nehmen.
    pub peer: String,
    /// Optionales gemeinsames Geheimnis. Leer = keine Prüfung.
    pub token: String,
    /// Ordner, in dem empfangene Dateien landen.
    pub download_dir: PathBuf,
    /// Name dieses PCs, der anderen angezeigt wird.
    pub device_name: String,
    /// Bildschirmkante, an der das Fenster sitzt.
    pub edge: Side,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            port: DEFAULT_PORT,
            peer: String::new(),
            token: String::new(),
            download_dir: default_download_dir(),
            device_name: util::default_device_name(),
            edge: Side::Right,
        }
    }
}

impl Config {
    /// Lädt die Konfiguration. Bei Fehlern werden Standardwerte benutzt.
    pub fn load() -> Self {
        let path = config_path();
        let Ok(text) = fs::read_to_string(&path) else {
            return Self::default();
        };
        match serde_json::from_str(&text) {
            Ok(cfg) => cfg,
            Err(err) => {
                eprintln!(
                    "Konfiguration {} ist ungültig ({err}); benutze Standardwerte.",
                    path.display()
                );
                Self::default()
            }
        }
    }

    /// Speichert die Konfiguration atomar (temp-Datei + rename).
    pub fn save(&self) -> Result<()> {
        let path = config_path();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Konfigurationsordner {} anlegen", parent.display()))?;
        }
        let json = serde_json::to_string_pretty(self).context("Konfiguration serialisieren")?;
        let tmp = path.with_extension("json.tmp");
        fs::write(&tmp, json).with_context(|| format!("{} schreiben", tmp.display()))?;
        if path.exists() {
            // Windows erlaubt kein rename über eine existierende Datei.
            let _ = fs::remove_file(&path);
        }
        fs::rename(&tmp, &path)
            .with_context(|| format!("Konfiguration nach {} verschieben", path.display()))?;
        Ok(())
    }
}

/// Vollständiger Pfad der Konfigurationsdatei.
pub fn config_path() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("filesend")
        .join("config.json")
}

/// Standard-Empfangsordner: `Downloads/FileSend`.
pub fn default_download_dir() -> PathBuf {
    dirs::download_dir()
        .or_else(|| dirs::home_dir().map(|home| home.join("Downloads")))
        .unwrap_or_else(|| PathBuf::from("."))
        .join(APP_NAME)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_roundtrip_preserves_values() {
        let cfg = Config {
            port: 12345,
            peer: "192.168.1.20:12345".into(),
            token: "geheim".into(),
            download_dir: PathBuf::from("/tmp/filesend-test"),
            device_name: "Test-PC".into(),
            edge: Side::Left,
        };
        let json = serde_json::to_string(&cfg).unwrap();
        let back: Config = serde_json::from_str(&json).unwrap();
        assert_eq!(back.port, 12345);
        assert_eq!(back.peer, "192.168.1.20:12345");
        assert_eq!(back.token, "geheim");
        assert_eq!(back.download_dir, PathBuf::from("/tmp/filesend-test"));
        assert_eq!(back.device_name, "Test-PC");
        assert_eq!(back.edge, Side::Left);
    }

    #[test]
    fn sides_only_pair_with_opposite_sides() {
        assert_eq!(Side::Right.opposite(), Side::Left);
        assert_eq!(Side::Left.opposite(), Side::Right);
        assert!(Side::Right.can_send_to(Side::Left));
        assert!(Side::Left.can_send_to(Side::Right));
        assert!(!Side::Right.can_send_to(Side::Right));
        assert!(!Side::Left.can_send_to(Side::Left));
        assert_eq!(Side::parse("links"), Some(Side::Left));
        assert_eq!(Side::parse("RIGHT"), Some(Side::Right));
        assert_eq!(Side::parse("oben"), None);
    }

    #[test]
    fn missing_fields_get_defaults() {
        let cfg: Config = serde_json::from_str(r#"{"port": 9999}"#).unwrap();
        assert_eq!(cfg.port, 9999);
        assert_eq!(cfg.edge, Side::Right);
        assert!(!cfg.device_name.is_empty());
    }
}
