//! Drahtprotokoll von FileSend.
//!
//! Aufbau einer Verbindung (TCP):
//!
//! ```text
//! Client -> {"magic":"filesend","version":1,"token":"…","sender":"…","edge":"right"}\n
//! Server -> {"magic":"filesend","version":1,"ok":true,"receiver":"…","edge":"left"}\n
//!
//! pro Datei:
//! Client -> {"type":"file","name":"foto.jpg","size":123}\n
//! Client -> <genau `size` Bytes>
//! Client -> <32 Bytes SHA-256 der Datei>
//! Server -> {"ok":true,"saved_as":"foto.jpg","sha256":"…"}\n
//!
//! Abschluss:
//! Client -> {"type":"done","count":1}\n
//! Server -> {"ok":true,"received":1}\n
//! ```
//!
//! `edge` ist die Bildschirmseite (`left`/`right`). Sender und Empfänger
//! lehnen die Verbindung ab, wenn beide auf derselben Seite stehen.
//!
//! Jede Kontrollnachricht ist eine einzelne JSON-Zeile. Die Dateidaten
//! folgen binär und unverändert, damit auch große Dateien ohne Umweg über
//! den Speicher übertragen werden können.

use crate::config::Side;
use anyhow::{Context, Result, bail};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, ErrorKind, Read, Write};
use std::path::{Path, PathBuf};

pub const PROTOCOL_MAGIC: &str = "filesend";
pub const PROTOCOL_VERSION: u32 = 1;
/// Maximale Länge einer JSON-Kontrollzeile (Schutz vor Speicher-Overflow).
pub const MAX_CONTROL_LINE: usize = 64 * 1024;
pub const SHA256_LEN: usize = 32;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Hello {
    pub magic: String,
    pub version: u32,
    #[serde(default)]
    pub token: String,
    #[serde(default)]
    pub sender: String,
    /// Bildschirmseite des Senders. Nur die Gegenseite darf empfangen.
    #[serde(default)]
    pub edge: Side,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HelloReply {
    pub magic: String,
    pub version: u32,
    pub ok: bool,
    #[serde(default)]
    pub receiver: String,
    /// Bildschirmseite des Empfängers.
    #[serde(default)]
    pub edge: Side,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    File { name: String, size: u64 },
    Done { count: u64 },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileReply {
    pub ok: bool,
    #[serde(default)]
    pub saved_as: Option<String>,
    #[serde(default)]
    pub sha256: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DoneReply {
    pub ok: bool,
    #[serde(default)]
    pub received: u64,
    #[serde(default)]
    pub error: Option<String>,
}

/// Schreibt eine JSON-Kontrollnachricht inklusive Zeilenumbruch und flush.
pub fn write_json_line<W: Write, T: Serialize>(writer: &mut W, message: &T) -> Result<()> {
    serde_json::to_writer(&mut *writer, message).context("Kontrollnachricht serialisieren")?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

/// Liest eine JSON-Kontrollnachricht, begrenzt auf [`MAX_CONTROL_LINE`] Bytes.
pub fn read_json_line<R: BufRead, T: DeserializeOwned>(reader: &mut R) -> Result<T> {
    let mut buffer = Vec::new();
    let read = reader
        .by_ref()
        .take(MAX_CONTROL_LINE as u64 + 1)
        .read_until(b'\n', &mut buffer)?;
    if read == 0 {
        bail!("Die Gegenstelle hat die Verbindung geschlossen");
    }
    if buffer.len() > MAX_CONTROL_LINE {
        bail!("Kontrollnachricht ist zu groß");
    }
    if buffer.last() != Some(&b'\n') {
        bail!("Unvollständige Kontrollnachricht");
    }
    buffer.pop();
    if buffer.last() == Some(&b'\r') {
        buffer.pop();
    }
    serde_json::from_slice(&buffer).context("Ungültige Kontrollnachricht")
}

/// Prüft Dateinamen aus dem Netz und macht sie sicher für das lokale
/// Dateisystem. Pfadtrenner werden abgelehnt, damit niemand aus dem
/// Empfangsordner ausbrechen kann.
pub fn sanitize_file_name(raw: &str) -> Result<String> {
    if raw.is_empty() {
        bail!("Dateiname fehlt");
    }
    if raw.contains('/') || raw.contains('\\') {
        bail!("Dateiname darf keine Pfadtrenner enthalten");
    }
    if raw == "." || raw == ".." {
        bail!("Ungültiger Dateiname");
    }

    let mut name = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch == '\0' || ch.is_control() {
            bail!("Dateiname enthält Steuerzeichen");
        }
        name.push(match ch {
            '<' | '>' | ':' | '"' | '|' | '?' | '*' => '_',
            other => other,
        });
    }

    // Windows ignoriert Punkte/Leerzeichen am Ende.
    let name = name.trim().trim_end_matches(['.', ' ']).to_string();
    if name.is_empty() {
        bail!("Ungültiger Dateiname");
    }

    let name = truncate_name(&name, 180);
    let stem = name.split('.').next().unwrap_or("").to_ascii_uppercase();
    let reserved = matches!(
        stem.as_str(),
        "CON"
            | "PRN"
            | "AUX"
            | "NUL"
            | "COM1"
            | "COM2"
            | "COM3"
            | "COM4"
            | "COM5"
            | "COM6"
            | "COM7"
            | "COM8"
            | "COM9"
            | "LPT1"
            | "LPT2"
            | "LPT3"
            | "LPT4"
            | "LPT5"
            | "LPT6"
            | "LPT7"
            | "LPT8"
            | "LPT9"
    );
    Ok(if reserved { format!("_{name}") } else { name })
}

fn truncate_name(name: &str, max_bytes: usize) -> String {
    // Dateisysteme limitieren meist Bytes, nicht Zeichen.
    if name.len() <= max_bytes {
        return name.to_string();
    }
    let extension = name
        .rsplit_once('.')
        .filter(|(stem, ext)| !stem.is_empty() && ext.len() <= 32)
        .map(|(_, ext)| format!(".{ext}"))
        .unwrap_or_default();
    let keep = max_bytes.saturating_sub(extension.len());
    let mut stem = String::new();
    for ch in name.chars() {
        if stem.len() + ch.len_utf8() > keep {
            break;
        }
        stem.push(ch);
    }
    format!("{stem}{extension}")
}

/// Erzeugt im Zielordner eine neue Datei und weicht bei Namenskonflikten
/// auf `name (1).ext`, `name (2).ext`, … aus.
pub fn create_unique_file(dir: &Path, name: &str) -> Result<(PathBuf, File)> {
    for attempt in 0..10_000u32 {
        let candidate = if attempt == 0 {
            name.to_string()
        } else {
            numbered_name(name, attempt)
        };
        let path = dir.join(candidate);
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(err) if err.kind() == ErrorKind::AlreadyExists => continue,
            Err(err) => {
                return Err(err).with_context(|| format!("{} anlegen", path.display()));
            }
        }
    }
    bail!("Kein freier Dateiname für {name} gefunden")
}

fn numbered_name(name: &str, number: u32) -> String {
    match name.rfind('.') {
        Some(index) if index > 0 => {
            format!("{} ({number}){}", &name[..index], &name[index..])
        }
        _ => format!("{name} ({number})"),
    }
}

/// SHA-256 als Hex-String.
pub fn sha256_hex(data: &[u8]) -> String {
    hex(&Sha256::digest(data))
}

/// Bytes als Hex-String.
pub fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    out
}

/// Konstantzeit-Vergleich für das optionale Token.
pub fn token_matches(expected: &str, actual: &str) -> bool {
    let a = expected.as_bytes();
    let b = actual.as_bytes();
    let mut diff = a.len() ^ b.len();
    let len = a.len().max(b.len());
    for index in 0..len {
        let x = a.get(index).copied().unwrap_or(0);
        let y = b.get(index).copied().unwrap_or(0);
        diff |= (x ^ y) as usize;
    }
    diff == 0
}

/// Liest genau `size` Bytes aus `reader` in `file` und aktualisiert den Hasher.
pub fn copy_exact_hashing<R: Read>(
    reader: &mut R,
    file: &mut File,
    hash: &mut Sha256,
    size: u64,
    mut on_progress: impl FnMut(u64),
) -> Result<()> {
    let mut buffer = vec![0u8; 128 * 1024];
    let mut remaining = size;
    let mut done = 0u64;
    while remaining > 0 {
        let wanted = remaining.min(buffer.len() as u64) as usize;
        let read = reader.read(&mut buffer[..wanted])?;
        if read == 0 {
            bail!("Verbindung endete vorzeitig nach {done} von {size} Bytes");
        }
        file.write_all(&buffer[..read])?;
        hash.update(&buffer[..read]);
        remaining -= read as u64;
        done += read as u64;
        on_progress(done);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn json_line_roundtrip() {
        let message = ClientMessage::File {
            name: "urlaub.jpg".into(),
            size: 42,
        };
        let mut buffer = Vec::new();
        write_json_line(&mut buffer, &message).unwrap();
        assert!(buffer.ends_with(b"\n"));
        let mut reader = Cursor::new(buffer);
        let parsed: ClientMessage = read_json_line(&mut reader).unwrap();
        assert_eq!(parsed, message);
    }

    #[test]
    fn oversized_control_message_is_rejected() {
        let mut reader = Cursor::new(vec![b'x'; MAX_CONTROL_LINE + 10]);
        let result: Result<Hello> = read_json_line(&mut reader);
        assert!(result.is_err());
    }

    #[test]
    fn accepts_normal_names() {
        assert_eq!(
            sanitize_file_name("Urlaub 2026.jpg").unwrap(),
            "Urlaub 2026.jpg"
        );
        assert_eq!(sanitize_file_name(".gitignore").unwrap(), ".gitignore");
        assert_eq!(sanitize_file_name("a:b?.txt").unwrap(), "a_b_.txt");
        assert_eq!(sanitize_file_name("CON.txt").unwrap(), "_CON.txt");
        assert_eq!(sanitize_file_name("foo.   ").unwrap(), "foo");
    }

    #[test]
    fn rejects_traversal_and_control_characters() {
        assert!(sanitize_file_name("../evil.txt").is_err());
        assert!(sanitize_file_name("..\\evil.txt").is_err());
        assert!(sanitize_file_name("..").is_err());
        assert!(sanitize_file_name("").is_err());
        assert!(sanitize_file_name("bad\0name").is_err());
    }

    #[test]
    fn numbers_conflicting_names() {
        assert_eq!(numbered_name("a.txt", 1), "a (1).txt");
        assert_eq!(numbered_name("archiv.tar.gz", 2), "archiv.tar (2).gz");
        assert_eq!(numbered_name("README", 3), "README (3)");
        assert_eq!(numbered_name(".env", 4), ".env (4)");
    }

    #[test]
    fn shortens_long_names_and_keeps_extension() {
        let long = format!("{}.txt", "x".repeat(300));
        let short = sanitize_file_name(&long).unwrap();
        assert!(short.chars().count() <= 180);
        assert!(short.len() <= 180);
        assert!(short.ends_with(".txt"));

        // Mehrbytige Zeichen dürfen nicht mitten im UTF-8-Zeichen abgeschnitten werden.
        let umlauts = format!("{}.txt", "ä".repeat(300));
        let short = sanitize_file_name(&umlauts).unwrap();
        assert!(short.len() <= 180);
        assert!(short.ends_with(".txt"));
        assert!(short.is_char_boundary(short.len()));
    }

    #[test]
    fn token_comparison_is_exact() {
        assert!(token_matches("geheim", "geheim"));
        assert!(!token_matches("geheim", "Geheim"));
        assert!(!token_matches("geheim", "geheim "));
        assert!(!token_matches("", "x"));
        assert!(token_matches("", ""));
    }
}
