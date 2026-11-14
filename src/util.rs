//! Kleine plattformübergreifende Helfer.

use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::path::Path;
use std::process::Command;

/// Formatiert eine Byteanzahl menschenlesbar, z. B. `4,2 MB`.
pub fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KB", "MB", "GB", "TB", "PB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if value >= 100.0 {
        format!("{value:.0} {}", UNITS[unit])
    } else if value >= 10.0 {
        format!("{value:.1} {}", UNITS[unit])
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}

/// Name dieses PCs, wie er anderen angezeigt wird.
pub fn default_device_name() -> String {
    for key in ["COMPUTERNAME", "HOSTNAME", "USER"] {
        if let Ok(value) = std::env::var(key) {
            let value = value.trim().trim_end_matches(".local");
            if !value.is_empty() {
                return value.to_string();
            }
        }
    }
    "Mein PC".to_string()
}

/// Primäre lokale IPv4-Adresse (ohne Pakete zu senden).
pub fn local_ip() -> Option<IpAddr> {
    let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).ok()?;
    // Ein UDP-"connect" verschickt nichts, wählt aber die Route/Quelladresse.
    socket.connect((Ipv4Addr::new(8, 8, 8, 8), 80)).ok()?;
    Some(socket.local_addr().ok()?.ip())
}

/// Ergänzt bei Bedarf den Standardport und normalisiert die Eingabe.
pub fn normalize_target(input: &str, default_port: u16) -> String {
    let target = input.trim();
    if target.is_empty() {
        return String::new();
    }
    if let Ok(addr) = target.parse::<SocketAddr>() {
        return addr.to_string();
    }
    if let Ok(ip) = target.parse::<IpAddr>() {
        return SocketAddr::new(ip, default_port).to_string();
    }
    // Hostname ohne Port (IPv6-Sonderfälle bleiben unangetastet).
    if !target.contains(':') {
        return format!("{target}:{default_port}");
    }
    target.to_string()
}

/// Öffnet eine Datei bzw. einen Ordner im Dateimanager des Systems.
pub fn open_path(path: &Path) -> std::io::Result<()> {
    #[cfg(target_os = "macos")]
    let mut command = {
        let mut cmd = Command::new("open");
        cmd.arg(path);
        cmd
    };
    #[cfg(target_os = "windows")]
    let mut command = {
        let mut cmd = Command::new("explorer");
        cmd.arg(path);
        cmd
    };
    #[cfg(all(unix, not(target_os = "macos")))]
    let mut command = {
        let mut cmd = Command::new("xdg-open");
        cmd.arg(path);
        cmd
    };
    command.spawn().map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(999), "999 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(5 * 1024 * 1024), "5.00 MB");
        assert_eq!(format_bytes(3 * 1024 * 1024 * 1024), "3.00 GB");
    }

    #[test]
    fn normalizes_targets() {
        assert_eq!(normalize_target("192.168.1.7", 45871), "192.168.1.7:45871");
        assert_eq!(
            normalize_target("192.168.1.7:1234", 45871),
            "192.168.1.7:1234"
        );
        assert_eq!(
            normalize_target("wohnzimmer-pc", 45871),
            "wohnzimmer-pc:45871"
        );
        assert_eq!(normalize_target("  ", 45871), "");
        assert_eq!(normalize_target("[::1]:9000", 45871), "[::1]:9000");
    }
}
