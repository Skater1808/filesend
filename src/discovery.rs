//! Automatische Erkennung von FileSend-Empfängern im lokalen Netz.
//!
//! Der Sender schickt ein UDP-Broadcast-Paket, jeder laufende Empfänger
//! antwortet mit Name und TCP-Port. So muss man keine IP-Adressen abtippen.

use crate::config::Side;
use serde::{Deserialize, Serialize};
use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::protocol::PROTOCOL_VERSION;

pub const DISCOVERY_MAGIC: &str = "filesend-discovery";
pub const DISCOVERY_REQUEST: &[u8] = b"FILESEND_DISCOVER 1";
const MAX_DATAGRAM: usize = 2048;

/// Antwort eines Empfängers auf die Erkennungsanfrage.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Announcement {
    pub magic: String,
    pub version: u32,
    pub name: String,
    pub tcp_port: u16,
    /// An welcher Bildschirmkante der PC sitzt. Nur die Gegenseite ist kompatibel.
    #[serde(default)]
    pub edge: Side,
}

/// Ein gefundener PC.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Peer {
    pub name: String,
    pub addr: SocketAddr,
    pub edge: Side,
}

/// Startet den UDP-Antwortdienst. Der Socket wird vom Aufrufer erzeugt,
/// damit TCP und UDP denselben Port benutzen können.
pub fn start_responder(
    socket: UdpSocket,
    name: String,
    tcp_port: u16,
    edge: Side,
    stop: Arc<AtomicBool>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let announcement = Announcement {
            magic: DISCOVERY_MAGIC.to_string(),
            version: PROTOCOL_VERSION,
            name,
            tcp_port,
            edge,
        };
        let Ok(payload) = serde_json::to_vec(&announcement) else {
            return;
        };
        let mut buffer = [0u8; 256];
        while !stop.load(Ordering::Relaxed) {
            match socket.recv_from(&mut buffer) {
                Ok((length, from)) => {
                    if &buffer[..length] == DISCOVERY_REQUEST {
                        let _ = socket.send_to(&payload, from);
                    }
                }
                Err(err)
                    if matches!(
                        err.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) => {}
                Err(_) => break,
            }
        }
    })
}

/// Sucht Empfänger im lokalen Netz. Blockiert höchstens `timeout`.
pub fn discover(port: u16, timeout: Duration) -> Vec<Peer> {
    let socket = match UdpSocket::bind(("0.0.0.0", 0)) {
        Ok(socket) => socket,
        Err(_) => return Vec::new(),
    };
    let _ = socket.set_broadcast(true);
    let _ = socket.set_read_timeout(Some(Duration::from_millis(50)));

    // Broadcast für andere PCs, zusätzlich Loopback für denselben Rechner.
    let _ = socket.send_to(DISCOVERY_REQUEST, ("255.255.255.255", port));
    let _ = socket.send_to(DISCOVERY_REQUEST, ("127.0.0.1", port));

    let deadline = Instant::now() + timeout;
    let mut peers: Vec<Peer> = Vec::new();
    let mut buffer = [0u8; MAX_DATAGRAM];
    while Instant::now() < deadline {
        match socket.recv_from(&mut buffer) {
            Ok((length, from)) => {
                let Ok(announcement) = serde_json::from_slice::<Announcement>(&buffer[..length])
                else {
                    continue;
                };
                if announcement.magic != DISCOVERY_MAGIC
                    || announcement.version != PROTOCOL_VERSION
                    || announcement.tcp_port == 0
                {
                    continue;
                }
                let peer = Peer {
                    name: announcement.name,
                    addr: SocketAddr::new(from.ip(), announcement.tcp_port),
                    edge: announcement.edge,
                };
                if !peers.contains(&peer) {
                    peers.push(peer);
                }
            }
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(_) => break,
        }
    }
    peers
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::PROTOCOL_MAGIC;

    #[test]
    fn announcement_roundtrip() {
        let announcement = Announcement {
            magic: DISCOVERY_MAGIC.into(),
            version: PROTOCOL_VERSION,
            name: "Küche".into(),
            tcp_port: 45871,
            edge: Side::Left,
        };
        let json = serde_json::to_vec(&announcement).unwrap();
        let parsed: Announcement = serde_json::from_slice(&json).unwrap();
        assert_eq!(parsed, announcement);
    }

    #[test]
    fn protocol_magic_is_shared() {
        assert_eq!(PROTOCOL_MAGIC, "filesend");
    }
}
