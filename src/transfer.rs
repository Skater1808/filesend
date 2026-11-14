//! Sender und Empfänger.

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::{BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs, UdpSocket};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::config::Side;
use crate::discovery;
use crate::protocol::{
    self, ClientMessage, DoneReply, FileReply, Hello, HelloReply, PROTOCOL_MAGIC, PROTOCOL_VERSION,
    SHA256_LEN, copy_exact_hashing, create_unique_file, read_json_line, sanitize_file_name,
    token_matches, write_json_line,
};

pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
pub const IO_TIMEOUT: Duration = Duration::from_secs(30);
const CHUNK_SIZE: usize = 128 * 1024;
const PROGRESS_INTERVAL: Duration = Duration::from_millis(60);

// ---------------------------------------------------------------------------
// Senden
// ---------------------------------------------------------------------------

/// Fortschrittsmeldungen einer Sendung. `batch` identifiziert den Drop-Vorgang.
#[derive(Clone, Debug)]
pub enum SendEvent {
    Started {
        batch: u64,
        target: String,
        files: Vec<(String, u64)>,
        total_bytes: u64,
    },
    FileStarted {
        batch: u64,
        index: usize,
    },
    Progress {
        batch: u64,
        index: usize,
        sent: u64,
        total: u64,
    },
    FileSaved {
        batch: u64,
        index: usize,
        name: String,
        saved_as: String,
        sha256: String,
    },
    Finished {
        batch: u64,
        count: u64,
    },
    Failed {
        batch: u64,
        error: String,
        cancelled: bool,
    },
}

/// Sendet eine oder mehrere Dateien an einen Ziel-PC.
///
/// `on_event` wird aus dem aufrufenden Thread aufgerufen. Der Rückgabewert
/// ist `Ok`, wenn der Empfänger alle Dateien bestätigt hat.
#[allow(clippy::too_many_arguments)]
pub fn send_files(
    batch: u64,
    paths: &[PathBuf],
    target: &str,
    token: &str,
    sender_name: &str,
    local_edge: Side,
    cancel: Arc<AtomicBool>,
    mut on_event: impl FnMut(SendEvent),
) -> Result<()> {
    let mut prepared: Vec<(PathBuf, String, u64)> = Vec::new();
    for path in paths {
        let meta =
            fs::metadata(path).with_context(|| format!("{} ist nicht lesbar", path.display()))?;
        if !meta.is_file() {
            bail!("{} ist keine Datei", path.display());
        }
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .map(str::to_string)
            .with_context(|| format!("Ungültiger Dateiname: {}", path.display()))?;
        prepared.push((path.clone(), name, meta.len()));
    }
    if prepared.is_empty() {
        bail!("Keine Dateien ausgewählt");
    }
    let total_bytes = prepared.iter().map(|(_, _, size)| *size).sum();
    on_event(SendEvent::Started {
        batch,
        target: target.to_string(),
        files: prepared
            .iter()
            .map(|(_, name, size)| (name.clone(), *size))
            .collect(),
        total_bytes,
    });

    let result = (|| -> Result<u64> {
        let mut stream = connect(target)?;
        stream.set_read_timeout(Some(IO_TIMEOUT))?;
        stream.set_write_timeout(Some(IO_TIMEOUT))?;
        stream.set_nodelay(true)?;
        let reader_stream = stream.try_clone().context("TCP-Stream klonen")?;
        let mut reader = BufReader::new(reader_stream);

        write_json_line(
            &mut stream,
            &Hello {
                magic: PROTOCOL_MAGIC.to_string(),
                version: PROTOCOL_VERSION,
                token: token.to_string(),
                sender: sender_name.to_string(),
                edge: local_edge,
            },
        )?;
        let reply: HelloReply = read_json_line(&mut reader).context("Handshake lesen")?;
        if reply.magic != PROTOCOL_MAGIC || reply.version != PROTOCOL_VERSION {
            bail!(
                "Gegenstelle spricht ein unbekanntes Protokoll ({}, Version {})",
                reply.magic,
                reply.version
            );
        }
        if !reply.ok {
            bail!(
                "{}",
                reply
                    .error
                    .unwrap_or_else(|| "Der Empfänger hat die Verbindung abgelehnt".into())
            );
        }
        if !local_edge.can_send_to(reply.edge) {
            bail!(
                "Der Ziel-PC ist auf der {} Seite. Bei einer Ablage auf der {} Seite \
                 werden nur PCs auf der {} Seite angesprochen.",
                reply.edge.label(),
                local_edge.label(),
                local_edge.opposite().label()
            );
        }

        for (index, (path, name, size)) in prepared.iter().enumerate() {
            check_cancel(&cancel)?;
            on_event(SendEvent::FileStarted { batch, index });
            write_json_line(
                &mut stream,
                &ClientMessage::File {
                    name: name.clone(),
                    size: *size,
                },
            )?;

            let mut file =
                File::open(path).with_context(|| format!("{} öffnen", path.display()))?;
            let mut hash = Sha256::new();
            let mut sent = 0u64;
            let mut last_report = Instant::now();
            let mut buffer = vec![0u8; CHUNK_SIZE];
            loop {
                check_cancel(&cancel)?;
                let read = file.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                stream.write_all(&buffer[..read])?;
                hash.update(&buffer[..read]);
                sent += read as u64;
                if last_report.elapsed() >= PROGRESS_INTERVAL || sent == *size {
                    on_event(SendEvent::Progress {
                        batch,
                        index,
                        sent,
                        total: *size,
                    });
                    last_report = Instant::now();
                }
            }
            if sent != *size {
                bail!(
                    "{} wurde während des Sendens verändert ({} statt {} Bytes)",
                    path.display(),
                    sent,
                    size
                );
            }
            let digest = hash.finalize();
            stream.write_all(digest.as_ref())?;
            stream.flush()?;

            let file_reply: FileReply = read_json_line(&mut reader)
                .with_context(|| format!("Antwort für {} lesen", name))?;
            if !file_reply.ok {
                bail!(
                    "{}: {}",
                    name,
                    file_reply
                        .error
                        .unwrap_or_else(|| "Der Empfänger hat die Datei abgelehnt".into())
                );
            }
            on_event(SendEvent::FileSaved {
                batch,
                index,
                name: name.clone(),
                saved_as: file_reply.saved_as.unwrap_or_else(|| name.clone()),
                sha256: file_reply
                    .sha256
                    .unwrap_or_else(|| protocol::hex(digest.as_ref())),
            });
        }

        write_json_line(
            &mut stream,
            &ClientMessage::Done {
                count: prepared.len() as u64,
            },
        )?;
        let done: DoneReply = read_json_line(&mut reader).context("Abschlussantwort lesen")?;
        if !done.ok {
            bail!(
                "{}",
                done.error
                    .unwrap_or_else(|| "Der Empfänger hat die Übertragung nicht bestätigt".into())
            );
        }
        if done.received != prepared.len() as u64 {
            bail!(
                "Empfänger meldet {} von {} Dateien",
                done.received,
                prepared.len()
            );
        }
        Ok(done.received)
    })();

    match &result {
        Ok(count) => on_event(SendEvent::Finished {
            batch,
            count: *count,
        }),
        Err(err) => on_event(SendEvent::Failed {
            batch,
            error: format!("{err:#}"),
            cancelled: cancel.load(Ordering::Relaxed),
        }),
    }
    result.map(|_| ())
}

fn check_cancel(cancel: &AtomicBool) -> Result<()> {
    if cancel.load(Ordering::Relaxed) {
        bail!("Übertragung abgebrochen");
    }
    Ok(())
}

fn connect(target: &str) -> Result<TcpStream> {
    let addresses: Vec<SocketAddr> = target
        .to_socket_addrs()
        .with_context(|| format!("Zieladresse „{target}“ auflösen"))?
        .collect();
    if addresses.is_empty() {
        bail!("Zieladresse „{target}“ ergab keine IP-Adresse");
    }
    let mut last_error = None;
    for address in addresses {
        match TcpStream::connect_timeout(&address, CONNECT_TIMEOUT) {
            Ok(stream) => return Ok(stream),
            Err(err) => last_error = Some(err),
        }
    }
    Err(last_error
        .map(anyhow::Error::from)
        .unwrap_or_else(|| anyhow::anyhow!("Keine Verbindung zu {target}")))
    .with_context(|| format!("Verbindung zu {target} fehlgeschlagen"))
}

// ---------------------------------------------------------------------------
// Empfangen
// ---------------------------------------------------------------------------

/// Fortschrittsmeldungen des Empfängers.
#[derive(Clone, Debug)]
pub enum ReceiveEvent {
    Started {
        id: u64,
        name: String,
        size: u64,
        from: String,
    },
    Progress {
        id: u64,
        received: u64,
        total: u64,
    },
    Saved {
        id: u64,
        name: String,
        saved_as: String,
        path: PathBuf,
        size: u64,
        sha256: String,
        from: String,
    },
    Failed {
        id: u64,
        name: String,
        error: String,
    },
}

/// Einstellungen des Empfängers.
#[derive(Clone, Debug)]
pub struct ReceiverConfig {
    pub dir: PathBuf,
    pub token: String,
    pub name: String,
    /// Bildschirmseite dieses PCs. Es werden nur Sender der Gegenseite akzeptiert.
    pub edge: Side,
}

/// Laufender Empfänger (TCP + UDP-Erkennung).
pub struct ReceiverHandle {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    accept_thread: Option<JoinHandle<()>>,
    discovery_thread: Option<JoinHandle<()>>,
}

impl ReceiverHandle {
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    /// Stoppt Annahme- und Erkennungs-Thread. Laufende Übertragungen dürfen
    /// noch zu Ende laufen.
    pub fn shutdown(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.accept_thread.take() {
            let _ = thread.join();
        }
        if let Some(thread) = self.discovery_thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for ReceiverHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Startet den Empfänger. `bind` darf Port 0 enthalten (z. B. für Tests);
/// der tatsächliche Port steht anschließend in [`ReceiverHandle::addr`].
pub fn start_receiver(
    bind: SocketAddr,
    config: ReceiverConfig,
    on_event: impl Fn(ReceiveEvent) + Send + Sync + 'static,
) -> Result<ReceiverHandle> {
    fs::create_dir_all(&config.dir)
        .with_context(|| format!("Empfangsordner {} anlegen", config.dir.display()))?;
    let listener = TcpListener::bind(bind)
        .with_context(|| format!("TCP-Port {bind} belegen (bereits belegt?)"))?;
    listener.set_nonblocking(true)?;
    let addr = listener.local_addr()?;

    let udp = UdpSocket::bind(addr)
        .with_context(|| format!("UDP-Port {addr} belegen (bereits belegt?)"))?;
    udp.set_read_timeout(Some(Duration::from_millis(200)))?;

    let stop = Arc::new(AtomicBool::new(false));
    let on_event = Arc::new(on_event);
    let config = Arc::new(config);
    let next_id = Arc::new(AtomicU64::new(1));

    let discovery_thread = discovery::start_responder(
        udp,
        config.name.clone(),
        addr.port(),
        config.edge,
        stop.clone(),
    );

    let accept_stop = stop.clone();
    let accept_config = config.clone();
    let accept_events = on_event.clone();
    let accept_ids = next_id.clone();
    let accept_thread = thread::spawn(move || {
        while !accept_stop.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, peer)) => {
                    let config = accept_config.clone();
                    let events = accept_events.clone();
                    let ids = accept_ids.clone();
                    thread::spawn(move || {
                        let from = peer.to_string();
                        if let Err(err) = handle_connection(stream, &config, &from, &ids, &*events)
                        {
                            eprintln!("[empfang] Fehler von {from}: {err:#}");
                        }
                    });
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(30));
                }
                Err(err) => {
                    eprintln!("[empfang] accept fehlgeschlagen: {err}");
                    thread::sleep(Duration::from_millis(200));
                }
            }
        }
    });

    Ok(ReceiverHandle {
        addr,
        stop,
        accept_thread: Some(accept_thread),
        discovery_thread: Some(discovery_thread),
    })
}

fn handle_connection(
    stream: TcpStream,
    config: &ReceiverConfig,
    from: &str,
    ids: &AtomicU64,
    on_event: &(impl Fn(ReceiveEvent) + ?Sized),
) -> Result<()> {
    // Auf macOS erben acceptierte Sockets das nonblocking-Flag des Listeners.
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(IO_TIMEOUT))?;
    stream.set_write_timeout(Some(IO_TIMEOUT))?;
    stream.set_nodelay(true)?;
    let mut writer = stream.try_clone().context("TCP-Stream klonen")?;
    let mut reader = BufReader::new(stream);

    let hello: Hello = read_json_line(&mut reader).context("Handshake lesen")?;
    let handshake_error = if hello.magic != PROTOCOL_MAGIC {
        Some(format!("Unbekanntes Protokoll: {}", hello.magic))
    } else if hello.version != PROTOCOL_VERSION {
        Some(format!("Unbekannte Protokollversion: {}", hello.version))
    } else if !config.token.is_empty() && !token_matches(&config.token, &hello.token) {
        Some("Falsches Token".to_string())
    } else if !config.edge.can_send_to(hello.edge) {
        Some(format!(
            "Dieser PC ist auf der {} Seite. Es werden nur PCs auf der {} Seite angenommen \
             (Sender ist auf der {} Seite).",
            config.edge.label(),
            config.edge.opposite().label(),
            hello.edge.label()
        ))
    } else {
        None
    };
    if let Some(error) = handshake_error {
        let _ = write_json_line(
            &mut writer,
            &HelloReply {
                magic: PROTOCOL_MAGIC.to_string(),
                version: PROTOCOL_VERSION,
                ok: false,
                receiver: config.name.clone(),
                edge: config.edge,
                error: Some(error.clone()),
            },
        );
        bail!("{error}");
    }

    write_json_line(
        &mut writer,
        &HelloReply {
            magic: PROTOCOL_MAGIC.to_string(),
            version: PROTOCOL_VERSION,
            ok: true,
            receiver: config.name.clone(),
            edge: config.edge,
            error: None,
        },
    )?;

    loop {
        let message: ClientMessage = read_json_line(&mut reader).context("Nachricht lesen")?;
        match message {
            ClientMessage::File { name, size } => {
                receive_file(
                    &mut reader,
                    &mut writer,
                    config,
                    from,
                    ids,
                    on_event,
                    name,
                    size,
                )?;
            }
            ClientMessage::Done { count } => {
                write_json_line(
                    &mut writer,
                    &DoneReply {
                        ok: true,
                        received: count,
                        error: None,
                    },
                )?;
                return Ok(());
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn receive_file(
    reader: &mut BufReader<TcpStream>,
    writer: &mut TcpStream,
    config: &ReceiverConfig,
    from: &str,
    ids: &AtomicU64,
    on_event: &(impl Fn(ReceiveEvent) + ?Sized),
    name: String,
    size: u64,
) -> Result<()> {
    let id = ids.fetch_add(1, Ordering::Relaxed);
    on_event(ReceiveEvent::Started {
        id,
        name: name.clone(),
        size,
        from: from.to_string(),
    });

    let safe_name = match sanitize_file_name(&name) {
        Ok(name) => name,
        Err(err) => {
            let _ = write_json_line(
                writer,
                &FileReply {
                    ok: false,
                    saved_as: None,
                    sha256: None,
                    error: Some(format!("{err:#}")),
                },
            );
            on_event(ReceiveEvent::Failed {
                id,
                name,
                error: format!("{err:#}"),
            });
            return Err(err);
        }
    };

    let (path, mut file) = create_unique_file(&config.dir, &safe_name)?;
    // Bei Namenskonflikten heißt die Datei tatsächlich anders als angefragt.
    let saved_name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| safe_name.clone());
    let mut hash = Sha256::new();
    let mut last_report = Instant::now();
    let copy_result = copy_exact_hashing(reader, &mut file, &mut hash, size, |received| {
        if last_report.elapsed() >= PROGRESS_INTERVAL || received == size {
            on_event(ReceiveEvent::Progress {
                id,
                received,
                total: size,
            });
            last_report = Instant::now();
        }
    });

    if let Err(err) = copy_result {
        drop(file);
        let _ = fs::remove_file(&path);
        let message = format!("{err:#}");
        let _ = write_json_line(
            writer,
            &FileReply {
                ok: false,
                saved_as: None,
                sha256: None,
                error: Some(message.clone()),
            },
        );
        on_event(ReceiveEvent::Failed {
            id,
            name,
            error: message,
        });
        return Err(err);
    }

    let mut expected = [0u8; SHA256_LEN];
    if let Err(err) = reader.read_exact(&mut expected) {
        drop(file);
        let _ = fs::remove_file(&path);
        let message = format!("Prüfsumme konnte nicht gelesen werden: {err}");
        on_event(ReceiveEvent::Failed {
            id,
            name,
            error: message.clone(),
        });
        bail!("{message}");
    }

    let actual = hash.finalize();
    let actual_bytes = actual.as_ref();
    if actual_bytes != expected {
        drop(file);
        let _ = fs::remove_file(&path);
        let message = format!(
            "Prüfsumme falsch (erwartet {}, erhalten {})",
            protocol::hex(&expected),
            protocol::hex(actual_bytes)
        );
        let _ = write_json_line(
            writer,
            &FileReply {
                ok: false,
                saved_as: None,
                sha256: None,
                error: Some(message.clone()),
            },
        );
        on_event(ReceiveEvent::Failed {
            id,
            name,
            error: message.clone(),
        });
        bail!("{message}");
    }

    file.flush()?;
    drop(file);
    let digest = protocol::hex(actual_bytes);
    on_event(ReceiveEvent::Saved {
        id,
        name,
        saved_as: saved_name.clone(),
        path,
        size,
        sha256: digest.clone(),
        from: from.to_string(),
    });
    write_json_line(
        writer,
        &FileReply {
            ok: true,
            saved_as: Some(saved_name),
            sha256: Some(digest),
            error: None,
        },
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::sha256_hex;
    use std::sync::Mutex;

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_dir(tag: &str) -> PathBuf {
        let unique = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "filesend-test-{}-{}-{unique}",
            std::process::id(),
            tag
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn receiver(
        tag: &str,
        token: &str,
    ) -> (ReceiverHandle, PathBuf, Arc<Mutex<Vec<ReceiveEvent>>>) {
        receiver_on(tag, token, Side::Right)
    }

    fn receiver_on(
        tag: &str,
        token: &str,
        edge: Side,
    ) -> (ReceiverHandle, PathBuf, Arc<Mutex<Vec<ReceiveEvent>>>) {
        let dir = temp_dir(tag);
        let events = Arc::new(Mutex::new(Vec::new()));
        let sink = events.clone();
        let handle = start_receiver(
            "127.0.0.1:0".parse().unwrap(),
            ReceiverConfig {
                dir: dir.clone(),
                token: token.to_string(),
                name: "Test-PC".into(),
                edge,
            },
            move |event| sink.lock().unwrap().push(event),
        )
        .unwrap();
        (handle, dir, events)
    }

    #[test]
    fn sends_and_receives_multiple_files() {
        let (handle, dir, events) = receiver("multi", "");
        let source = temp_dir("multi-source");
        let first = source.join("hallo.txt");
        let second = source.join("daten.bin");
        fs::write(&first, b"Hallo, PC daneben!\n").unwrap();
        let binary: Vec<u8> = (0..300_000u32).map(|value| (value % 251) as u8).collect();
        fs::write(&second, &binary).unwrap();

        let sends = Arc::new(Mutex::new(Vec::new()));
        let sink = sends.clone();
        send_files(
            7,
            &[first.clone(), second.clone()],
            &handle.addr().to_string(),
            "",
            "Test-Sender",
            Side::Left,
            Arc::new(AtomicBool::new(false)),
            move |event| sink.lock().unwrap().push(event),
        )
        .unwrap();

        let received: Vec<_> = events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|event| match event {
                ReceiveEvent::Saved { saved_as, path, .. } => {
                    Some((saved_as.clone(), path.clone()))
                }
                _ => None,
            })
            .collect();
        assert_eq!(received.len(), 2, "beide Dateien müssen bestätigt sein");
        assert_eq!(fs::read(&received[0].1).unwrap(), b"Hallo, PC daneben!\n");
        assert_eq!(fs::read(&received[1].1).unwrap(), binary);
        assert!(dir.join(&received[0].0).exists());

        let send_events = sends.lock().unwrap();
        assert!(matches!(
            send_events.last(),
            Some(SendEvent::Finished { count: 2, .. })
        ));
        assert!(send_events.iter().any(|event| matches!(
            event,
            SendEvent::FileSaved { sha256, .. } if sha256 == &sha256_hex(&binary)
        )));
    }

    #[test]
    fn avoids_overwriting_existing_files() {
        let (handle, dir, events) = receiver("collision", "");
        fs::write(dir.join("bericht.txt"), b"alt").unwrap();
        let source = temp_dir("collision-source");
        let file = source.join("bericht.txt");
        fs::write(&file, b"neu").unwrap();

        send_files(
            1,
            std::slice::from_ref(&file),
            &handle.addr().to_string(),
            "",
            "Test",
            Side::Left,
            Arc::new(AtomicBool::new(false)),
            |_| {},
        )
        .unwrap();

        assert_eq!(fs::read(dir.join("bericht.txt")).unwrap(), b"alt");
        assert_eq!(fs::read(dir.join("bericht (1).txt")).unwrap(), b"neu");
        let recorded = events.lock().unwrap();
        assert!(
            recorded.iter().any(|event| matches!(
                event,
                ReceiveEvent::Saved { saved_as, .. } if saved_as == "bericht (1).txt"
            )),
            "Events: {recorded:?}"
        );
    }

    #[test]
    fn rejects_wrong_token() {
        let (handle, dir, _) = receiver("token", "streng-geheim");
        let source = temp_dir("token-source");
        let file = source.join("geheim.txt");
        fs::write(&file, b"x").unwrap();

        let error = send_files(
            1,
            &[file],
            &handle.addr().to_string(),
            "falsch",
            "Test",
            Side::Left,
            Arc::new(AtomicBool::new(false)),
            |_| {},
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("Token"));
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 0);
    }

    #[test]
    fn rejects_same_edge_peer() {
        // Empfänger rechts, Sender ebenfalls rechts → darf nicht ankommen.
        let (handle, dir, _) = receiver_on("same-edge", "", Side::Right);
        let source = temp_dir("same-edge-source");
        let file = source.join("gleich.txt");
        fs::write(&file, b"x").unwrap();

        let error = send_files(
            1,
            &[file],
            &handle.addr().to_string(),
            "",
            "Test",
            Side::Right,
            Arc::new(AtomicBool::new(false)),
            |_| {},
        )
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("Seite"),
            "unerwarteter Fehler: {error:#}"
        );
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 0);
    }

    #[test]
    fn rejects_path_traversal() {
        let (handle, dir, _) = receiver("traversal", "");
        let parent = dir.parent().unwrap().to_path_buf();
        let escape = parent.join("ausbruch.txt");
        let _ = fs::remove_file(&escape);

        let mut stream = TcpStream::connect(handle.addr()).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        write_json_line(
            &mut stream,
            &Hello {
                magic: PROTOCOL_MAGIC.into(),
                version: PROTOCOL_VERSION,
                token: String::new(),
                sender: "Angreifer".into(),
                edge: Side::Left,
            },
        )
        .unwrap();
        let _: HelloReply = read_json_line(&mut reader).unwrap();
        write_json_line(
            &mut stream,
            &ClientMessage::File {
                name: "../ausbruch.txt".into(),
                size: 4,
            },
        )
        .unwrap();
        let reply: FileReply = read_json_line(&mut reader).unwrap();
        assert!(!reply.ok);
        assert!(!escape.exists(), "Pfad-Ausbruch darf nicht funktionieren");
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 0);
    }

    #[test]
    fn rejects_wrong_checksum_and_removes_partial_file() {
        let (handle, dir, _) = receiver("checksum", "");
        let mut stream = TcpStream::connect(handle.addr()).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        write_json_line(
            &mut stream,
            &Hello {
                magic: PROTOCOL_MAGIC.into(),
                version: PROTOCOL_VERSION,
                token: String::new(),
                sender: "Test".into(),
                edge: Side::Left,
            },
        )
        .unwrap();
        let _: HelloReply = read_json_line(&mut reader).unwrap();
        write_json_line(
            &mut stream,
            &ClientMessage::File {
                name: "kaputt.bin".into(),
                size: 4,
            },
        )
        .unwrap();
        stream.write_all(b"data").unwrap();
        stream.write_all(&[0u8; SHA256_LEN]).unwrap();
        let reply: FileReply = read_json_line(&mut reader).unwrap();
        assert!(!reply.ok);
        assert!(reply.error.unwrap().contains("Prüfsumme"));
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 0);
    }

    #[test]
    fn discovery_finds_local_receiver() {
        let (handle, _dir, _) = receiver("discovery", "");
        let peers = discovery::discover(handle.port(), Duration::from_millis(700));
        assert!(
            peers.iter().any(|peer| peer.addr == handle.addr()),
            "gefundene Peers: {peers:?}, erwartet: {}",
            handle.addr()
        );
    }

    #[test]
    fn cancellation_stops_transfer() {
        let (handle, _dir, _) = receiver("cancel", "");
        let source = temp_dir("cancel-source");
        let file = source.join("gross.bin");
        fs::write(&file, vec![7u8; 2 * 1024 * 1024]).unwrap();
        let cancel = Arc::new(AtomicBool::new(true));
        let error = send_files(
            1,
            &[file],
            &handle.addr().to_string(),
            "",
            "Test",
            Side::Left,
            cancel,
            |_| {},
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("abgebrochen"));
    }
}
