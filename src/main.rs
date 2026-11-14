//! FileSend – Datei an die Bildschirmkante ziehen, PC daneben empfängt sie.

mod gui;

use anyhow::{Context, Result, bail};
use filesend::config::{Config, Side};
use filesend::transfer::{self, ReceiveEvent, ReceiverConfig};
use filesend::util;
use filesend::{APP_NAME, DEFAULT_PORT, VERSION, discovery};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

fn main() {
    if let Err(err) = run() {
        eprintln!("Fehler: {err:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();

    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        print_help();
        return Ok(());
    }
    if args.iter().any(|arg| arg == "--version" || arg == "-V") {
        println!("{APP_NAME} {VERSION}");
        return Ok(());
    }
    if args.iter().any(|arg| arg == "--receive") {
        return run_receive(&args);
    }
    if let Some(index) = args.iter().position(|arg| arg == "--send") {
        return run_send(&args, index);
    }
    if args.iter().any(|arg| arg == "--discover") {
        return run_discover(&args);
    }
    if !args.is_empty() {
        bail!(
            "Unbekannte Argumente: {}\nNutze --help für die Hilfe.",
            args.join(" ")
        );
    }

    eframe::run_native(
        APP_NAME,
        gui::native_options(),
        Box::new(|cc| Ok(Box::new(gui::FilesendApp::new(cc)))),
    )
    .map_err(|err| anyhow::anyhow!("Fenster konnte nicht geöffnet werden: {err}"))
}

fn run_receive(args: &[String]) -> Result<()> {
    let config = Config::load();
    let mut port = config.port;
    let mut dir = config.download_dir.clone();
    let mut token = config.token.clone();
    let mut name = config.device_name.clone();
    let mut edge = config.edge;

    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--receive" => index += 1,
            "--port" => {
                port = parse_port(args, index)?;
                index += 2;
            }
            "--dir" => {
                dir = PathBuf::from(value(args, index, "--dir")?);
                index += 2;
            }
            "--token" => {
                token = value(args, index, "--token")?.to_string();
                index += 2;
            }
            "--name" => {
                name = value(args, index, "--name")?.to_string();
                index += 2;
            }
            "--edge" => {
                edge = parse_edge(args, index)?;
                index += 2;
            }
            other => bail!("Unbekannte Option für --receive: {other}"),
        }
    }

    let handle = transfer::start_receiver(
        SocketAddr::from(([0, 0, 0, 0], port)),
        ReceiverConfig {
            dir: dir.clone(),
            token,
            name,
            edge,
        },
        |event| match event {
            ReceiveEvent::Started {
                name, size, from, ..
            } => println!("← {from}: {name} ({})", util::format_bytes(size)),
            ReceiveEvent::Saved {
                saved_as, sha256, ..
            } => println!("✔ gespeichert: {saved_as} (sha256 {})", &sha256[..12]),
            ReceiveEvent::Failed { name, error, .. } => {
                eprintln!("✘ {name}: {error}");
            }
            ReceiveEvent::Progress { .. } => {}
        },
    )
    .context("Empfänger starten")?;

    println!("{APP_NAME} {VERSION} – Empfänger läuft");
    println!("  Adresse:  {}", handle.addr());
    println!("  Ordner:   {}", dir.display());
    println!(
        "  Seite:    {} (nimmt nur Sender von {} an)",
        edge.label(),
        edge.opposite().label()
    );
    println!("  Beenden:  Strg+C");
    loop {
        thread::sleep(Duration::from_secs(3600));
    }
}

fn run_send(args: &[String], send_index: usize) -> Result<()> {
    let config = Config::load();
    let mut files: Vec<PathBuf> = Vec::new();
    let mut target: Option<String> = None;
    let mut token = config.token.clone();
    let mut edge = config.edge;
    let mut index = send_index + 1;

    while index < args.len() {
        match args[index].as_str() {
            "--to" => {
                target = Some(value(args, index, "--to")?.to_string());
                index += 2;
            }
            "--token" => {
                token = value(args, index, "--token")?.to_string();
                index += 2;
            }
            "--edge" => {
                edge = parse_edge(args, index)?;
                index += 2;
            }
            other if other.starts_with("--") => {
                bail!("Unbekannte Option für --send: {other}");
            }
            file => {
                files.push(PathBuf::from(file));
                index += 1;
            }
        }
    }

    if files.is_empty() {
        bail!(
            "Keine Dateien angegeben. Beispiel: filesend --send foto.jpg --to 192.168.1.20:45871"
        );
    }
    let target = target.context("--to <host:port> fehlt")?;
    let target = util::normalize_target(&target, config.port);

    println!(
        "Sende {} Datei(en) an {target} (Ablage {}, Ziel muss auf der {} Seite sein) …",
        files.len(),
        edge.short(),
        edge.opposite().label()
    );
    transfer::send_files(
        1,
        &files,
        &target,
        &token,
        &config.device_name,
        edge,
        std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        |event| match event {
            transfer::SendEvent::Started {
                files, total_bytes, ..
            } => {
                println!(
                    "  {} Datei(en), {}",
                    files.len(),
                    util::format_bytes(total_bytes)
                );
            }
            transfer::SendEvent::FileSaved { saved_as, .. } => {
                println!("  ✔ {saved_as}");
            }
            transfer::SendEvent::Finished { count, .. } => {
                println!("Fertig: {count} Datei(en) bestätigt.");
            }
            transfer::SendEvent::Failed { error, .. } => {
                eprintln!("  ✘ {error}");
            }
            _ => {}
        },
    )?;
    Ok(())
}

fn run_discover(args: &[String]) -> Result<()> {
    let mut port = DEFAULT_PORT;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--discover" => index += 1,
            "--port" => {
                port = parse_port(args, index)?;
                index += 2;
            }
            other => bail!("Unbekannte Option für --discover: {other}"),
        }
    }
    if port != DEFAULT_PORT && port == 0 {
        bail!("Ungültiger Port");
    }
    println!("Suche FileSend-Empfänger auf UDP-Port {port} …");
    let peers = discovery::discover(port, Duration::from_secs(1));
    if peers.is_empty() {
        println!("Keine Empfänger gefunden. Läuft auf dem anderen PC FileSend?");
    } else {
        for peer in peers {
            println!(
                "  {}  {}  (Seite: {})",
                peer.name,
                peer.addr,
                peer.edge.label()
            );
        }
    }
    Ok(())
}

fn value<'a>(args: &'a [String], index: usize, option: &str) -> Result<&'a str> {
    args.get(index + 1)
        .map(String::as_str)
        .with_context(|| format!("Wert für {option} fehlt"))
}

fn parse_port(args: &[String], index: usize) -> Result<u16> {
    let raw = value(args, index, "--port")?;
    let port: u16 = raw
        .parse()
        .with_context(|| format!("Ungültiger Port: {raw}"))?;
    if port == 0 {
        bail!("Port 0 ist nicht erlaubt");
    }
    Ok(port)
}

fn parse_edge(args: &[String], index: usize) -> Result<Side> {
    let raw = value(args, index, "--edge")?;
    Side::parse(raw).with_context(|| format!("Ungültige Seite „{raw}“ (left oder right)"))
}

fn print_help() {
    println!(
        "{APP_NAME} {VERSION}
Dateien an den PC daneben senden – per Drag & Drop an die Bildschirmkante.

AUFRUF
  filesend                         Grafische Leiste am rechten Bildschirmrand starten.
                                   Sie empfängt gleichzeitig Dateien.
  filesend --receive [Optionen]    Nur empfangen (ohne Fenster), z. B. auf dem Ziel-PC.
  filesend --send DATEI... --to ADRESSE
                                   Dateien einmalig per Kommandozeile senden.
  filesend --discover              PCs im lokalen Netz suchen.

DIE KANTE BESTIMMT DIE RICHTUNG
  Eine Leiste auf der rechten Seite sendet nur an PCs, die links stehen –
  und umgekehrt. Die Kante wird auf beiden PCs geprüft.

OPTIONEN
  --port <PORT>       TCP/UDP-Port (Standard: {DEFAULT_PORT})
  --dir <ORDNER>      Zielordner für empfangene Dateien
  --token <TOKEN>     Gemeinsames Geheimnis (muss auf beiden PCs gleich sein)
  --name <NAME>       Anzeigename dieses PCs
  --edge <SEITE>      Bildschirmseite: right/rechts oder left/links
  --to <HOST:PORT>    Ziel-PC, z. B. 192.168.1.20:{DEFAULT_PORT}
  -h, --help          Diese Hilfe
  -V, --version       Version anzeigen

BEISPIELE
  filesend
  filesend --receive --edge left --dir ~/Downloads/FileSend
  filesend --send urlaub.jpg --to 192.168.1.20:{DEFAULT_PORT} --edge right
  filesend --send a.pdf b.zip --to wohnzimmer-pc --token geheim
  filesend --discover

Die Konfiguration liegt unter:
  {}",
        filesend::config::config_path().display()
    );
}
