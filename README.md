# FileSend

Eine schmale Rust-App, die sich an die Bildschirmkante andockt. Datei mit der
Maus hineinziehen, loslassen – der PC daneben empfängt sie über das lokale
Netzwerk. Kein Cloud-Konto, kein USB-Stick, keine Dateifreigabe einrichten.

```
┌──────────────────────────┐
│  ● FileSend          Rand ×│
│                          │
│  ╭────────────────────╮  │
│  │         ↓          │  │   Datei hier ablegen
│  │  Datei hier ablegen│  │   → wird an den Ziel-PC geschickt
│  │  ziehen & loslassen│  │
│  ╰────────────────────╯  │
│                          │
│  ZIEL-PC                 │
│  [ Wohnzimmer-PC  ▾ ]    │
│  [ 192.168.1.20:45871 ]  │
│                          │
│  GESENDET                │
│  ● urlaub.jpg  Gesendet  │
│  ██████████████████ 100% │
│                          │
│  Dieser PC · 192.168.1.7 │
└──────────────────────────┘
```

## Schnellstart

Auf beiden PCs:

```bash
cargo run --release
```

1. Die Leiste erscheint am rechten Bildschirmrand und bleibt im Vordergrund.
2. Die Kante bestimmt die Richtung: rechts sendet nur an PCs, die links
   stehen, links nur an PCs, die rechts stehen. Der jeweils passende PC wird
   automatisch im lokalen Netz gefunden („1 auf der linken Seite“).
   Alternativ IP-Adresse und Port eintragen, z. B. `192.168.1.20:45871`.
3. Datei aus dem Finder/Explorer in die Ablage ziehen und loslassen.
4. Fertig – auf dem anderen PC liegt die Datei in `Downloads/FileSend`.

**Seite wechseln:** Die Leiste einfach mit der Maus auf die andere
Bildschirmseite ziehen und loslassen. Sie dockt dort an, wechselt automatisch
die Senderichtung (rechts ↔ links) und verwirft ein Ziel, das nicht mehr zur
neuen Seite passt. Die Einstellung wird gespeichert.

Beide PCs brauchen FileSend. Der Empfang läuft in der GUI automatisch mit.
Wer auf dem Ziel-PC kein Fenster möchte, startet nur den Empfänger – mit der
Seite, auf der dieser PC steht:

```bash
filesend --receive --edge left
```

Die Gegenstelle muss dann auf der anderen Seite stehen (`--edge right`). Diese
Zuordnung wird im Handshake auf beiden Seiten geprüft; gleiche Seiten werden
abgelehnt.

## Kommandozeile

```text
filesend                          GUI am Bildschirmrand starten (empfängt mit)
filesend --receive [Optionen]     Nur empfangen, ohne Fenster
filesend --send DATEI... --to ADRESSE
                                  Dateien einmalig senden
filesend --discover               PCs im lokalen Netz suchen
```

Optionen: `--port`, `--dir`, `--token`, `--name`, `--edge`, `--to`.

`--edge` gibt die Bildschirmseite an: `right`/`rechts` oder `left`/`links`.
Ohne Angabe gilt die gespeicherte Einstellung (Standard: rechts).

Beispiele:

```bash
# Auf dem Ziel-PC links: nur Empfänger, eigener Ordner
filesend --receive --edge left --dir ~/Downloads/FileSend

# Vom Quell-PC rechts: mehrere Dateien an den linken PC senden
filesend --send urlaub.jpg rechnung.pdf --to 192.168.1.20:45871 --edge right

# Mit Hostname (Port wird automatisch ergänzt)
filesend --send backup.zip --to wohnzimmer-pc

# Mit gemeinsamem Geheimnis auf beiden Seiten
filesend --receive --edge left --token geheim
filesend --send foto.jpg --to 192.168.1.20 --edge right --token geheim

# Nachschauen, wer im Netz erreichbar ist (inkl. Seite)
filesend --discover
```

## Einstellungen

Die GUI speichert ihre Einstellungen unter:

- macOS: `~/Library/Application Support/filesend/config.json`
- Linux: `~/.config/filesend/config.json`
- Windows: `%APPDATA%\filesend\config.json`

```json
{
  "port": 45871,
  "peer": "192.168.1.20:45871",
  "token": "",
  "download_dir": "/Users/emil/Downloads/FileSend",
  "device_name": "emils-macbook",
  "edge": "right"
}
```

`edge` ist `right` oder `left`. Der Button **Rand** dockt das Fenster erneut
an die gewählte Bildschirmkante an. Wird das Fenster auf die andere Seite
gezogen, übernimmt die App diese Kante automatisch und speichert sie.

## Wie es funktioniert

- **Kante = Richtung:** Jeder PC meldet seine Bildschirmseite. Sender und
  Empfänger prüfen im Handshake, ob sie auf gegenüberliegenden Seiten stehen.
  Rechts → links und links → rechts sind erlaubt; gleiche Seiten werden
  abgelehnt, bevor eine Datei übertragen wird.
- **Erkennung:** Der Sender schickt ein UDP-Broadcast-Paket. Jeder laufende
  Empfänger antwortet mit Name, TCP-Port und Bildschirmseite. Die GUI zeigt
  nur PCs der passenden Gegenseite. Zusätzlich wird Loopback abgefragt, damit
  es auch auf einem Rechner testbar ist.
- **Übertragung:** Eine TCP-Verbindung pro Drop-Vorgang. Pro Datei geht eine
  JSON-Kopfzeile, danach die reinen Dateibytes (128 KiB Blöcke), danach
  32 Bytes SHA-256.
- **Prüfung:** Der Empfänger hasht beim Schreiben mit, vergleicht die
  Prüfsumme und bestätigt erst dann die Datei. Bei Abweichung wird die
  unvollständige Datei gelöscht.
- **Sicherheit:** Dateinamen mit Pfadtrennern (`../…`) werden abgelehnt,
  sodass niemand aus dem Empfangsordner ausbrechen kann. Ein optionales
  gemeinsames Token schützt vor fremden Sendern im selben Netz.
- **Namenskonflikte:** Existiert eine Datei schon, wird automatisch
  `name (1).ext`, `name (2).ext`, … gewählt. Nichts wird überschrieben.
- **Abbruch:** Der ×-Button an einer laufenden Übertragung bricht sie ab;
  unvollständige Dateien werden auf der Empfängerseite entfernt.

Das Protokoll ist in [`src/protocol.rs`](src/protocol.rs) dokumentiert.

## Tests

```bash
cargo test
cargo clippy --all-targets -- -D warnings
```

Die Tests starten echte TCP-/UDP-Server auf Loopback und prüfen unter anderem:

- mehrere Dateien inkl. Binärdaten und SHA-256-Gleichheit
- Namenskonflikte (`bericht (1).txt`)
- Ablehnung von Pfad-Ausbrüchen und falschen Prüfsummen
- Token-Prüfung
- Erkennung im lokalen Netz inkl. Bildschirmseite
- Ablehnung gleicher Bildschirmseiten (rechts → rechts)
- Abbruch einer Übertragung

## Firewall

Der erste Start fragt unter macOS/Windows eventuell nach einer
Firewall-Freigabe. FileSend braucht eingehende Verbindungen auf dem
konfigurierten Port (Standard **45871**, TCP **und** UDP für die Erkennung).
Beide PCs müssen im selben LAN sein; Gäste-WLANs blockieren oft die
gegenseitige Kommunikation.

## Grenzen

- Es werden einzelne Dateien übertragen, keine Ordner.
- Die Übertragung ist für vertrauenswürdige lokale Netze gedacht. Wer mehr
  Schutz braucht, setzt auf beiden Seiten ein Token und/oder nutzt ein VPN.
- Die App ist ein schlankes Werkzeug, kein Sync-Client: Es gibt keine
  Wiederaufnahme abgebrochener Übertragungen und keine Versionshistorie.
