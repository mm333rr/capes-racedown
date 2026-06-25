# capes-racedown

Race-download daemon for Sonarr + Radarr on the Capes homelab.

## What it does

On every `on_grab` webhook event, capes-racedown:

1. Identifies the original torrent Sonarr/Radarr sent to qBittorrent
2. Searches for the top `RACE_COUNT-1` alternative releases by **seeder count** (same quality, score ≥ MIN_SCORE, no trap formats)
3. Adds them as race competitors to qBit (tagged `capes-race-<key>`)
4. Monitors every 30 seconds:
   - **Winner by completion**: first torrent to reach `uploading`/`stalledUP` state
   - **Winner by speed**: first torrent to sustain ≥ `SPEED_THRESHOLD_GBH` GB/hr for two consecutive checks
5. Deletes all losers (files included)
6. If the winner was **not** the original arr grab: removes original from arr queue and triggers `DownloadedEpisodesScan` / `DownloadedMoviesScan` so arr imports the winner

## Architecture

```
Sonarr/Radarr ──on_grab──▶ racedown (FastAPI :6789)
                                │
                    ┌───────────▼───────────┐
                    │  search arr releases  │
                    │  sort by seeders desc │
                    │  filter trap formats  │
                    └───────────┬───────────┘
                                │
                    ┌───────────▼───────────┐
                    │  add extras to qBit   │
                    │  tag: capes-race-<k>  │
                    └───────────┬───────────┘
                                │
                    ┌───────────▼───────────┐
                    │  monitor every 30s    │
                    │  winner = done/speed  │
                    └───────────┬───────────┘
                                │
                    ┌───────────▼───────────┐
                    │  delete losers        │
                    │  trigger arr import   │
                    │  if extra won         │
                    └───────────────────────┘
```

## Installation (mbuntu)

The daemon is installed at `/home/mbuntuadmin/Claude Scripts and Venvs/capes-racedown/` and supervised by systemd.

```bash
# Status
sudo systemctl status capes-racedown

# Logs
journalctl -u capes-racedown -f

# Active races
curl http://localhost:6789/status

# Restart
sudo systemctl restart capes-racedown
```

## Webhook URLs

Configured automatically in Sonarr and Radarr → Settings → Connect → RaceDown.

| App    | URL                                         |
|--------|---------------------------------------------|
| Sonarr | `http://172.24.0.1:6789/webhook/sonarr`     |
| Radarr | `http://172.24.0.1:6789/webhook/radarr`     |

The `172.24.0.1` IP is the mbuntu Docker gateway reachable from the `arr-net` bridge.

## Configuration

All config lives in `.racedown.env` (chmod 600, not committed):

| Variable             | Default  | Description                                    |
|----------------------|----------|------------------------------------------------|
| `SONARR_URL`         | :8989    | Sonarr base URL                                |
| `SONARR_API_KEY`     | —        | Sonarr API key                                 |
| `RADARR_URL`         | :7878    | Radarr base URL                                |
| `RADARR_API_KEY`     | —        | Radarr API key                                 |
| `QBIT_URL`           | :5555    | qBittorrent WebUI URL                          |
| `QBIT_USER`          | admin    | qBit username                                  |
| `QBIT_PASS`          | —        | qBit password                                  |
| `RACE_COUNT`         | 3        | Total torrents to race (1 original + N extras) |
| `SPEED_THRESHOLD_GBH`| 1.0      | GB/hr sustained speed to declare winner early  |
| `MIN_SCORE`          | 0        | Minimum arr custom format score for candidates |
| `RACE_TIMEOUT_SEC`   | 14400    | Abort race after this many seconds (4h)        |
| `TV_SAVE_PATH`       | /tank/qb/downloads/tv     | qBit save path for TV     |
| `MOVIE_SAVE_PATH`    | /tank/qb/downloads/movies | qBit save path for movies |

## Trap format blocking

The daemon skips any candidate whose release title matches:
`.zip .rar .scr .bat .exe .cmd .msi .vbs .ps1 .pif .jar .apk`

These are also blocked at the arr level via the "Block Malicious Extensions" Release Profile (14 patterns as of 2026-06-25).

## Scoring (recyclarr)

- **No-RlsGroup**: soft `-1000` (not `-10000`) — some indexers space-normalize titles, falsely triggering this CF
- **Trap/unwanted CFs**: `-10000` hard block (3D, Bad Dual Groups, BR-DISK, LQ, x265 HD, etc.)
- **Streaming services** (AMZN, NF, DSNP, etc.): `+75` per service
- **WEB Tier 01-03**: `+1600`–`+1700`

## systemd setup

```bash
# Install service + wrapper
sudo cp capes-racedown.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now capes-racedown

# iptables (persist across reboots)
sudo iptables -I INPUT -s 172.24.0.0/24 -p tcp --dport 6789 -j ACCEPT
sudo netfilter-persistent save
```
