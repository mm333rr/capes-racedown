# Changelog

All notable changes to capes-racedown will be documented in this file.

## [0.1.0] — 2026-06-25

### Added
- Initial release of `capes-racedown` daemon
- FastAPI webhook receiver on port 6789 for Sonarr (`/webhook/sonarr`) and Radarr (`/webhook/radarr`)
- Race logic: on `on_grab` event, fetch alternative releases from arr, add top N-1 by seeder count to qBittorrent
- Winner detection: completion state (uploading/stalledUP) or sustained speed >= 1 GB/hr for 2 consecutive 30s polls
- Loser deletion: all non-winner torrents (files + data) removed from qBit
- Auto-import: if race extra wins, removes original from arr queue and triggers DownloadedEpisodesScan / DownloadedMoviesScan
- Trap format filter: blocks .zip .rar .scr .bat .exe .cmd .msi .vbs .ps1 .pif .jar .apk from race candidates
- Race timeout: auto-cleanup after 4 hours
- systemd unit with wrapper script to handle spaces in project path
- iptables rules for Docker arr-net (172.24.0.0/24) to reach host port 6789
- GET /health and GET /status endpoints
- All configuration via env vars (.racedown.env, chmod 600)

### Fixed
- Sonarr/Radarr release scoring: No-RlsGroup CF changed from -10000 to -1000 (soft penalty)
- Added .zip, .rar, .scr, .pif to Sonarr + Radarr Block Malicious Extensions release profile (14 patterns total)
- recyclarr.yml fully rewritten to v8 custom_format_groups format (removed deprecated include: syntax)
- recyclarr state repaired via state repair --adopt to adopt existing quality profiles

### Infrastructure
- Daemon runs as mbuntuadmin via systemd, supervised with Restart=always
- Wrapper script /home/mbuntuadmin/bin/capes-racedown-start.sh handles path with spaces
- Webhooks registered in Sonarr (id=2) and Radarr (id=2) at http://172.24.0.1:6789/webhook/{sonarr,radarr}
- qBit bypass_local_auth=True — host connections require no credentials
