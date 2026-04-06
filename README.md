# Match Replay Pipeline

End-to-end data pipeline that collects, downloads, parses, and stores competitive match replays from a live online game (30M+ monthly matches, 10M+ active players).

Built to run fully unattended on GitHub Actions with zero infrastructure cost.

## Architecture

```
 ┌─────────────────────────────────────────────────────────────────┐
 │                     GitHub Actions (cron)                       │
 │                                                                 │
 │  ┌──────────┐   ┌───────────┐   ┌──────────┐   ┌────────────┐  │
 │  │ Collect   │──▶│ Download  │──▶│ Parse    │──▶│ Enrich     │  │
 │  │ (Node.js) │   │ (Python)  │   │ (Python) │   │ (Node+Py)  │  │
 │  └──────────┘   └───────────┘   └──────────┘   └────────────┘  │
 │       │               │              │               │          │
 │       ▼               ▼              ▼               ▼          │
 │    Match IDs      Replay files   Structured CSV   Backfilled    │
 │    (JSON)         (.sgs binary)  + SQLite DB      metadata      │
 └─────────────────────────────────────────────────────────────────┘
                              │
                    ┌─────────▼─────────┐
                    │  Cloudflare R2    │
                    │  (persistent      │
                    │   state store)    │
                    └───────────────────┘
```

## What It Does

1. **Collect** — Headless Puppeteer browser logs into the game client, intercepts the WebSocket (Protobuf) protocol, and crawls leaderboards + social graph to discover match IDs. Supports multi-account rotation and daily caching.

2. **Download** — Async Python downloader fetches replay files from CDN at 50 concurrent connections. Deduplicates against a persistent index to avoid re-downloading.

3. **Parse** — Custom binary parser reverse-engineered from the proprietary `.sgs` replay format. Extracts player identities, draft picks, match results, seat assignments, and Elo ratings from a variable-length frame-based event stream.

4. **Enrich** — Detects incomplete player records (e.g. disconnected players missing rank data), then queries the game server via WebSocket to backfill nicknames and rank levels. Results are cached locally to avoid redundant API calls.

5. **Sync** — Bidirectional sync with Cloudflare R2 for cross-run state persistence. Handles incremental uploads, pulls, and TTL-based cleanup of stale data.

## Technical Highlights

### Protocol Reverse Engineering
- Reverse-engineered the game's **WebSocket + Protobuf** protocol by intercepting `console.log` dispatch from the obfuscated game client
- Identified 6 critical message types (authentication, leaderboard queries, match records, friend discovery, user info, seat assignment)
- Built a reusable `encodeField` / `encodeVarint` serialiser to construct arbitrary Protobuf payloads without `.proto` definitions

### Binary Replay Parsing (Zero Dependencies)
- Wrote a from-scratch parser for the proprietary `.sgs` binary format — no external protobuf library
- File structure: 4-byte magic (`sgsz`) → variable-length header (Protobuf-encoded) → event frame stream
- Each frame: 16-byte fixed header + variable payload; events identified by 32-bit message type
- Handles edge cases: extended headers in long matches (>90 min), disconnected players, draw conditions

### Browser Automation & Anti-Detection
- Puppeteer-based headless automation with WebDriver flag spoofing, plugin emulation, and `console.log` property locking (the game client overwrites `console.log` — we overwrite it back)
- Captures the `ProtoSocketClient` singleton by hooking `Laya.Socket.prototype.send` at runtime
- Enables **active** WebSocket communication: not just sniffing — the pipeline constructs and sends its own protocol messages through the captured socket

### Pipeline Engineering
- **Incremental processing**: persistent index tracks which replays have been downloaded and parsed; each run only processes net-new data
- **Multi-account rotation**: accounts rotate by `UTC_hour % account_count`, no hardcoded limits
- **Fault tolerance**: `INSERT OR IGNORE` deduplication, graceful handling of timeout/disconnect/malformed replays, anomaly archiving for post-mortem analysis
- **Stateless runner**: all persistent state lives in R2; GitHub Actions runner is fully ephemeral

## Stack

| Layer | Technology | Why |
|-------|-----------|-----|
| Collection | Node.js 22, Puppeteer | Only way to interact with WebSocket game client |
| Parsing | Python 3.11, zero deps | Binary struct unpacking, protobuf decoding |
| Download | Python `aiohttp` | 50-connection async I/O |
| Storage | Cloudflare R2 (S3-compatible) | Free egress, persistent state |
| Database | SQLite | Single-file, zero-config, portable |
| Enrichment | Node.js + Python | WebSocket queries + DB updates |
| Orchestration | GitHub Actions | Free CI for public repos, cron scheduling |

## Project Stats

| Metric | Value |
|--------|-------|
| Codebase | ~3,300 lines (Python + JavaScript) |
| Daily throughput | 5,000–10,000 matches |
| Replay formats parsed | 2 (2v2 ranked, 3-player mode) |
| Characters in mapping table | 628 |
| Pipeline run time | ~30 min per cycle |
| Infrastructure cost | $0 |

## Repository Structure

```
├── collect/
│   ├── collect.js          # WebSocket-based match ID crawler
│   ├── query_ranks.js      # Player rank backfill via game protocol
│   └── save_cookies.js     # One-time auth cookie export
│
├── pipeline/
│   ├── common.py           # Binary .sgs parser & protobuf decoder
│   ├── db.py               # SQLite schema & bulk insert operations
│   ├── download.py         # Async CDN downloader (50 concurrent)
│   ├── parse_2v2.py        # 2v2 ranked replay → structured records
│   ├── parse_doudizhu.py   # 3-player mode replay → structured records
│   ├── enrich_ranks.py     # Backfill missing player metadata
│   ├── merge_indexes.py    # Cross-batch index deduplication
│   └── sync_r2.py          # R2 push/pull/cleanup
│
├── data/
│   ├── generals_mapping.csv   # 628-entry character ID mapping
│   └── output/                # Parsed CSVs + SQLite database
│
└── .github/workflows/
    └── daily.yml              # Cron-scheduled pipeline (every 2h)
```

## Running Locally

```bash
npm ci && pip install -r requirements.txt

# Full pipeline
export SGS_ACCOUNTS='...' SGS_PASSWORDS='...'
export R2_ENDPOINT='...' R2_BUCKET='...' R2_ACCESS_KEY_ID='...' R2_SECRET_ACCESS_KEY='...'

python pipeline/sync_r2.py --pull
node collect/collect.js
python pipeline/merge_indexes.py
python pipeline/download.py --use-indexes --days=7
python pipeline/parse_2v2.py --quiet --update-index
python pipeline/parse_doudizhu.py --quiet --update-index
python pipeline/enrich_ranks.py
python pipeline/sync_r2.py --push
```

## License

MIT
