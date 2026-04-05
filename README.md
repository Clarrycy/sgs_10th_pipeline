# sgs_10th_pipeline

Data analytics pipeline for Sanguosha 10th Anniversary.

Collects match data, parses replay files, and stores structured results for win-rate analysis.

## Stack

- **Node.js** — data collection
- **Python 3** — replay parsing (`aiohttp`, `boto3`)
- **Cloudflare R2** — result storage
- **GitHub Actions** — scheduled automation (daily)

## Supported Modes

| Mode | Description |
|------|-------------|
| 2v2  | 2v2 ranked |
| Doudizhu | 3-player landlord |

## Usage

See pipeline scripts in `pipeline/` for download and parsing logic.
Automation runs daily via `.github/workflows/daily.yml`.
