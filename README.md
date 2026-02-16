# JoDi Flow

Data pipeline for JoDi, an affiliate marketing company distributing offers via WhatsApp. Extracts engagement metrics from WhatsApp group chat exports and loads them into PostgreSQL for analysis.

## What it does

Parses WhatsApp `.txt` chat exports (Spanish/English) to extract member activity events:

- **joined** — user joined via group invite link
- **left** — user left the group
- **added** — user was added by an admin or member

Events are loaded into a `raw_whatsapp_logs` table in PostgreSQL (AWS RDS) with deduplication. Phone numbers and nicknames are SHA-256 hashed for privacy.

## Tech stack

- **Python 3.11+** (Dockerized)
- **PostgreSQL** on AWS RDS, accessed via SSH bastion tunnel
- **GitHub Actions** for daily scheduled extraction

## Setup

1. Clone the repo:
   ```bash
   git clone git@github.com:JorgeOsorio97/jodi-flow.git
   cd jodi-flow
   ```

2. Copy and fill in environment variables:
   ```bash
   cp .env.example .env
   ```

3. Install dependencies:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

## Usage

### Upload to PostgreSQL (production)

Hashes user identifiers and loads events via SSH tunnel to RDS:

```bash
python -m src.extraction.whatsapp_logs <path_to_txt_or_directory>
```

### Export to local CSV (debugging)

Keeps raw (unhashed) identifiers and writes to `data/raw/whatsapp_logs.csv`:

```bash
python -m src.extraction.whatsapp_logs --local <path_to_txt_or_directory>
```

### Docker

```bash
# Verify dependencies are installed
docker compose run --rm setup-check
```

## Project structure

```
jodi-flow/
├── src/
│   ├── extraction/
│   │   └── whatsapp_logs.py   # WhatsApp chat parser & loader
│   └── setup_check.py         # Dependency sanity check
├── .github/workflows/
│   └── daily-pipeline.yml     # GitHub Actions daily cron
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

## How the pipeline works

1. **Extract**: Parse WhatsApp `.txt` exports to identify join/leave/added events using regex patterns for Spanish and English formats.
2. **Load**: Insert events into PostgreSQL via SSH tunnel using chunked multi-row `INSERT ... ON CONFLICT DO NOTHING` for deduplication and performance.
3. **Transform**: (Planned) dbt or similar transformation layer in a separate project.
4. **Visualize**: (Planned) Looker Studio dashboards.
