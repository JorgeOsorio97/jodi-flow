Project Context: JoDi Data Pipeline
JoDi is an affiliate marketing company distributing offers via WhatsApp. Goal: Build a metrics pipeline that extracts data from multiple sources into PostgreSQL (RDS), with future visualization via Looker Studio.

Tech Stack & Architecture
Language: Python 3.11+ (Dockerized).

Storage (DB): PostgreSQL (AWS RDS), accessed via SSH bastion tunnel.

EL (Extract & Load):
- WhatsApp: Custom Python parser (`src/extraction/whatsapp_logs.py`).

Orchestration: GitHub Actions (Daily Cron) running Docker containers.

Visualization: Looker Studio (planned).

Local Development (Docker)
```bash
# Build and verify setup
docker compose run --rm setup-check
```

WhatsApp Extraction (local, outside Docker)
```bash
# Export to local CSV (no hashing, for debugging)
python -m src.extraction.whatsapp_logs --local <path_to_txt_or_directory>

# Upload to PostgreSQL (hashed, via SSH tunnel)
python -m src.extraction.whatsapp_logs <path_to_txt_or_directory>
```

Required files: `.env` with environment variables.

Data Pipeline Flow
1. Ingestion (EL) -> PostgreSQL (RDS)

WhatsApp: Parser script reads .txt exports (Spanish/English) -> Extracts join/leave/added events -> Loads to `raw_whatsapp_logs` table via SSH tunnel with deduplication (ON CONFLICT DO NOTHING).

Columns: timestamp, group_name, user_phone_hash, event_type (joined/left/added).

Supports both phone numbers (+52 55 1234 5678) and WhatsApp nicknames (~Username).

2. Transformation (T) -> TBD
Transformation layer to be implemented in a separate project (dbt or similar).

Infrastructure
- PostgreSQL: AWS RDS (jodiflow.ctigikugm0u3.us-east-2.rds.amazonaws.com)
- SSH Bastion: EC2 instance for tunneling to RDS
- SSH Key: PEM file configured via SSH_KEY_PATH env var

Development Constraints
Security: `.env` must be Git-ignored. SSH keys injected via GitHub Secrets.

SSH Tunnel: All PostgreSQL connections go through the EC2 bastion. The `sshtunnel` library requires `paramiko<4` for compatibility.

Password Encoding: DB passwords with special characters must be URL-encoded (`urllib.parse.quote_plus`) for SQLAlchemy connection strings.

Bulk Inserts: Use chunked multi-row INSERT statements (500 rows/chunk) for performance over SSH tunnels.
