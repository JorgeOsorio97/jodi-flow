"""Parse WhatsApp group chat exports and extract join/leave events."""

import hashlib
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from urllib.parse import quote_plus

from sqlalchemy import create_engine, text
from sshtunnel import SSHTunnelForwarder

load_dotenv()

RAW_TABLE_NAME = "raw_whatsapp_logs"
LOCAL_CSV_PATH = Path(__file__).resolve().parents[2] / "data" / "raw" / "whatsapp_logs.csv"

# PostgreSQL configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres")
POSTGRES_USER = os.getenv("POSTGRES_USER", "jodi")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

# SSH Bastion configuration
SSH_BASTION_HOST = os.getenv("SSH_BASTION_HOST", "")
SSH_BASTION_PORT = int(os.getenv("SSH_BASTION_PORT", "22"))
SSH_BASTION_USER = os.getenv("SSH_BASTION_USER", "ec2-user")
SSH_KEY_PATH = os.path.expanduser(os.getenv("SSH_KEY_PATH", "~/.ssh/id_rsa"))

# Regex: match lines starting with a WhatsApp timestamp
LINE_RE = re.compile(r"^(\d{1,2}/\d{1,2}/\d{4}, \d{1,2}:\d{2})\s*-\s*(.+)$")

# User identifier: phone number (+52 55 1234 5678) or nickname (~Currio🦝)
# WhatsApp uses \u202f (narrow no-break space) or regular space after ~
_USER_RE = r"(?:\+[\d\s]+?|~[\s\u202f].+?)"

# Event patterns (Spanish WhatsApp exports)
JOINED_RE = re.compile(rf"^[\u200e]?({_USER_RE})\s+se unió con el enlace del grupo")
LEFT_RE = re.compile(rf"^[\u200e]?({_USER_RE})\s+salió del grupo")
ADDED_BY_ADMIN_RE = re.compile(rf"^Se añadió a ({_USER_RE})\s*\.?\s*$")
ADDED_BY_MEMBER_RE = re.compile(rf"^[\u200e]?({_USER_RE})\s+añadió a (.+)")


def hash_phone(phone: str) -> str:
    """Hash a phone number or nickname for privacy using SHA-256."""
    normalized = re.sub(r"\s+", "", phone).strip("\u200e")
    return hashlib.sha256(normalized.encode()).hexdigest()[:16]


def extract_group_name(filepath: str) -> str:
    """Extract group name from the WhatsApp export filename."""
    filename = Path(filepath).stem
    for prefix in ("Chat de WhatsApp con ", "WhatsApp Chat with "):
        if filename.startswith(prefix):
            return filename[len(prefix):]
    return filename


def parse_added_users(text: str) -> list[str]:
    """Parse one or more users (phone numbers or nicknames) from an 'añadió a' message."""
    parts = re.split(r"\s+y\s+|,\s*", text.rstrip("."))
    users = []
    for part in parts:
        part = part.strip().strip("\u200e")
        phone_match = re.search(r"(\+[\d\s]+)", part)
        if phone_match:
            users.append(phone_match.group(1).strip())
        elif part:
            users.append(part)
    return users


def parse_chat_file(filepath: str, hash_users: bool = True) -> list[dict]:
    """
    Parse a WhatsApp chat export file and extract join/leave events.

    Args:
        filepath: Path to the WhatsApp chat export .txt file.
        hash_users: If True, hash phone numbers/nicknames. If False, keep raw values.

    Returns:
        List of event dicts with keys: timestamp, group_name, user_phone_hash, event_type.
    """
    group_name = extract_group_name(filepath)
    events = []
    identify = hash_phone if hash_users else lambda x: re.sub(r"\s+", "", x).strip("\u200e")

    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            line_match = LINE_RE.match(line)
            if not line_match:
                continue

            timestamp_str = line_match.group(1)
            message = line_match.group(2)

            try:
                timestamp = datetime.strptime(timestamp_str, "%d/%m/%Y, %H:%M")
            except ValueError:
                continue

            m = JOINED_RE.match(message)
            if m:
                events.append({
                    "timestamp": timestamp,
                    "group_name": group_name,
                    "user_phone_hash": identify(m.group(1)),
                    "event_type": "joined",
                })
                continue

            m = LEFT_RE.match(message)
            if m:
                events.append({
                    "timestamp": timestamp,
                    "group_name": group_name,
                    "user_phone_hash": identify(m.group(1)),
                    "event_type": "left",
                })
                continue

            m = ADDED_BY_ADMIN_RE.match(message)
            if m:
                events.append({
                    "timestamp": timestamp,
                    "group_name": group_name,
                    "user_phone_hash": identify(m.group(1)),
                    "event_type": "added",
                })
                continue

            m = ADDED_BY_MEMBER_RE.match(message)
            if m:
                added_phones = parse_added_users(m.group(2))
                for phone in added_phones:
                    events.append({
                        "timestamp": timestamp,
                        "group_name": group_name,
                        "user_phone_hash": identify(phone),
                        "event_type": "added",
                    })
                continue

    return events


def get_ssh_tunnel() -> SSHTunnelForwarder:
    """Create SSH tunnel to the bastion host."""
    tunnel = SSHTunnelForwarder(
        (SSH_BASTION_HOST, SSH_BASTION_PORT),
        ssh_username=SSH_BASTION_USER,
        ssh_pkey=SSH_KEY_PATH,
        remote_bind_address=(POSTGRES_HOST, POSTGRES_PORT),
        allow_agent=False,
        host_pkey_directories=[],
        set_keepalive=10,
    )
    tunnel.ssh_host_key = None  # Skip host key verification
    return tunnel


def get_engine(local_port: int):
    """Create SQLAlchemy engine connecting through the SSH tunnel."""
    encoded_password = quote_plus(POSTGRES_PASSWORD)
    connection_string = (
        f"postgresql://{POSTGRES_USER}:{encoded_password}"
        f"@127.0.0.1:{local_port}/{POSTGRES_DB}"
    )
    return create_engine(connection_string)


def ensure_table(engine) -> None:
    """Create the raw_whatsapp_logs table if it doesn't exist."""
    with engine.connect() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {RAW_TABLE_NAME} (
                timestamp TIMESTAMP NOT NULL,
                group_name TEXT NOT NULL,
                user_phone_hash TEXT NOT NULL,
                event_type TEXT NOT NULL,
                UNIQUE (timestamp, group_name, user_phone_hash, event_type)
            )
        """))
        conn.commit()


def load_to_postgres(df: pd.DataFrame, engine, chunk_size: int = 500) -> int:
    """
    Load DataFrame to PostgreSQL, skipping duplicates.

    Uses chunked multi-row INSERT ... ON CONFLICT DO NOTHING for performance
    over SSH tunnels (fewer round-trips).

    Returns:
        Number of new rows inserted.
    """
    if df.empty:
        print("No events to load.")
        return 0

    ensure_table(engine)

    records = df.to_dict("records")
    total_new = 0

    with engine.connect() as conn:
        for i in range(0, len(records), chunk_size):
            chunk = records[i : i + chunk_size]
            # Build multi-row VALUES clause
            placeholders = []
            params = {}
            for j, rec in enumerate(chunk):
                placeholders.append(
                    f"(:ts_{j}, :gn_{j}, :uph_{j}, :et_{j})"
                )
                params[f"ts_{j}"] = rec["timestamp"]
                params[f"gn_{j}"] = rec["group_name"]
                params[f"uph_{j}"] = rec["user_phone_hash"]
                params[f"et_{j}"] = rec["event_type"]

            sql = text(f"""
                INSERT INTO {RAW_TABLE_NAME} (timestamp, group_name, user_phone_hash, event_type)
                VALUES {', '.join(placeholders)}
                ON CONFLICT (timestamp, group_name, user_phone_hash, event_type) DO NOTHING
            """)
            result = conn.execute(sql, params)
            total_new += result.rowcount
            print(f"  Chunk {i // chunk_size + 1}: inserted {result.rowcount}/{len(chunk)} rows", flush=True)

        conn.commit()

    skipped = len(records) - total_new
    print(f"Inserted {total_new} new rows to {RAW_TABLE_NAME} ({skipped} duplicates skipped)")
    return total_new


def load_to_csv(df: pd.DataFrame) -> None:
    """Export DataFrame to local CSV, merging with existing data and deduplicating."""
    LOCAL_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)

    if LOCAL_CSV_PATH.exists():
        existing = pd.read_csv(LOCAL_CSV_PATH)
        combined = pd.concat([existing, df], ignore_index=True)
        combined["timestamp"] = pd.to_datetime(combined["timestamp"]).astype(str)
        combined = combined.drop_duplicates(
            subset=["timestamp", "group_name", "user_phone_hash", "event_type"]
        )
        new_rows = len(combined) - len(existing)
    else:
        combined = df.copy()
        combined["timestamp"] = pd.to_datetime(combined["timestamp"]).astype(str)
        new_rows = len(combined)

    combined.to_csv(LOCAL_CSV_PATH, index=False)
    skipped = len(df) - new_rows
    print(f"Saved {new_rows} new rows to {LOCAL_CSV_PATH} ({skipped} duplicates skipped)")


def run_extraction(path: str, local: bool = False) -> None:
    """
    Run WhatsApp log extraction.

    Args:
        path: Path to a single .txt file or a directory containing .txt files.
        local: If True, export to CSV instead of PostgreSQL.
    """
    input_path = Path(path)

    if input_path.is_dir():
        txt_files = sorted(input_path.glob("*.txt"))
        if not txt_files:
            print(f"No .txt files found in {input_path}")
            return
        print(f"Found {len(txt_files)} chat files in {input_path}")
    elif input_path.is_file():
        txt_files = [input_path]
    else:
        print(f"Path not found: {path}")
        sys.exit(1)

    all_events = []
    for txt_file in txt_files:
        print(f"  Parsing: {txt_file.name}")
        events = parse_chat_file(str(txt_file), hash_users=not local)
        all_events.extend(events)

    df = pd.DataFrame(all_events)

    if df.empty:
        print("No join/leave events found.")
        return

    joined = len(df[df["event_type"] == "joined"])
    left = len(df[df["event_type"] == "left"])
    added = len(df[df["event_type"] == "added"])
    print(f"Parsed {len(df)} events: {joined} joined, {left} left, {added} added")

    if local:
        load_to_csv(df)
    else:
        print(f"Connecting via SSH tunnel ({SSH_BASTION_HOST})...")
        tunnel = get_ssh_tunnel()
        tunnel.start()
        print(f"  Tunnel started on local port {tunnel.local_bind_port}")

        try:
            engine = get_engine(tunnel.local_bind_port)
            load_to_postgres(df, engine)
        finally:
            tunnel.stop()
            print("SSH tunnel closed.")

    print("WhatsApp log extraction complete.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m src.extraction.whatsapp_logs [--local] <path_to_chat.txt_or_directory>")
        sys.exit(1)

    args = sys.argv[1:]
    local_mode = "--local" in args
    if local_mode:
        args.remove("--local")

    run_extraction(args[0], local=local_mode)
