#!/usr/bin/env python3
"""FIDAS text via FTP → CSV → CITIESair.

This script:
- Connects to an FTP server where FIDAS .txt files are stored.
- Tracks per-file processing state in a SQLite database.
- Aggregates raw 1-minute (or similar) data to hourly means for
  completed hours.
- Appends/creates monthly CSVs in a local directory.
- Sends each new batch of hourly rows to the IQAir OpenAir API
  as a CSV upload.
"""

from __future__ import annotations

import datetime
import io
import logging
import os
import sqlite3
import time
from ftplib import FTP, all_errors as FTP_ERRORS
from typing import List, Optional, Tuple

import pandas as pd
import requests

from appConfig import (
    API_URL,
    DB_PATH,
    FTP_HOST,
    FTP_HOME_DIR,
    FTP_PASSWORD,
    FTP_PORT,
    FTP_USERNAME,
    HEADERS,
    SLEEP_INTERVAL,
    TZ_OFFSET
)

from utils import (extract_year_month)

from metadata_table import (setup_metadata_table, get_last_timestamp, set_last_timestamp)

LOGGER = logging.getLogger("CITESair_uploader")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ---------------------------------------------------------------------------
# FTP helpers
# ---------------------------------------------------------------------------
def create_ftp_client() -> Optional[FTP]:
    """Create, connect, and authenticate a plain FTP client.

    NOTE:
        The remote server does not support AUTH TLS (FTPS). This client
        therefore uses unencrypted FTP. Ensure this is acceptable in
        your environment (e.g., local/VPN-only traffic).
    """
    try:
        ftp = FTP()
        ftp.connect(host=FTP_HOST, port=FTP_PORT, timeout=30)
        ftp.login(user=FTP_USERNAME, passwd=FTP_PASSWORD)
        LOGGER.info(
            "Connected to FTP server %s:%s as user '%s'.",
            FTP_HOST,
            FTP_PORT,
            FTP_USERNAME,
        )

        # Attempt to change into the configured home directory. If the
        # server chroots the user, this may fail; in that case we stay
        # in the default directory and continue.
        if FTP_HOME_DIR:
            try:
                ftp.cwd(FTP_HOME_DIR)
                LOGGER.info(
                    "Changed FTP working directory to '%s'.", FTP_HOME_DIR
                )
            except FTP_ERRORS as exc:
                LOGGER.warning(
                    "Could not change to FTP_HOME_DIR '%s': %s. "
                    "Continuing in current directory.",
                    FTP_HOME_DIR,
                    exc,
                )

        return ftp
    except FTP_ERRORS as exc:
        LOGGER.error("FTP connection or login failed: %s", exc)
        return None

def list_remote_txt_files_with_mtime(ftp: FTP) -> List[Tuple[str, Optional[datetime.datetime]]]:
    """
    Return list of (filename, mtime) for .txt files.

    mtime is a naive datetime parsed from the FTP 'modify' fact (YYYYMMDDHHMMSS).
    If we can't determine mtime for some file, its mtime is None.
    """
    files: List[Tuple[str, Optional[datetime.datetime]]] = []

    try:
        # MLSD gives (name, facts) where facts may include 'modify'
        for name, facts in ftp.mlsd():
            if not name.lower().endswith(".txt"):
                continue

            modify = facts.get("modify")
            if modify:
                # 'modify' looks like '20251126094512'
                try:
                    mtime = datetime.datetime.strptime(modify, "%Y%m%d%H%M%S")
                except ValueError:
                    LOGGER.warning("Could not parse modify time '%s' for '%s'", modify, name)
                    mtime = None
            else:
                mtime = None

            files.append((name, mtime))

    except error_perm as exc:
        # Server doesn't support MLSD; caller can fall back to MDTM version
        LOGGER.warning("MLSD not supported by server: %s", exc)

    return sorted(files, key=lambda t: (t[1] or datetime.datetime.min, t[0]))

def select_files_to_process(remote_files: List[Tuple[str, Optional[datetime.datetime]]], last_timestamp: Optional[datetime.datetime]) -> List[str]:
    """
    Given a list of (filename, mtime) returned by list_remote_txt_files_with_mtime,
    return the list of filenames that should be processed.
    
    Rules:
      - If last_timestamp is None → process ALL files
      - If mtime is None → skip the file (no way to compare safely)
      - If mtime > last_timestamp → include
    """
    files_to_process: List[str] = []

    for filename, mtime in remote_files:
        if mtime is None:
            LOGGER.warning(
                "Skipping '%s' because it has no mtime from FTP.", filename
            )
            continue

        if last_timestamp is None or mtime > last_timestamp:
            files_to_process.append(filename)

    # Sort filenames alphabetically for deterministic order
    files_to_process.sort()
    return files_to_process

# ---------------------------------------------------------------------------
# Raw file processing and API upload
# ---------------------------------------------------------------------------
def clean(x, transform=None):
    """
    Return cleaned value:
    - If x is NaN → return None
    - Else apply optional transform(x)
    """
    if pd.isna(x):
        return None
    return transform(x) if transform else x


def process_raw_file(
    ftp: FTP,
    filename: str,
    last_timestamp: Optional[datetime.datetime]
) -> Optional[Tuple[List[dict], datetime.datetime]]:
    """
    Download a remote FIDAS .txt file and convert NEW rows into
    backend measurement JSON objects.

    Returns:
        A list of measurement dicts, or None if nothing new.
    """
    buffer = io.BytesIO()

    # -- Fetch file over FTP
    try:
        ftp.retrbinary(f"RETR {filename}", callback=buffer.write)
    except FTP_ERRORS as exc:
        LOGGER.error("Could not retrieve %s: %s", filename, exc)
        return None

    buffer.seek(0)

    # -- Parse file
    try:
        df = pd.read_table(buffer)
    except Exception as exc:
        LOGGER.error("Failed to parse %s: %s", filename, exc)
        return None
    finally:
        buffer.close()

    if df.empty:
        LOGGER.info("File %s is empty.", filename)
        return None

    # --- Ensure required columns exist
    required = ["date", "time", "PM1", "PM2.5", "PM10", "rH", "T", "p"]
    for col in required:
        if col not in df.columns:
            LOGGER.error("Missing required column '%s' in %s", col, filename)
            return None

    # --- Construct timestamp column
    try:
        df["ts"] = pd.to_datetime(
            df["date"] + " " + df["time"],
            format="%m/%d/%Y %I:%M:%S %p"
        )

    except Exception as exc:
        LOGGER.error("Date parsing error in %s: %s", filename, exc)
        return None

    # --- Filter only new rows
    if last_timestamp:
        df = df[df["ts"] > last_timestamp]

    if df.empty:
        LOGGER.info("No new rows in %s after last_timestamp.", filename)
        return None


    df = df.sort_values("ts")
    # newest naive timestamp (before timezone added)
    newest_timestamp = df["ts"].iloc[-1]

    # --- Build measurement objects
    measurements = []

    for idx, row in df.iterrows():
        m = {
            "ts": row["ts"].replace(tzinfo=datetime.timezone(datetime.timedelta(hours=TZ_OFFSET))).isoformat(),
            "t": clean(row["T"]),
            "h": clean(row["rH"]),
            "p": clean(row["p"], lambda v: int(round(v * 100))),
            "p1": clean(row["PM1"]),
            "p25": clean(row["PM2.5"]),
            "p10": clean(row["PM10"])
        }
        measurements.append(m)

    return measurements, newest_timestamp

def send_measurements_through_api(measurements: List[dict]) -> bool:
    """
    Send raw measurement data JSON array to backend.

    Returns True on success, False on failure.
    """
    try:
        response = requests.post(
            API_URL,
            headers=HEADERS,
            json=measurements,
            timeout=30
        )
    except requests.RequestException as exc:
        LOGGER.error("Network error sending measurements: %s", exc)
        return False

    LOGGER.info("Backend status: %s", response.status_code)

    if response.status_code not in (200, 207):
        LOGGER.error("Backend reject: %s", response.text)
        return False

    return True

# ---------------------------------------------------------------------------
# Main loop helpers
# ---------------------------------------------------------------------------

def sleep_until_next_run(interval_seconds: int = 60) -> None:
    """Sleep until the next scheduled run.

    The next run is simply ``now + interval_seconds``. This keeps the
    loop readable and close to the original behaviour (check roughly
    every minute).
    """
    now = datetime.datetime.now()
    next_run = now + datetime.timedelta(seconds=interval_seconds)
    sleep_seconds = max(0, int((next_run - now).total_seconds()))

    LOGGER.info(
        "Sleeping for %d seconds until next check at %s.",
        sleep_seconds,
        next_run.isoformat(timespec="seconds"),
    )
    time.sleep(sleep_seconds)


def main() -> None:
    setup_metadata_table(DB_PATH)

    while True:
        ftp = create_ftp_client()
        if ftp is None:
            LOGGER.error(
                "FTP client could not be created; skipping this cycle and "
                "will retry after the sleep interval.",
            )
        else:
            try:
                """Fetch all the available .txt files and filter out the old files"""
                remote_files = list_remote_txt_files_with_mtime(ftp)
                last_timestamp = get_last_timestamp(DB_PATH)
                files_to_process = select_files_to_process(remote_files, last_timestamp)

                if not files_to_process:
                    LOGGER.info("No updated files found since last timestamp: %s", last_timestamp)
                else:
                    LOGGER.info("Files selected for processing: %s", files_to_process)

                for filename in files_to_process:
                    LOGGER.info("Processing %s ...", filename)

                    result = process_raw_file(
                        ftp=ftp,
                        filename=filename,
                        last_timestamp=last_timestamp
                    )
                    if not result:
                        continue

                    measurements, newest_timestamp = result

                    # --- Send to backend
                    ok = send_measurements_through_api(measurements)
                    if not ok:
                        continue

                    # --- Update last_timestamp
                    set_last_timestamp(DB_PATH, newest_timestamp)
                    last_timestamp = newest_timestamp

                    LOGGER.info("Updated last_timestamp to %s", newest_timestamp)
          
            finally:
                try:
                    ftp.quit()
                except FTP_ERRORS:
                    # Some servers may close the connection early; ignore.
                    try:
                        ftp.close()
                    except FTP_ERRORS:
                        pass

        sleep_until_next_run(interval_seconds=SLEEP_INTERVAL)

if __name__ == "__main__":
    main()
