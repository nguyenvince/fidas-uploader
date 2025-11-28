from typing import Optional, Tuple
import sqlite3
import datetime

def setup_metadata_table(db_path: str) -> None:
    """Ensure a table exists to track the latest timestamp"""
    connection = sqlite3.connect(db_path)
    try:
        cursor = connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        connection.commit()
    finally:
        connection.close()


def get_last_timestamp(db_path: str) -> Optional[datetime.datetime]:
    """
    Return the saved last_timestamp from the meta table as a datetime object.

    The stored format is expected to be 'YYYYMMDDHHMMSS'. If missing or invalid,
    return None.
    """
    connection = sqlite3.connect(db_path)
    try:
        cursor = connection.cursor()
        cursor.execute(
            "SELECT value FROM meta WHERE key='last_timestamp'"
        )
        row = cursor.fetchone()
    finally:
        connection.close()

    if not row:
        return None

    ts_str = row[0]

    try:
        # Parse 'YYYYMMDDHHMMSS' → datetime
        return datetime.datetime.strptime(ts_str, "%Y%m%d%H%M%S")
    except Exception:
        LOGGER.warning("Invalid last_timestamp format stored in DB: %s", ts_str)
        return None

def set_last_timestamp(db_path: str, timestamp: datetime.datetime) -> None:
    """
    Store the last_timestamp as 'YYYYMMDDHHMMSS' in the meta table.
    """
    ts_str = timestamp.strftime("%Y%m%d%H%M%S")

    connection = sqlite3.connect(db_path)
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            INSERT OR REPLACE INTO meta (key, value)
            VALUES ('last_timestamp', ?)
            """,
            (ts_str,),
        )
        connection.commit()
    finally:
        connection.close()
