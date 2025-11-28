import re
from typing import Optional, Tuple
import os

FILENAME_RE = re.compile(
    r"^DUSTMONITOR_\d+_(\d{4})_(\d{2})\.txt$", re.IGNORECASE
)

def extract_year_month(filename: str) -> Optional[Tuple[int, int]]:
    """Extract (year, month) from DUSTMONITOR_..._YYYY_MM.txt filenames."""
    m = FILENAME_RE.match(os.path.basename(filename))
    if not m:
        return None
    year, month = int(m.group(1)), int(m.group(2))
    return year, month