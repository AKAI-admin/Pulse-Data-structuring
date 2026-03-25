"""
Name cleaning and date extraction from folder paths.
Mirrors the logic from the original src/index.js but fixes the timezone issue.
"""

import re
from datetime import date
from pathlib import PurePosixPath, PureWindowsPath

# ── Name cleaning ────────────────────────────────────────────────────────────

_PREFIX_RE = re.compile(r"^(mrs|dr|review)\s+", re.IGNORECASE)
_SUFFIX_RE = re.compile(r"\s+(comparison|comarison)$", re.IGNORECASE)
_TRAILING_NUM_RE = re.compile(r"\s+\d+$")


def clean_patient_name(filename: str) -> str:
    stem = PureWindowsPath(filename).stem if "\\" in filename else PurePosixPath(filename).stem
    name = _TRAILING_NUM_RE.sub("", stem)
    name = _PREFIX_RE.sub("", name)
    name = _SUFFIX_RE.sub("", name)
    return name.strip()


def normalize_name_key(name: str) -> str:
    return " ".join(name.lower().split())


# ── Date parsing from folder names ───────────────────────────────────────────

_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

_DATE_LONG_RE = re.compile(r"^(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})$")
_DATE_DOT_RE = re.compile(r"^(\d{1,2})\.(\d{1,2})\.(\d{4})$")


def parse_date_folder(name: str) -> date | None:
    m = _DATE_LONG_RE.match(name)
    if m:
        day, month_str, year = int(m.group(1)), m.group(2).lower(), int(m.group(3))
        month_num = _MONTHS.get(month_str)
        if month_num is not None:
            try:
                return date(year, month_num, day)
            except ValueError:
                return None

    m = _DATE_DOT_RE.match(name)
    if m:
        day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return date(year, month, day)
        except ValueError:
            return None

    return None


def extract_study_date_and_year(relative_path: str) -> tuple[date | None, str | None]:
    """Return (study_date, year_str) by walking path components."""
    parts = PureWindowsPath(relative_path).parts if "\\" in relative_path else PurePosixPath(relative_path).parts

    study_date: date | None = None
    year_str: str | None = None

    for part in parts:
        if re.fullmatch(r"\d{4}", part):
            year_str = part
        parsed = parse_date_folder(part)
        if parsed:
            study_date = parsed
            if year_str is None:
                year_str = str(parsed.year)

    return study_date, year_str
