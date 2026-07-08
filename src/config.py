"""
Central configuration for the CLIRNET Content Analysis BI pipeline.

This is the single source of truth for:
  - which month the pipeline reports on (TARGET_MONTH)
  - every date boundary derived from it (replaces ALL hardcoded dates that
    used to live inside the 11 notebooks, e.g. "2025-12-31", "2026-07-01",
    "dt.month == 6", "01-06-2026", filenames ending in "_JUNE")
  - database credentials, read from environment variables / GitHub Secrets
    (replaces every plaintext password that used to be committed inside
    the notebooks)
  - output file paths and naming

Nothing else in this pipeline should contain a hardcoded date, year, or
credential. Everything flows from TARGET_MONTH and the environment.
"""
from __future__ import annotations

import calendar
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from dateutil.relativedelta import relativedelta


def _env(name: str, default: str | None = None) -> str | None:
    """os.getenv, but treats an empty string the same as 'unset' so that
    GitHub Actions referencing an undefined secret (which resolves to "")
    doesn't silently stomp on a sensible default."""
    val = os.getenv(name)
    if val is None or val == "":
        return default
    return val


# ---------------------------------------------------------------------------
# 1. WHICH MONTH ARE WE REPORTING ON?
# ---------------------------------------------------------------------------
def _resolve_target_month() -> date:
    """
    Returns the 1st-of-month date this pipeline run reports on.

    Resolution order:
      1. TARGET_MONTH env var, format "YYYY-MM" (e.g. "2026-06").
         Set this for backfills / re-runs, or pass it as a
         workflow_dispatch input in GitHub Actions.
      2. Default: the PREVIOUS calendar month relative to "today".
         The pipeline is designed to run early in a month (via the
         GitHub Actions cron) and report on the month that just closed
         (e.g. it runs on Jul 1st and reports on June).
    """
    raw = (_env("TARGET_MONTH") or "").strip()
    if raw:
        try:
            year_str, month_str = raw.split("-")
            return date(int(year_str), int(month_str), 1)
        except ValueError as exc:
            raise ValueError(
                f'TARGET_MONTH="{raw}" is invalid — expected format "YYYY-MM", e.g. "2026-06"'
            ) from exc
    today = date.today()
    return today.replace(day=1) - relativedelta(months=1)


TARGET_MONTH: date = _resolve_target_month()
TARGET_YEAR: int = TARGET_MONTH.year
TARGET_MONTH_NUM: int = TARGET_MONTH.month
MONTH_NAME: str = calendar.month_name[TARGET_MONTH_NUM]           # "June"
MONTH_NAME_SHORT: str = calendar.month_abbr[TARGET_MONTH_NUM]     # "Jun"
TARGET_MONTH_LABEL: str = f"{MONTH_NAME} {TARGET_YEAR}"            # "June 2026"
TARGET_MONTH_DATE_DDMMYYYY: str = f"01-{TARGET_MONTH_NUM:02d}-{TARGET_YEAR}"  # "01-06-2026"

# ---------------------------------------------------------------------------
# 2. DERIVED DATE BOUNDARIES (these replace every hardcoded date in the
#    original notebooks)
# ---------------------------------------------------------------------------
# Year-to-date window used by Step 1 / Step 2 to pull campaign & shortlink
# data: everything from Jan 1st of the target year through the end of the
# target month. Expressed as exclusive SQL boundaries the same way the
# original notebooks did it (`> day-before-Jan-1` and `< first-day-after-month`).
YTD_LOWER_BOUND_EXCLUSIVE: date = date(TARGET_YEAR - 1, 12, 31)
YTD_UPPER_BOUND_EXCLUSIVE: date = TARGET_MONTH + relativedelta(months=1)

# The original step_1 notebook additionally excluded January from the YTD
# window after fetching it (a hardcoded `dt.month > 1` filter). We preserve
# that behaviour by default — except when the target month itself IS
# January, in which case excluding it would zero out the whole dataset.
# If January was excluded for a real business reason (bad historical data,
# fiscal year boundary, etc.) rather than by accident, leave this as-is.
# If it was NOT intentional, set EXCLUDE_JANUARY = False.
EXCLUDE_JANUARY: bool = TARGET_MONTH_NUM != 1


def in_month_window_mask(series):
    """
    Reproduces the original notebook's `(month < 7) & (month > 1)` filter,
    generalised: keep any month from February through TARGET_MONTH
    (inclusive), or, if TARGET_MONTH is January, keep January only.
    """
    months = series.dt.month
    if EXCLUDE_JANUARY:
        return (months > 1) & (months <= TARGET_MONTH_NUM)
    return months == TARGET_MONTH_NUM


# ---------------------------------------------------------------------------
# 3. DATABASE CREDENTIALS — never hardcoded, always from the environment
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class MySQLConfig:
    host: str
    user: str
    password: str
    database: str


def _mysql_cfg(prefix: str) -> MySQLConfig:
    def required(suffix: str) -> str:
        val = _env(f"{prefix}_{suffix}")
        if not val:
            raise EnvironmentError(
                f"Missing required environment variable {prefix}_{suffix}. "
                f"Copy .env.example to .env and fill it in locally, or add it "
                f"as a GitHub Actions secret (see README.md)."
            )
        return val

    return MySQLConfig(
        host=required("HOST"),
        user=required("USER"),
        password=required("PASSWORD"),
        database=required("DATABASE"),
    )


def crm_db() -> MySQLConfig:
    """`crm-db` on PlanetScale — campaigns, shortlink-adjacent data."""
    return _mysql_cfg("CRM_DB")


def crm_db_replica() -> MySQLConfig:
    """`crm-db@replica` — same credentials as crm_db(), different logical DB."""
    base = _mysql_cfg("CRM_DB")
    replica_db = _env("CRM_DB_REPLICA_DATABASE") or "crm-db@replica"
    return MySQLConfig(base.host, base.user, base.password, replica_db)


def shortlink_db() -> MySQLConfig:
    """`shortlink-db` — shortlink_dictionaries / shortlink_short_urls."""
    return _mysql_cfg("SHORTLINK_DB")


def notification_shortlink_db() -> MySQLConfig:
    """`notification-shortlink-db` — notification_temp / notification_logs."""
    return _mysql_cfg("NOTIFICATION_SHORTLINK_DB")


def clirnet_db() -> MySQLConfig:
    """`clirnetdb` — content tables (MedWiki, video, CME, etc.) + user_master."""
    return _mysql_cfg("CLIRNET_DB")


SSL_CA_PATH: str = _env("SSL_CA_PATH") or "/etc/ssl/certs/ca-certificates.crt"


@dataclass(frozen=True)
class ClickHouseConfig:
    host: str
    user: str
    password: str
    secure: bool = True


def clickhouse_cfg() -> ClickHouseConfig:
    host = _env("CLICKHOUSE_HOST")
    password = _env("CLICKHOUSE_PASSWORD")
    if not host or not password:
        raise EnvironmentError(
            "CLICKHOUSE_HOST / CLICKHOUSE_PASSWORD not set. "
            "Copy .env.example to .env and fill it in, or add GitHub secrets."
        )
    user = _env("CLICKHOUSE_USER") or "default"
    return ClickHouseConfig(host=host, user=user, password=password)


# ---------------------------------------------------------------------------
# 4. EMAIL
# ---------------------------------------------------------------------------
EMAIL_FROM: str | None = _env("EMAIL_FROM")
EMAIL_PASSWORD: str | None = _env("EMAIL_PASSWORD")
EMAIL_SMTP_SERVER: str = _env("EMAIL_SMTP_SERVER") or "smtp.office365.com"
EMAIL_SMTP_PORT: int = int(_env("EMAIL_SMTP_PORT") or "587")
EMAIL_SIGNOFF_NAME: str = _env("EMAIL_SIGNOFF_NAME") or "Swapnanil Chatterjee"
EMAIL_TO: list[str] = [addr.strip() for addr in (_env("EMAIL_TO") or "").split(",") if addr.strip()]

# ---------------------------------------------------------------------------
# 5. PATHS / OUTPUT FILE NAMES
# ---------------------------------------------------------------------------
DATA_DIR = Path(_env("PIPELINE_DATA_DIR") or "data")
DATA_DIR.mkdir(parents=True, exist_ok=True)


def data_path(filename: str) -> Path:
    return DATA_DIR / filename


# The 3 final deliverables — these are the exact files emailed out every run.
OUT_SPECIALITY_MONTH_DIFF: Path = data_path(f"Speciality_month_diff_campaign_{MONTH_NAME}.csv")
OUT_CONTENT_TEMPLATE_ANALYSIS: Path = data_path(f"Content_template_analysis_{MONTH_NAME}.csv")
OUT_ALL_CONTENT_DATA: Path = data_path("All_content_data_Current.xlsx")

# ---------------------------------------------------------------------------
# 6. CHUNK / BATCH SIZES — tune via env vars for very large tables
#    (e.g. user_data_index, which can hold millions of physician records)
# ---------------------------------------------------------------------------
SQL_PAGE_SIZE: int = int(_env("SQL_PAGE_SIZE") or "100000")
ID_BATCH_SIZE: int = int(_env("ID_BATCH_SIZE") or "1000")
SPEC_BATCH_SIZE: int = int(_env("SPEC_BATCH_SIZE") or "5000")
