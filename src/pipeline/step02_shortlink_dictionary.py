"""
Step 2 — Shortlink dictionary (was step_2_notification_sent_details.ipynb)

Pulls every shortlink created this year for a real campaign (excludes
`_self` / `_ab` test variants), joins against `notification.parquet` on
`campaign_utmsource`, and writes `short_url_id-Notifications.parquet`.

The lower date bound is dynamic (Dec 31 of TARGET_YEAR - 1), replacing the
notebook's hardcoded `> "2025-12-31"`.
"""
from __future__ import annotations

import logging

import pandas as pd

from .. import config, db

log = logging.getLogger("pipeline.step02")

SHORTLINK_SQL = """
SELECT ssu.id as shorturl_id, sd.url_key, sd.service_code, sd.page_id,
       sd.user_name, sd.campaign as campaign_utmsource, ssu.created_at
FROM `shortlink_dictionaries` as sd
LEFT JOIN shortlink_short_urls as ssu ON sd.url_key = ssu.url_key
WHERE Date(sd.created_at) > %s AND sd.service_code is not null
  AND sd.campaign not like "%%_self" AND sd.campaign not like "%%_ab"
ORDER BY created_at
LIMIT %s OFFSET %s
"""


def run() -> pd.DataFrame:
    cfg = config.shortlink_db()
    lower = config.YTD_LOWER_BOUND_EXCLUSIVE.isoformat()
    log.info("Shortlink creation-date lower bound (exclusive): %s", lower)

    shortlinks = db.fetch_offset_paginated_all(cfg, SHORTLINK_SQL, params=(lower,))
    shortlinks = shortlinks.copy()
    shortlinks["campaign_utmsource"] = shortlinks["campaign_utmsource"].str.removesuffix("_final")
    log.info("Fetched %s shortlink rows", len(shortlinks))

    notification = pd.read_parquet(config.data_path("notification.parquet"))
    data = notification.merge(shortlinks, on="campaign_utmsource", how="left")
    compact = data.dropna().copy()
    compact["shorturl_id"] = compact["shorturl_id"].astype(int)

    out_path = config.data_path("short_url_id-Notifications.parquet")
    compact.to_parquet(out_path, index=False)
    log.info("Wrote %s (%s rows)", out_path, len(compact))
    return compact
