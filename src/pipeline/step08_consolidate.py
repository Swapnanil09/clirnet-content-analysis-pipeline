"""
Step 8 — Consolidate (was Step_A1_-_read_the_files.ipynb)

The original notebook mostly read a parquet file and rewrote it unchanged
under the same or a new name. Those no-op copies are skipped here — only
the transforms that actually change the data are kept:
  - notification_temp: rename `id` -> `temp_id`, de-duplicate
  - shortlink_click: de-duplicate on (short_url_id, uid)

Two harmless renames are kept so downstream step file names stay stable
and self-documenting.
"""
from __future__ import annotations

import logging

import pandas as pd

from .. import config

log = logging.getLogger("pipeline.step08")


def run() -> None:
    # notification_temp: rename id -> temp_id, dedupe (feeds Step 9's join)
    temp = pd.read_parquet(config.data_path("notification_temp_Data.parquet"))
    temp = temp[["campaign_id", "transaction_id", "id"]].fillna(0)
    temp["id"] = temp["id"].astype(int)
    temp = temp.rename(columns={"id": "temp_id"})
    temp = temp.drop_duplicates(subset=["transaction_id", "temp_id"])
    temp.to_parquet(config.data_path("notification_temp.parquet"), index=False)
    log.info("notification_temp.parquet: %s rows", len(temp))

    # notification log: passthrough rename for a clearer downstream filename
    log_df = pd.read_parquet(config.data_path("Notification_log_all.parquet"))
    log_df.to_parquet(config.data_path("notification_log.parquet"), index=False)

    # shortlink dictionary: passthrough rename for a clearer downstream filename
    shortlink = pd.read_parquet(config.data_path("short_url_id-Notifications.parquet"))
    shortlink.to_parquet(config.data_path("notifications_with_shortlink.parquet"), index=False)

    # shortlink clicks: dedupe (feeds Step 9's join)
    clicks = pd.read_parquet(config.data_path("shortlink_click.parquet"))
    clicks = clicks.drop_duplicates(subset=["short_url_id", "uid"])
    clicks.to_parquet(config.data_path("shortlink_click.parquet"), index=False)
    log.info("shortlink_click.parquet (deduped): %s rows", len(clicks))

    log.info("Step 8 consolidation complete")
