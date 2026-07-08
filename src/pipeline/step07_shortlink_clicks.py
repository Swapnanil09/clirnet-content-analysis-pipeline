"""
Step 7 — Shortlink click events (was step_7__shortlink_Click.ipynb)

For every shortlink surfaced in Step 2, pulls the last click timestamp per
(short_url_id, uid) pair from ClickHouse. Writes `shortlink_click.parquet`.
"""
from __future__ import annotations

import logging

import pandas as pd

from .. import config, db

log = logging.getLogger("pipeline.step07")


def run() -> pd.DataFrame:
    data = pd.read_parquet(config.data_path("short_url_id-Notifications.parquet"))
    unique_ids = data["shorturl_id"].dropna().unique()
    log.info("Looking up clicks for %s unique shortlink IDs", len(unique_ids))

    def builder(id_csv: str) -> str:
        return f"""
            SELECT
                short_url_id,
                uid,
                toDate(max_time) AS date,
                formatDateTime(max_time, '%H:%i:%s') AS time
            FROM (
                SELECT short_url_id, uid, max(addMinutes(visited_at, 330)) AS max_time
                FROM prod_short_url_visits
                WHERE short_url_id IN ({id_csv})
                GROUP BY short_url_id, uid
            )
        """

    clicks = db.fetch_id_batches_clickhouse(unique_ids, builder)
    if clicks.empty:
        clicks = pd.DataFrame(columns=["short_url_id", "uid", "date", "time"])
    else:
        clicks["date"] = pd.to_datetime(clicks["date"])

    out_path = config.data_path("shortlink_click.parquet")
    clicks.to_parquet(out_path, index=False)
    log.info("Wrote %s (%s rows)", out_path, len(clicks))
    return clicks
