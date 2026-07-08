"""
Step 3 — Notification "temp" IDs (was step_3_notification_sent_details_form_bigQ.ipynb)

For every campaign transaction_id, look up its internal notification
`temp_id` — first in ClickHouse (`prod_notification_temp`), then for
anything still missing, in the PlanetScale fallback table
(`notification-shortlink-db.notification_temp`). Writes
`notification_temp_Data.parquet`.
"""
from __future__ import annotations

import json
import logging

import pandas as pd

from .. import config, db

log = logging.getLogger("pipeline.step03")


def _clickhouse_temp_ids(unique_ids) -> pd.DataFrame:
    def builder(id_csv: str) -> str:
        return f"SELECT id, transaction_id, created_at FROM prod_notification_temp WHERE transaction_id in ({id_csv})"

    df = db.fetch_id_batches_clickhouse(unique_ids, builder, quote=lambda x: f"'{x}'")
    if df.empty:
        df = pd.DataFrame(columns=["id", "transaction_id", "created_at"])
    return df


def _planetscale_temp_ids(remaining_ids) -> pd.DataFrame:
    if len(remaining_ids) == 0:
        return pd.DataFrame(columns=["id", "transaction_id", "created_at"])

    cfg = config.notification_shortlink_db()

    def builder(id_csv: str) -> str:
        return f"""
            SELECT id, transaction_id, created_at
            FROM notification_temp
            WHERE transaction_id IN ({id_csv})
            LIMIT %s OFFSET %s
        """

    return db.fetch_id_batches_mysql_paginated(cfg, remaining_ids, builder, quote=lambda x: f'"{x}"')


def run() -> pd.DataFrame:
    data = pd.read_parquet(config.data_path("notification.parquet"))
    data = data.dropna(subset=["transaction_id"])

    df = data[["campaign_id", "transaction_id"]].copy()
    df["transaction_id"] = df["transaction_id"].apply(lambda x: json.loads(x.replace("\n", "")))
    flattened = df.explode("transaction_id").reset_index(drop=True)

    unique_ids = flattened["transaction_id"].dropna().unique()
    log.info("Looking up %s unique transaction IDs", len(unique_ids))

    from_clickhouse = _clickhouse_temp_ids(unique_ids)
    log.info("ClickHouse resolved %s temp IDs", len(from_clickhouse))

    joined = flattened.merge(from_clickhouse, on="transaction_id", how="left")
    still_missing = joined.loc[joined["created_at"].isna(), "transaction_id"].dropna().unique()
    log.info("%s transaction IDs still missing — falling back to PlanetScale", len(still_missing))

    from_planetscale = _planetscale_temp_ids(still_missing)
    log.info("PlanetScale resolved %s more temp IDs", len(from_planetscale))

    all_notification_temp = pd.concat([from_clickhouse, from_planetscale], ignore_index=True)
    notification_temp = flattened.merge(all_notification_temp, on="transaction_id", how="left")
    notification_temp.dropna(inplace=True)

    out_path = config.data_path("notification_temp_Data.parquet")
    notification_temp.to_parquet(out_path, index=False)
    log.info("Wrote %s (%s rows)", out_path, len(notification_temp))
    return notification_temp
