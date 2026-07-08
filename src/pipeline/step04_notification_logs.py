"""
Step 4 — Notification delivery logs (was Step_4_finding_notification_log.ipynb)

For every `temp_id` found in Step 3, pull delivery status
(Sent/Delivered/Rejected/Seen) from ClickHouse first, then PlanetScale for
anything still missing. Writes `Notification_log_all.parquet`.
"""
from __future__ import annotations

import logging

import pandas as pd

from .. import config, db

log = logging.getLogger("pipeline.step04")

STATUS_MAP = {1: "Sent", 2: "Delivered", 3: "Rejected", 4: "Seen"}


def _apply_status(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    frame["status"] = frame["status"].astype("object").replace(STATUS_MAP)
    return frame


def run() -> pd.DataFrame:
    data = pd.read_parquet(config.data_path("notification_temp_Data.parquet"))
    data = data.dropna().copy()
    data["id"] = data["id"].fillna(0).astype(int)

    unique_ids = data["id"].dropna().unique()
    log.info("Looking up delivery logs for %s unique temp IDs", len(unique_ids))

    def ch_builder(id_csv: str) -> str:
        return f"""
            SELECT temp_id, status, uid, channel
            FROM prod_notification_logs
            WHERE temp_id IN ({id_csv})
        """

    from_ch = db.fetch_id_batches_clickhouse(unique_ids, ch_builder, batch_size=2000)
    if from_ch.empty:
        from_ch = pd.DataFrame(columns=["temp_id", "status", "uid", "channel"])

    merged_ch = pd.merge(data, from_ch, left_on="id", right_on="temp_id", how="left")
    merged_ch = _apply_status(merged_ch)

    remaining = merged_ch[merged_ch["temp_id"].isna()][["campaign_id", "transaction_id", "id", "created_at"]]
    merged_ch = merged_ch.dropna()
    log.info("ClickHouse resolved %s logs, %s temp IDs still missing", len(merged_ch), len(remaining))

    from_plantscale = pd.DataFrame(
        columns=["campaign_id", "transaction_id", "id", "created_at", "temp_id", "status", "uid", "channel"]
    )
    if len(remaining):
        ns_cfg = config.notification_shortlink_db()
        remaining_ids = remaining["id"].dropna().unique()

        def ps_builder(id_csv: str) -> str:
            return f"""
                SELECT temp_id, status, uid, channel
                FROM notification_logs
                WHERE temp_id IN ({id_csv})
                LIMIT %s OFFSET %s
            """

        ps_logs = db.fetch_id_batches_mysql_paginated(ns_cfg, remaining_ids, ps_builder, batch_size=1000)
        if ps_logs.empty:
            ps_logs = pd.DataFrame(columns=["temp_id", "status", "uid", "channel"])
        from_plantscale = pd.merge(remaining, ps_logs, left_on="id", right_on="temp_id", how="left").dropna()
        from_plantscale = _apply_status(from_plantscale)
        log.info("PlanetScale resolved %s more logs", len(from_plantscale))

    all_logs = pd.concat([merged_ch, from_plantscale], ignore_index=True)
    all_logs["created_at"] = pd.to_datetime(all_logs["created_at"])

    out_path = config.data_path("Notification_log_all.parquet")
    all_logs.to_parquet(out_path, index=False)
    log.info("Wrote %s (%s rows)", out_path, len(all_logs))
    return all_logs
