"""
Step 9 — Join notifications + delivery status + click events (was Step_A2_-_basic_join.ipynb)

Produces `notifications_click.parquet`: one row per notification recipient
with a `user_clicked` flag — the richest table the final reports (Step 11)
are built from.
"""
from __future__ import annotations

import logging

import pandas as pd

from .. import config

log = logging.getLogger("pipeline.step09")

FINAL_COLUMNS = [
    "campaign_id", "name", "campaign_utmsource", "department_name",
    "service_name", "campaign_content_id", "campaign_type", "channel_name",
    "final_datetime", "sending_type", "template_name", "template_content",
    "final_date", "final_time", "shorturl_id", "url_key", "user_name",
    "temp_id", "status", "uid", "channel", "transaction_id_y",
]


def run() -> pd.DataFrame:
    notification_log = pd.read_parquet(config.data_path("notification_log.parquet"))
    notification_log = notification_log[["temp_id", "status", "uid", "channel"]]

    notification_temp = pd.read_parquet(config.data_path("notification_temp.parquet"))
    temp_log = notification_log.merge(notification_temp, on="temp_id", how="left")

    campaign = pd.read_parquet(config.data_path("notifications_with_shortlink.parquet"))
    notifications = campaign.merge(temp_log, on="campaign_id", how="left")
    notifications = notifications[FINAL_COLUMNS]
    notifications = notifications.dropna().copy()
    notifications["uid"] = notifications["uid"].astype(int)

    clicks = pd.read_parquet(config.data_path("shortlink_click.parquet"))
    clicks = clicks[["short_url_id", "uid", "date", "time"]].rename(
        columns={"date": "click_date", "time": "click_time", "short_url_id": "shorturl_id"}
    )

    notifications_click = notifications.merge(clicks, on=["shorturl_id", "uid"], how="left")
    notifications_click["user_clicked"] = notifications_click["click_date"].notna().astype(int)

    out_path = config.data_path("notifications_click.parquet")
    notifications_click.to_parquet(out_path, index=False)
    log.info("Wrote %s (%s rows)", out_path, len(notifications_click))
    return notifications_click
