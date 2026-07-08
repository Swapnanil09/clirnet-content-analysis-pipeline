"""
Step 10 — Speciality click-rate summary (was Step_A3_-_Analysis.ipynb)

A supplementary analysis: click-through rate by campaign / speciality /
service for doctors & medical students. Kept for parity with the original
11-notebook pipeline; this is NOT one of the 3 files that get emailed out
every month — see Step 11 for those.
"""
from __future__ import annotations

import logging

import pandas as pd

from .. import config, db

log = logging.getLogger("pipeline.step10")

USER_TYPE_SQL = """
    SELECT um.user_master_id AS uid, mut.type_name AS usertype
    FROM user_master AS um
    LEFT JOIN master_user_type AS mut ON um.master_user_type_id = mut.user_type_id
    WHERE um.user_master_id > %s
    ORDER BY um.user_master_id
    LIMIT %s
"""


def _fetch_user_types() -> pd.DataFrame:
    cfg = config.clirnet_db()
    return db.fetch_pk_paginated(cfg, USER_TYPE_SQL, id_col_alias="uid", batch_size=50000)


def run() -> pd.DataFrame:
    notifications_click = pd.read_parquet(config.data_path("notifications_click.parquet"))

    user_ty = _fetch_user_types()[["uid", "usertype"]]
    user_data = pd.read_parquet(config.data_path("user_data.parquet"))
    user_data = user_data.rename(columns={"user_master_id": "uid"})
    user_data = user_data.merge(user_ty, on="uid", how="left")[["uid", "Speciality", "usertype"]]

    target = notifications_click[
        (notifications_click["final_datetime"].dt.year == config.TARGET_YEAR)
        & (notifications_click["final_datetime"].dt.month == config.TARGET_MONTH_NUM)
    ]
    target = target[target["channel_name"].isin(["whatsapp", "sms"])]

    data = target.merge(user_data, on="uid", how="left")
    data = data[data["usertype"].isin(["doctor", "Medical Student"])]

    result = (
        data.groupby(["campaign_id", "Speciality", "service_name", "campaign_content_id", "usertype"])
        .agg(
            uid_count=("uid", "nunique"),
            user_clicked_count=("user_clicked", "sum"),
        )
        .reset_index()
    )
    result["click_rate"] = round((result["user_clicked_count"] / result["uid_count"]) * 100, 2)
    result = result.sort_values(by="click_rate", ascending=False)

    out_path = config.data_path(f"data_month_{config.MONTH_NAME}.xlsx")
    result.to_excel(out_path, index=False)
    log.info("Wrote %s (%s rows) — supplementary, not emailed", out_path, len(result))
    return result
