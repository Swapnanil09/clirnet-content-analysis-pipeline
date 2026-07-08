"""
Step 11 — Final deliverables (was Step_A4_-_Analysis-2.ipynb)

Produces the two CSV reports that get emailed out every month:
  - Speciality_month_diff_campaign_<Month>.csv
  - Content_template_analysis_<Month>.csv

Filenames and the "Date" column inside them are fully dynamic (derived
from TARGET_MONTH), replacing the notebook's hardcoded `dt.year == 2026`,
`dt.month == 6`, `"01-06-2026"`, and `_JUNE.csv` suffix.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from .. import config, db

log = logging.getLogger("pipeline.step11")

USER_TYPE_SQL = """
    SELECT um.user_master_id AS uid, mut.type_name AS usertype
    FROM user_master AS um
    LEFT JOIN master_user_type AS mut ON um.master_user_type_id = mut.user_type_id
    WHERE um.user_master_id > %s
    ORDER BY um.user_master_id
    LIMIT %s
"""

SERVICE_NAMES_IN_SCOPE = [
    "Training", "MedWiki", "E-Paper", "Clinical Videos",
    "Digital CME", "Grand Rounds", "SPQs",
]
CONTENT_TYPE_RENAME = {
    "E-Pub": "E-Paper", "Clinical_Video": "Clinical Videos",
    "SPQs": "SPQS", "Grand Round": "Grand Rounds",
}
CONTENT_TYPES_IN_SCOPE = [
    "Training", "MedWiki", "E-Pub", "Clinical_Video", "SPQs", "Grand Round", "Digital CME",
]


def _fetch_user_types() -> pd.DataFrame:
    cfg = config.clirnet_db()
    return db.fetch_pk_paginated(cfg, USER_TYPE_SQL, id_col_alias="uid", batch_size=50000)


def run() -> None:
    notifications_click = pd.read_parquet(config.data_path("notifications_click.parquet"))

    user_ty = _fetch_user_types()[["uid", "usertype"]]
    user_data = pd.read_parquet(config.data_path("user_data.parquet"))
    user_data = user_data.rename(columns={"user_master_id": "uid"})
    user_data = user_data.merge(user_ty, on="uid", how="left")[["uid", "Speciality", "usertype", "State"]]

    log.info("Filtering to target month: %s-%02d", config.TARGET_YEAR, config.TARGET_MONTH_NUM)
    sms_whatsapp = notifications_click[notifications_click["channel_name"].isin(["whatsapp", "sms"])]
    sms_whatsapp = sms_whatsapp[sms_whatsapp["service_name"].isin(SERVICE_NAMES_IN_SCOPE)]
    sms_whatsapp = sms_whatsapp[
        (sms_whatsapp["final_datetime"].dt.year == config.TARGET_YEAR)
        & (sms_whatsapp["final_datetime"].dt.month == config.TARGET_MONTH_NUM)
    ]
    sms_whatsapp = sms_whatsapp.loc[sms_whatsapp["campaign_content_id"] != 0]

    data = sms_whatsapp.merge(user_data, on="uid", how="left")

    content_data = pd.read_parquet(config.data_path("content_data.parquet"))
    content_data = content_data[content_data["type"].isin(CONTENT_TYPES_IN_SCOPE)].copy()
    content_data["type"] = content_data["type"].replace(CONTENT_TYPE_RENAME)
    content_data = content_data.rename(
        columns={"type": "service_name", "type_id": "campaign_content_id"}
    )[["service_name", "campaign_content_id", "publication_date"]]

    final_data = data.merge(content_data, on=["service_name", "campaign_content_id"], how="left")

    # --- Report 1: Speciality x months-since-publication breakdown ---
    final_data["final_datetime"] = pd.to_datetime(final_data["final_datetime"], errors="coerce")
    final_data["publication_date"] = pd.to_datetime(final_data["publication_date"], errors="coerce")
    final_data = final_data.dropna(subset=["final_datetime", "publication_date"])

    final_data["month_diff"] = (
        (final_data["final_datetime"].dt.year - final_data["publication_date"].dt.year) * 12
        + (final_data["final_datetime"].dt.month - final_data["publication_date"].dt.month)
    )
    final_data["month_diff"] = pd.cut(
        final_data["month_diff"], bins=[-np.inf, 1, 2, 3, np.inf], labels=["1", "2", "3", "3+"]
    )

    result1 = (
        final_data.groupby(["service_name", "campaign_content_id", "Speciality", "month_diff"])
        .agg(
            count_uid=("uid", "count"),
            distinct_uid=("uid", "nunique"),
            count_user_clicked=("user_clicked", "sum"),
            unique_campaign_id=("campaign_id", "nunique"),
        )
        .reset_index()
    )
    result1["Date"] = config.TARGET_MONTH_DATE_DDMMYYYY

    result1 = result1[
        ~(
            (result1["count_uid"] == 0)
            & (result1["distinct_uid"] == 0)
            & (result1["count_user_clicked"] == 0)
            & (result1["unique_campaign_id"] == 0)
        )
    ]
    result1.to_csv(config.OUT_SPECIALITY_MONTH_DIFF, index=False)
    log.info("Wrote %s (%s rows)", config.OUT_SPECIALITY_MONTH_DIFF, len(result1))

    # --- Report 2: Content-template engagement breakdown ---
    result2 = (
        final_data.groupby(["service_name", "campaign_content_id", "channel_name", "template_content"])
        .agg(
            count_uid=("uid", "count"),
            distinct_uid=("uid", "nunique"),
            count_user_clicked=("user_clicked", "sum"),
            unique_campaign_id=("campaign_id", "nunique"),
        )
        .reset_index()
    )
    result2["Date"] = config.TARGET_MONTH_DATE_DDMMYYYY
    result2.to_csv(config.OUT_CONTENT_TEMPLATE_ANALYSIS, index=False)
    log.info("Wrote %s (%s rows)", config.OUT_CONTENT_TEMPLATE_ANALYSIS, len(result2))
