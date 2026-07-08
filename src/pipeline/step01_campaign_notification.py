"""
Step 1 — Campaign & Notification base data (was step_1_initial_work.ipynb)

Pulls campaign metadata from `crm-db`, message templates from
`crm-db@replica`, joins them, and writes `notification.parquet` — the
base table every later step builds on.

The date window is fully dynamic: it pulls Jan 1st of TARGET_YEAR through
the end of TARGET_MONTH (see src/config.py), instead of the notebook's
hardcoded `> "2025-12-31" and < "2026-07-01"`.
"""
from __future__ import annotations

import logging

import pandas as pd

from .. import config, db

log = logging.getLogger("pipeline.step01")

CAMPAIGN_SQL = """
SELECT camp.id as campaign_id, camp.campaign_utmsource, camp.name,
       dp.name as department_name, ss.name as service_name,
       camp.campaign_content_id, ct.name as campaign_type,
       cht.channel_code as channel_name, camp.final_datetime,
       camp.schedule_date_time, camp.final_request_id as transaction_id,
       CASE
           WHEN campaign_sending_type = 1 THEN 'schedule'
           WHEN campaign_sending_type = 0 THEN 'instant'
           ELSE 'unknown'
       END AS sending_type
FROM `campaigns` as camp
LEFT JOIN services as ss ON camp.campaign_service_id = ss.id
LEFT JOIN departments as dp ON camp.campaign_department_id = dp.id
LEFT JOIN campaigntypes as ct ON camp.campaign_campaigntype_id = ct.id
LEFT JOIN channels as cht ON camp.campaign_channel_id = cht.id
WHERE DATE(camp.final_datetime) > %s AND DATE(camp.final_datetime) < %s
LIMIT %s OFFSET %s
"""

TEMPLATE_SQL = """
-- SMS campaigns
SELECT ca.id as campaign_id, st.name as template_name, st.content as template_content
FROM `campaigns` AS ca
LEFT JOIN `services` AS sa ON ca.campaign_service_id = sa.id
LEFT JOIN sms_templates AS st ON ca.template_id = st.id
WHERE ca.campaign_channel_code LIKE 'sms' AND DATE(ca.final_datetime) > %s AND DATE(ca.final_datetime) < %s

UNION ALL

-- WhatsApp campaigns
SELECT ca.id as campaign_id, st.name as template_name, st.content as template_content
FROM `campaigns` AS ca
LEFT JOIN `services` AS sa ON ca.campaign_service_id = sa.id
LEFT JOIN whatsapp_templates AS st ON ca.template_id = st.id
WHERE ca.campaign_channel_code LIKE 'whatsapp' AND DATE(ca.final_datetime) > %s AND DATE(ca.final_datetime) < %s

UNION ALL

-- Platform campaigns
SELECT ca.id as campaign_id, st.name as template_name, st.content as template_content
FROM `campaigns` AS ca
LEFT JOIN `services` AS sa ON ca.campaign_service_id = sa.id
LEFT JOIN platform_templates AS st ON ca.template_id = st.id
WHERE ca.campaign_channel_code LIKE 'platform' AND DATE(ca.final_datetime) > %s AND DATE(ca.final_datetime) < %s

UNION ALL

-- Email campaigns
SELECT ca.id as campaign_id, st.name as template_name, st.email_subject as template_content
FROM `campaigns` AS ca
LEFT JOIN `services` AS sa ON ca.campaign_service_id = sa.id
LEFT JOIN email_templates AS st ON ca.template_id = st.id
WHERE ca.campaign_channel_code LIKE 'email' AND DATE(ca.final_datetime) > %s AND DATE(ca.final_datetime) < %s

LIMIT %s OFFSET %s
"""

KEEP_COLUMNS = [
    "campaign_id", "name", "campaign_utmsource", "department_name", "service_name",
    "campaign_content_id", "campaign_type", "channel_name", "transaction_id",
    "final_datetime", "sending_type", "template_name", "template_content",
]
STR_COLUMNS = [
    "name", "department_name", "service_name", "campaign_type",
    "channel_name", "sending_type", "template_name", "template_content",
]


def run() -> pd.DataFrame:
    crm_cfg = config.crm_db()
    lower = config.YTD_LOWER_BOUND_EXCLUSIVE.isoformat()
    upper = config.YTD_UPPER_BOUND_EXCLUSIVE.isoformat()
    log.info("Campaign date window (exclusive): %s .. %s", lower, upper)

    campaign = db.fetch_offset_paginated_all(crm_cfg, CAMPAIGN_SQL, params=(lower, upper))
    log.info("Fetched %s campaign rows", len(campaign))

    replica_cfg = config.crm_db_replica()
    templates = db.fetch_offset_paginated_all(
        replica_cfg, TEMPLATE_SQL, params=(lower, upper, lower, upper, lower, upper, lower, upper)
    )
    log.info("Fetched %s template rows", len(templates))

    notification = campaign.merge(templates, on="campaign_id", how="left")
    notification["final_datetime"] = notification["schedule_date_time"].combine_first(
        notification["final_datetime"]
    )
    notification = notification[KEEP_COLUMNS].copy()

    notification["department_name"] = notification["department_name"].fillna("not found")
    notification["service_name"] = notification["service_name"].fillna("not found")
    notification["campaign_content_id"] = notification["campaign_content_id"].fillna(0)

    notification[STR_COLUMNS] = notification[STR_COLUMNS].astype(str)
    notification[["campaign_id", "campaign_content_id"]] = (
        notification[["campaign_id", "campaign_content_id"]]
        .apply(pd.to_numeric, errors="coerce")
        .astype("Int64")
    )
    notification["final_datetime"] = pd.to_datetime(notification["final_datetime"])
    notification["final_date"] = notification["final_datetime"].dt.date
    notification["final_time"] = notification["final_datetime"].dt.time
    notification["campaign_utmsource"] = notification["campaign_utmsource"].str.strip()

    notification = notification[config.in_month_window_mask(notification["final_datetime"])]
    notification.dropna(inplace=True)

    out_path = config.data_path("notification.parquet")
    notification.to_parquet(out_path, index=False)
    log.info("Wrote %s (%s rows)", out_path, len(notification))
    return notification
