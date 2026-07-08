"""
Step 6 — Physician master data (was step_6_finding_user_data.ipynb)

This is the LARGEST table in the pipeline — the full CLIRNET physician
database (`user_data_index`), which can run into the millions of rows.

The original notebook accumulated every row in a Python list before
building one giant DataFrame, which risks running out of memory as the
table keeps growing. This version streams each page straight to Parquet
via `db.stream_offset_paginated_to_parquet`, so peak memory is bounded by
one page (SQL_PAGE_SIZE rows), not the whole table.
"""
from __future__ import annotations

import logging

import pandas as pd

from .. import config, db

log = logging.getLogger("pipeline.step06")

USER_DATA_SQL = """
SELECT user_data_index.uid as user_master_id, user_data_index.first_name,
       user_data_index.last_name, usertypes.name as user_type,
       user_data_index.crmstatus, degrees.name as drgree_name,
       user_data_index.gender as Sex, zones.name as Zone,
       specialties.name Speciality, cities.name as City, states.name as State,
       countries.name as Country, councils.name as medical_council
FROM user_data_index
Left join zones ON user_data_index.zone = zones.id
Left join specialties ON user_data_index.primary_specialty = specialties.id
Left join cities ON user_data_index.city = cities.id
Left join states ON user_data_index.state = states.id
Left join countries ON user_data_index.country = countries.id
Left join councils ON user_data_index.medical_council = councils.id
Left Join usertypes ON user_data_index.user_type_id = usertypes.master_id
Left Join degrees ON user_data_index.degree = degrees.id
WHERE user_data_index.uid > %s
ORDER BY user_data_index.uid
LIMIT %s
"""

CRM_STATUS_MAP = {
    "0": "New / Pending", "1": "Assigned", "2": "in-Progress",
    "3": "Onboarded", "4": "Approved", "5": "Disapproved",
}


def _post_process(chunk: pd.DataFrame) -> pd.DataFrame:
    chunk = chunk.copy()
    chunk["crmstatus"] = chunk["crmstatus"].astype(str).replace(CRM_STATUS_MAP)
    return chunk


def run() -> None:
    cfg = config.crm_db()
    out_path = config.data_path("user_data.parquet")
    log.info("Streaming user_data_index in %s-row pages -> %s", config.SQL_PAGE_SIZE, out_path)
    total = db.stream_pk_paginated_to_parquet(
        cfg, USER_DATA_SQL, id_col_alias="user_master_id", out_path=out_path, post_process=_post_process, batch_size=50000
    )
    log.info("Wrote %s (%s rows total, streamed — never held fully in memory)", out_path, total)
