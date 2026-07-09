"""
Step 5 — Content Data (was step_5_content_data.ipynb, 83 cells / ~1,500 lines)

Pulls every content type (MedWiki, Clinical Video, Digital CME, E-Pub,
SPQs, Grand Round, Training) from `clirnetdb`, joins tags + specialities,
adds reach/popularity from ClickHouse, and writes:
  - `content_data.parquet` (feeds Step 11)
  - `All_content_data_Current.xlsx` (one of the 3 emailed deliverables)

The original notebook repeated the same fetch/tag/reach logic seven times
with copy-pasted credentials in every cell. Here it's one loop driven by
`src/content_registry.py` — add a content type there, not here.
"""
from __future__ import annotations

import logging

import pandas as pd

from .. import config, db
from ..content_registry import CONTENT_TYPES, STYLE_PAGED, ContentTypeConfig

log = logging.getLogger("pipeline.step05")


def _fetch_main_paged(cfg: config.MySQLConfig, ct: ContentTypeConfig) -> pd.DataFrame:
    body = f"kc.{ct.body_col} AS description,\n               " if ct.body_col else ""
    sql = f"""
        SELECT kc.{ct.id_col}    AS type_id,
               kc.{ct.title_col} AS title,
               {body}mcc.category_name,
               kc.{ct.date_col}  AS publication_date,
               kc.status
        FROM {ct.main_table} AS kc
        LEFT JOIN {ct.cat_table} AS mcc
               ON kc.{ct.main_cat_col} = mcc.{ct.cat_pk_col}
        WHERE kc.{ct.id_col} > %s
        ORDER BY kc.{ct.id_col}
        LIMIT %s
    """
    return db.fetch_pk_paginated(cfg, sql, id_col_alias="type_id", batch_size=10000)


def _fetch_specialities(cfg: config.MySQLConfig, ct: ContentTypeConfig, ids) -> pd.DataFrame:
    bridge_col = ct.resolved_bridge_content_col()

    def build(placeholders: str) -> str:
        return (
            f"SELECT cs.{bridge_col} AS type_id, ms.specialities_name "
            f"FROM {ct.bridge_table} AS cs "
            f"JOIN master_specialities_v1 AS ms "
            f"  ON cs.{ct.bridge_spec_col} = ms.master_specialities_id "
            f"WHERE cs.{bridge_col} IN ({placeholders})"
        )

    return db.fetch_ids_in_batches_mysqlconnector(cfg, ids, build)


def _fetch_tags(cfg: config.MySQLConfig, ct: ContentTypeConfig) -> pd.DataFrame:
    sql = f"""
        SELECT
            kc.{ct.id_col} AS type_id,
            GROUP_CONCAT(CASE WHEN ctt.tag_status = 1 THEN mm.name END SEPARATOR ", ") AS primary_tag,
            GROUP_CONCAT(CASE WHEN ctt.tag_status = 2 THEN mm.name END SEPARATOR ", ") AS secondary_tag
        FROM `{ct.main_table}` AS kc
        LEFT JOIN content_to_mesh_with_tag_status AS ctt ON kc.{ct.id_col} = ctt.content_id
        LEFT JOIN master_mesh AS mm ON ctt.mesh_id = mm.mesh_id
        WHERE ctt.content_type = {ct.tag_content_type}
        GROUP BY kc.{ct.id_col}
    """
    return db.fetch_plain(cfg, sql)


def _fetch_simple(cfg: config.MySQLConfig, ct: ContentTypeConfig) -> pd.DataFrame:
    bridge_col = ct.resolved_bridge_content_col()
    sql = f"""
        SELECT
            "{ct.type_label}" AS type,
            kc.{ct.title_col} AS title,
            kc.{ct.id_col} AS type_id,
            {ct.category_expr} as category_name,
            kc.{ct.date_col} as publication_date,
            kc.status,
            GROUP_CONCAT(DISTINCT ms.specialities_name SEPARATOR ", ") AS specialities
        FROM {ct.main_table} AS kc
        LEFT JOIN {ct.bridge_table} AS cs ON kc.{ct.id_col} = cs.{bridge_col}
        LEFT JOIN master_specialities_v1 AS ms ON cs.{ct.bridge_spec_col} = ms.master_specialities_id
        GROUP BY kc.{ct.id_col};
    """
    return db.fetch_plain(cfg, sql)


def _fetch_reach(ct: ContentTypeConfig, ids) -> pd.DataFrame:
    ids = list(ids)
    if not ids:
        return pd.DataFrame(columns=["type_id", "reach", "popularity"])

    def build(id_csv: str) -> str:
        return f"""
            SELECT activity_page_id AS type_id,
                   count(DISTINCT user_data_id) AS reach,
                   count(user_data_id) AS popularity
            FROM prod_user_activities
            WHERE activity_service_type_id = {ct.activity_service_type_id}
              AND user_data_id != 0
              AND activity_page_id IN ({id_csv})
            GROUP BY activity_page_id
        """

    result = db.fetch_id_batches_clickhouse(ids, build)
    return result if not result.empty else pd.DataFrame(columns=["type_id", "reach", "popularity"])


def _fetch_one_content_type(cfg: config.MySQLConfig, ct: ContentTypeConfig) -> pd.DataFrame:
    log.info("Fetching content type: %s (style=%s)", ct.type_label, ct.style)

    if ct.style == STYLE_PAGED:
        main_df = _fetch_main_paged(cfg, ct)
        spec_df = _fetch_specialities(cfg, ct, main_df["type_id"].unique())
        if not spec_df.empty:
            spec_agg = (
                spec_df.groupby("type_id")["specialities_name"]
                .apply(lambda s: ", ".join(sorted(set(s.dropna()))))
                .reset_index(name="specialities")
            )
        else:
            spec_agg = pd.DataFrame(columns=["type_id", "specialities"])
        details = main_df.merge(spec_agg, on="type_id", how="left")
        details["type"] = ct.type_label
    else:
        details = _fetch_simple(cfg, ct)

    tags = _fetch_tags(cfg, ct)
    merged = details.merge(tags, on="type_id", how="left")

    reach = _fetch_reach(ct, merged["type_id"].unique())
    merged = merged.merge(reach, on="type_id", how="left")

    log.info("  %s: %s rows", ct.type_label, len(merged))
    return merged


def run() -> pd.DataFrame:
    cfg = config.clirnet_db()
    frames = [_fetch_one_content_type(cfg, ct) for ct in CONTENT_TYPES]
    final_content = pd.concat(frames, ignore_index=True)

    columns_to_fill = ["type", "title", "type_id", "category_name",
                        "publication_date", "specialities", "primary_tag", "secondary_tag"]
    columns_to_fill_numeric = ["reach", "popularity"]

    final_content[columns_to_fill] = final_content[columns_to_fill].fillna("not found")
    final_content[columns_to_fill_numeric] = final_content[columns_to_fill_numeric].fillna(0)
    final_content["reach"] = final_content["reach"].astype(int)
    final_content["popularity"] = final_content["popularity"].astype(int)

    if "description" in final_content.columns:
        final_content["description"] = final_content["description"].fillna("").astype(str).str.slice(0, 200)

    final_content.to_csv(config.OUT_ALL_CONTENT_DATA, index=False)
    log.info("Wrote %s (%s rows)", config.OUT_ALL_CONTENT_DATA, len(final_content))

    # Parquet copy for the join steps downstream (cheaper to re-read than xlsx)
    for_parquet = final_content.copy()
    for_parquet["publication_date"] = for_parquet["publication_date"].astype(str)
    parquet_path = config.data_path("content_data.parquet")
    for_parquet.to_parquet(parquet_path, index=False)
    log.info("Wrote %s (%s rows)", parquet_path, len(for_parquet))

    return final_content
