"""
Registry describing every content type joined into `all_content_data`.

Replaces ~1,500 lines of copy-pasted notebook cells (one block per content
type, each with its own hardcoded DB credentials) with a single data-driven
table. To add a new content type, add one entry here — nothing else in
step05 needs to change.

Two query "shapes" are used, matching the two patterns in the original
notebook exactly (so results are unchanged, just de-duplicated):

  STYLE_PAGED  — main table is paged by primary key (id_col); specialities
                 are fetched separately in ID-batches and aggregated
                 locally. Used for the three largest tables: MedWiki,
                 Clinical Video, Digital CME.

  STYLE_SIMPLE — one single GROUP_CONCAT query per content type brings
                 specialities back inline. Used for the smaller tables:
                 E-Pub, SPQs, Grand Round, Training.
"""
from __future__ import annotations

from dataclasses import dataclass

STYLE_PAGED = "paged"
STYLE_SIMPLE = "simple"


@dataclass(frozen=True)
class ContentTypeConfig:
    key: str                        # short key, e.g. "medwiki"
    type_label: str                 # value written into the `type` column
    main_table: str
    id_col: str                     # primary key column name on main_table
    title_col: str
    date_col: str
    activity_service_type_id: int   # prod_user_activities.activity_service_type_id
    tag_content_type: int           # content_to_mesh_with_tag_status.content_type
    style: str = STYLE_PAGED

    # STYLE_PAGED only — category comes from a joined lookup table
    cat_table: str | None = None
    main_cat_col: str | None = None
    cat_pk_col: str | None = None
    body_col: str | None = None            # only MedWiki has a description column
    bridge_table: str | None = None        # "<content>_to_specialities"
    bridge_content_col: str | None = None  # defaults to id_col if not set
    bridge_spec_col: str = "specialities_id"  # SPQs uses "speciality_id" (singular)

    # STYLE_SIMPLE only — category is a literal column expression or fixed string
    category_expr: str | None = None       # e.g. "kc.author", "'none'"

    def resolved_bridge_content_col(self) -> str:
        return self.bridge_content_col or self.id_col


CONTENT_TYPES: list[ContentTypeConfig] = [
    ContentTypeConfig(
        key="medwiki", type_label="MedWiki",
        main_table="knwlg_compendium_v1", id_col="comp_qa_id",
        title_col="comp_qa_question_raw", body_col="comp_qa_answer_raw",
        date_col="publication_date",
        cat_table="master_compendium_category", main_cat_col="comp_qa_cat_id",
        cat_pk_col="master_comp_cat_id",
        bridge_table="compendium_to_specialities",
        tag_content_type=1, activity_service_type_id=2,
    ),
    ContentTypeConfig(
        key="video", type_label="Clinical_Video",
        main_table="knwlg_video_archive", id_col="video_archive_id",
        title_col="video_archive_question_raw",
        date_col="publication_date",
        cat_table="master_videoarchive_category", main_cat_col="video_archive_cat_id",
        cat_pk_col="master_video_archive_cat_id",
        bridge_table="video_archive_to_specialities",
        tag_content_type=2, activity_service_type_id=11,
    ),
    ContentTypeConfig(
        key="cme", type_label="Digital CME",
        main_table="knwlg_sessions_v1", id_col="session_id",
        title_col="session_topic",
        date_col="added_on",
        cat_table="master_session_category", main_cat_col="category_id",
        cat_pk_col="mastersession_category_id",
        bridge_table="session_to_specialities",
        tag_content_type=3, activity_service_type_id=3,
    ),
    ContentTypeConfig(
        key="epub", type_label="E-Pub", style=STYLE_SIMPLE,
        main_table="epub_master", id_col="epub_id",
        title_col="epub_title", date_col="publication_date",
        category_expr="kc.author",
        bridge_table="epub_to_specialities",
        tag_content_type=6, activity_service_type_id=13,
    ),
    ContentTypeConfig(
        key="survey", type_label="SPQs", style=STYLE_SIMPLE,
        main_table="survey", id_col="survey_id",
        title_col="survey_title", date_col="added_on",
        category_expr="kc.category",
        bridge_table="survey_to_speciality",
        bridge_spec_col="speciality_id",
        tag_content_type=5, activity_service_type_id=7,
    ),
    ContentTypeConfig(
        key="gr", type_label="Grand Round", style=STYLE_SIMPLE,
        main_table="knwlg_gr_register", id_col="gr_id",
        title_col="gr_title", date_col="gr_date_of_publication",
        category_expr="kc.gr_type",
        bridge_table="gr_to_specialities",
        tag_content_type=4, activity_service_type_id=6,
    ),
    ContentTypeConfig(
        key="training", type_label="Training", style=STYLE_SIMPLE,
        main_table="training_master", id_col="id",
        title_col="title", date_col="published_date",
        category_expr="'none'",
        bridge_table="training_to_speciality",
        bridge_content_col="training_id",
        tag_content_type=7, activity_service_type_id=15,
    ),
]
