"""
Shared database helpers: connection builders, retrying paginated fetchers,
and a streaming Parquet writer so very large tables (e.g. `user_data_index`,
which can hold millions of physician records) never need to be held fully
in memory as one giant DataFrame — the pattern the original notebooks used
and the main scaling risk as CLIRNET's data grows.

Every function here takes credentials as an explicit `config.MySQLConfig` /
reads `config.clickhouse_cfg()` — nothing is hardcoded.
"""
from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Callable, Iterable, Iterator, Sequence

import clickhouse_connect
import mysql.connector
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pymysql

from . import config

log = logging.getLogger("pipeline.db")

RETRYABLE_SLEEP_SECONDS = 2
MAX_RETRIES = 5


# ---------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------
def pymysql_connect(cfg: config.MySQLConfig):
    return pymysql.connect(
        host=cfg.host,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        ssl={"ca": config.SSL_CA_PATH},
    )


def mysql_connector_connect(cfg: config.MySQLConfig, autocommit: bool = True):
    return mysql.connector.connect(
        host=cfg.host,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        autocommit=autocommit,
    )


def clickhouse_client():
    cfg = config.clickhouse_cfg()
    return clickhouse_connect.get_client(
        host=cfg.host, user=cfg.user, password=cfg.password, secure=cfg.secure
    )


# ---------------------------------------------------------------------------
# Retry wrapper for transient connection blips
# ---------------------------------------------------------------------------
def with_retry(fn: Callable, *, what: str, max_retries: int = MAX_RETRIES):
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            return fn()
        except (pymysql.err.OperationalError, mysql.connector.Error) as e:
            last_exc = e
            if attempt == max_retries:
                raise
            log.warning("%s: transient error (%s) — retry %s/%s", what, e, attempt, max_retries)
            time.sleep(RETRYABLE_SLEEP_SECONDS * attempt)
    raise last_exc  # pragma: no cover


# ---------------------------------------------------------------------------
# Offset-paginated fetch (LIMIT/OFFSET style — Step 1, 2, 6)
# ---------------------------------------------------------------------------
def fetch_offset_paginated(
    cfg: config.MySQLConfig,
    sql_template: str,
    params: Sequence = (),
    page_size: int | None = None,
) -> Iterator[pd.DataFrame]:
    """
    Yields one DataFrame per page for a `... LIMIT %s OFFSET %s` query.
    `sql_template` must end in `LIMIT %s OFFSET %s`; `params` are any extra
    bind params that appear BEFORE the limit/offset placeholders.
    """
    page_size = page_size or config.SQL_PAGE_SIZE
    offset = 0
    conn = pymysql_connect(cfg)
    try:
        while True:
            def _run():
                cur = conn.cursor()
                cur.execute(sql_template, (*params, page_size, offset))
                rows = cur.fetchall()
                cols = [c[0] for c in cur.description]
                cur.close()
                return rows, cols

            rows, cols = with_retry(_run, what="offset page fetch")
            if not rows:
                break
            yield pd.DataFrame(rows, columns=cols)
            if len(rows) < page_size:
                break
            offset += page_size
    finally:
        conn.close()


def fetch_offset_paginated_all(
    cfg: config.MySQLConfig, sql_template: str, params: Sequence = (), page_size: int | None = None
) -> pd.DataFrame:
    """Concatenates every page into one DataFrame — fine for small/medium tables."""
    frames = list(fetch_offset_paginated(cfg, sql_template, params, page_size))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def stream_offset_paginated_to_parquet(
    cfg: config.MySQLConfig,
    sql_template: str,
    out_path: Path,
    params: Sequence = (),
    page_size: int | None = None,
    post_process: Callable[[pd.DataFrame], pd.DataFrame] | None = None,
) -> int:
    """
    Streams a LIMIT/OFFSET query straight to a Parquet file, one page at a
    time, WITHOUT ever holding the full table in memory. This is the
    pattern used for `user_data_index`, which can be several million rows —
    the original notebook accumulated every row in a Python list before
    building one giant DataFrame, which risks running out of memory as the
    table keeps growing. Returns the total row count written.
    """
    writer = None
    total = 0
    try:
        for chunk in fetch_offset_paginated(cfg, sql_template, params, page_size):
            if post_process is not None:
                chunk = post_process(chunk)
            table = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(str(out_path), table.schema)
            writer.write_table(table)
            total += len(chunk)
            log.info("  %s: %s rows written so far", out_path.name, total)
    finally:
        if writer is not None:
            writer.close()
    return total


# ---------------------------------------------------------------------------
# Primary-key paginated fetch (id > last_id, reconnect-on-error) — Step 5, 10, 11
# ---------------------------------------------------------------------------
def fetch_pk_paginated(
    cfg: config.MySQLConfig,
    sql_template: str,
    id_col_alias: str,
    batch_size: int | None = None,
    autocommit: bool = True,
) -> pd.DataFrame:
    """
    Pages through `sql_template` (which must select `... AS {id_col_alias}`
    and filter `WHERE <pk> > %s ... LIMIT %s`) by primary key rather than
    OFFSET — much faster on huge tables since the database doesn't have to
    skip N rows on every page. Reconnects and retries on transient errors,
    matching the resilience already present in the original content-data
    notebook (extended here to every step that needs it).
    """
    batch_size = batch_size or config.SQL_PAGE_SIZE
    frames: list[pd.DataFrame] = []
    last_id = 0
    conn = mysql_connector_connect(cfg, autocommit=autocommit)
    try:
        while True:
            try:
                cur = conn.cursor(buffered=True)
                cur.execute(sql_template, (last_id, batch_size))
                rows = cur.fetchall()
                cols = [c[0] for c in cur.description]
                cur.close()
            except mysql.connector.Error as e:
                log.warning("pk-paginated fetch blip at id %s: %s — reconnecting", last_id, e)
                try:
                    conn.close()
                except Exception:
                    pass
                time.sleep(RETRYABLE_SLEEP_SECONDS)
                conn = mysql_connector_connect(cfg, autocommit=autocommit)
                continue
            if not rows:
                break
            df = pd.DataFrame(rows, columns=cols)
            frames.append(df)
            last_id = int(df[id_col_alias].iloc[-1])
            log.info("  %s rows so far (id <= %s)", sum(len(f) for f in frames), last_id)
            if len(rows) < batch_size:
                break
    finally:
        conn.close()
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ---------------------------------------------------------------------------
# ID-batched IN(...) fetch — Step 3, 4, 5, 7
# ---------------------------------------------------------------------------
def chunk_ids(ids: Iterable, batch_size: int) -> Iterator[list]:
    ids = list(ids)
    for i in range(0, len(ids), batch_size):
        yield ids[i : i + batch_size]


def fetch_id_batches_clickhouse(
    ids: Iterable,
    query_builder: Callable[[str], str],
    batch_size: int | None = None,
    quote: Callable[[object], str] | None = None,
) -> pd.DataFrame:
    """
    query_builder(id_csv_string) -> full SQL query text for one ID batch.
    `quote`, if given, formats each id before joining with commas (e.g.
    `lambda x: f"'{x}'"` for string transaction IDs); defaults to plain str().
    """
    batch_size = batch_size or config.ID_BATCH_SIZE
    quote = quote or (lambda x: str(x))
    client = clickhouse_client()
    frames = []
    for chunk in chunk_ids(ids, batch_size):
        id_list = ",".join(quote(i) for i in chunk)
        result = client.query(query_builder(id_list))
        if result.result_rows:
            frames.append(pd.DataFrame(result.result_rows, columns=result.column_names))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def fetch_id_batches_mysql_paginated(
    cfg: config.MySQLConfig,
    ids: Iterable,
    query_builder: Callable[[str], str],
    batch_size: int | None = None,
    page_size: int | None = None,
    quote: Callable[[object], str] | None = None,
) -> pd.DataFrame:
    """
    query_builder(id_csv_string) -> SQL text ending in `LIMIT %s OFFSET %s`.
    Mirrors the original notebooks' nested pattern: batch the ID list, then
    LIMIT/OFFSET-paginate *within* each batch (defensive in case a single
    batch of IDs still returns more rows than one page can hold).
    """
    batch_size = batch_size or config.ID_BATCH_SIZE
    page_size = page_size or config.SQL_PAGE_SIZE
    quote = quote or (lambda x: str(x))
    conn = pymysql_connect(cfg)
    frames = []
    try:
        for chunk in chunk_ids(ids, batch_size):
            id_list = ",".join(quote(i) for i in chunk)
            sql = query_builder(id_list)
            offset = 0
            while True:
                def _run():
                    cur = conn.cursor()
                    cur.execute(sql, (page_size, offset))
                    rows = cur.fetchall()
                    cols = [c[0] for c in cur.description]
                    cur.close()
                    return rows, cols

                rows, cols = with_retry(_run, what="id-batch page fetch")
                if not rows:
                    break
                frames.append(pd.DataFrame(rows, columns=cols))
                if len(rows) < page_size:
                    break
                offset += page_size
    finally:
        conn.close()
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def fetch_ids_in_batches_mysqlconnector(
    cfg: config.MySQLConfig,
    ids: Iterable,
    sql_builder: Callable[[str], str],
    batch_size: int | None = None,
) -> pd.DataFrame:
    """
    For `IN (%s, %s, ...)`-style bound-parameter queries (safer than string
    interpolation for numeric ID lists). sql_builder(placeholders_str) ->
    SQL text with that many `%s` placeholders.
    """
    batch_size = batch_size or config.SPEC_BATCH_SIZE
    frames = []
    conn = mysql_connector_connect(cfg)
    try:
        ids = [int(x) for x in ids]
        for i in range(0, len(ids), batch_size):
            chunk = ids[i : i + batch_size]
            placeholders = ",".join(["%s"] * len(chunk))
            sql = sql_builder(placeholders)
            while True:
                try:
                    cur = conn.cursor(buffered=True)
                    cur.execute(sql, tuple(chunk))
                    rows = cur.fetchall()
                    cols = [c[0] for c in cur.description]
                    cur.close()
                    break
                except mysql.connector.Error as e:
                    log.warning("id-batch blip near offset %s: %s — reconnecting", i, e)
                    try:
                        conn.close()
                    except Exception:
                        pass
                    time.sleep(RETRYABLE_SLEEP_SECONDS)
                    conn = mysql_connector_connect(cfg)
            if rows:
                frames.append(pd.DataFrame(rows, columns=cols))
    finally:
        conn.close()
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def fetch_plain(cfg: config.MySQLConfig, sql: str) -> pd.DataFrame:
    """One-shot query with no pagination — for small GROUP BY summaries."""
    conn = mysql_connector_connect(cfg)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description]
        cur.close()
        return pd.DataFrame(rows, columns=cols)
    finally:
        conn.close()
