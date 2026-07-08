#!/usr/bin/env python3
"""
CLIRNET Content Analysis BI — full pipeline runner.

Runs every step in order (campaign data -> notification logs -> content
data -> physician data -> click events -> joins -> final reports), then
emails the 3 deliverables. Every date/month value comes from `src.config`;
set the TARGET_MONTH env var (format "YYYY-MM") to backfill a specific
month — otherwise it defaults to last month.

Usage:
    python run_pipeline.py                     # full run + email
    python run_pipeline.py --no-email           # generate files only
    python run_pipeline.py --only step05        # re-run a single step
    python run_pipeline.py --from-step step06   # resume from a step
    TARGET_MONTH=2026-03 python run_pipeline.py # backfill March 2026
"""
from __future__ import annotations

import argparse
import logging
import sys
import time

from src import config
from src.email_utils import send_report_email
from src.pipeline import (
    step01_campaign_notification as step01,
    step02_shortlink_dictionary as step02,
    step03_notification_temp as step03,
    step04_notification_logs as step04,
    step05_content_data as step05,
    step06_user_data as step06,
    step07_shortlink_clicks as step07,
    step08_consolidate as step08,
    step09_join_clicks as step09,
    step10_speciality_summary as step10,
    step11_final_reports as step11,
)

STEPS = [
    ("step01", "Campaign & notification base data", step01.run),
    ("step02", "Shortlink dictionary", step02.run),
    ("step03", "Notification temp IDs", step03.run),
    ("step04", "Notification delivery logs", step04.run),
    ("step05", "Content data (all 7 content types)", step05.run),
    ("step06", "Physician master data (user_data_index)", step06.run),
    ("step07", "Shortlink click events", step07.run),
    ("step08", "Consolidate", step08.run),
    ("step09", "Join notifications + clicks", step09.run),
    ("step10", "Speciality summary (supplementary, not emailed)", step10.run),
    ("step11", "Final reports (emailed deliverables)", step11.run),
]


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--no-email", action="store_true", help="Generate files but skip sending the email")
    parser.add_argument("--only", help="Run only one step, e.g. --only step05")
    parser.add_argument("--from-step", help="Resume the pipeline starting at this step, e.g. --from-step step06")
    args = parser.parse_args()

    configure_logging()
    log = logging.getLogger("pipeline")
    log.info("=" * 72)
    log.info("CLIRNET Content Analysis BI pipeline")
    log.info(
        "Reporting month: %s  (TARGET_MONTH=%s-%02d)",
        config.TARGET_MONTH_LABEL, config.TARGET_YEAR, config.TARGET_MONTH_NUM,
    )
    log.info("Output directory: %s", config.DATA_DIR.resolve())
    log.info("=" * 72)

    steps_to_run = STEPS
    ran_partial = False

    if args.only:
        steps_to_run = [s for s in STEPS if s[0] == args.only]
        if not steps_to_run:
            parser.error(f"Unknown step {args.only!r}. Valid: {[s[0] for s in STEPS]}")
        ran_partial = True
    elif args.from_step:
        names = [s[0] for s in STEPS]
        if args.from_step not in names:
            parser.error(f"Unknown step {args.from_step!r}. Valid: {names}")
        steps_to_run = STEPS[names.index(args.from_step):]
        ran_partial = steps_to_run[-1][0] != "step11"

    for name, description, fn in steps_to_run:
        log.info("--- %s: %s ---", name, description)
        t0 = time.time()
        fn()
        log.info("--- %s complete in %.1fs ---", name, time.time() - t0)

    if args.no_email:
        log.info("Skipping email (--no-email).")
        return

    if ran_partial:
        log.info("Ran a partial pipeline that didn't end at step11 — skipping email.")
        return

    send_report_email()


if __name__ == "__main__":
    main()
