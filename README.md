# CLIRNET Content Analysis BI Pipeline

Automated monthly pipeline that rebuilds CLIRNET's Content Analysis BI dataset
and emails 3 reports to the team. This replaces an 11-notebook manual
workflow (`step_1` → `step_7` → `Step_A1` → `Step_A4`) that had to be
opened, edited by hand for the new month/year, and run cell-by-cell every
month.

## What changed vs. the original notebooks

| Problem in the original notebooks | Fixed here |
|---|---|
| Hardcoded dates in SQL (`> "2025-12-31"`, `< "2026-07-01"`) and pandas filters (`dt.month == 6`, `dt.year == 2026`) had to be hand-edited every month | Everything derives from one `TARGET_MONTH` value (see [Which month gets reported](#which-month-gets-reported)) |
| Output filenames hardcoded with the month baked in (`speciality_month_diff_campign_JUNE.csv`) | Filenames are generated from `TARGET_MONTH` automatically |
| Every database password (5 different databases) and the email app password were stored in **plaintext inside the `.ipynb` files** | Moved to environment variables / GitHub Actions secrets — nothing is hardcoded anywhere in this repo |
| `step_5_content_data.ipynb` repeated the same fetch/tag/reach logic 7 times (once per content type, ~1,500 lines) | One data-driven loop over `src/content_registry.py` (~150 lines total) |
| `user_data_index` (CLIRNET's full physician table — potentially millions of rows) was pulled entirely into memory as one Python list before being turned into a DataFrame | Streamed straight to Parquet page-by-page (`db.stream_offset_paginated_to_parquet`) — memory usage stays flat no matter how large the table grows |
| Had to be run manually, notebook by notebook | One command (`python run_pipeline.py`), and a GitHub Actions workflow that runs it automatically every month |

## Pipeline stages

```
step01  Campaign & notification base data      (crm-db)
step02  Shortlink dictionary                    (shortlink-db)
step03  Notification "temp" IDs                 (ClickHouse -> notification-shortlink-db fallback)
step04  Notification delivery logs              (ClickHouse -> notification-shortlink-db fallback)
step05  Content data — all 7 content types      (clirnetdb + ClickHouse)   -> All_content_data_Current.xlsx
step06  Physician master data (user_data_index) (crm-db, streamed)
step07  Shortlink click events                  (ClickHouse)
step08  Consolidate
step09  Join notifications + delivery + clicks
step10  Speciality click-rate summary           (supplementary — not emailed)
step11  Final reports                           -> Speciality_month_diff_campaign_<Month>.csv
                                                 -> Content_template_analysis_<Month>.csv
        ↓
  email  Sends the 3 deliverables to the team
```

Each stage is `src/pipeline/stepNN_*.py`; `run_pipeline.py` runs them in
order and then calls `src/email_utils.send_report_email()`.

## Prerequisites

- Python 3.11+
- Network access to: PlanetScale (`aws.connect.psdb.cloud`), ClickHouse
  Cloud, and `smtp.office365.com` — all reachable directly over the
  internet, no VPN needed (same as the original notebooks).
- Credentials for 4 PlanetScale databases, 1 ClickHouse instance, and the
  sending email account (see `.env.example` for the full list).

## Local setup

```bash
git clone <your-repo-url> clirnet-content-analysis-pipeline
cd clirnet-content-analysis-pipeline

python3 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

pip install -r requirements.txt

cp .env.example .env
# now edit .env and fill in every credential (see .env.example comments)

# Load .env into your shell, then run:
export $(grep -v '^#' .env | xargs)   # or use a tool like `direnv` / `python-dotenv`
python run_pipeline.py --no-email     # generate files without emailing, to test
```

Generated files land in `data/` (git-ignored). Once you're happy with the
output, drop `--no-email` to also send the report:

```bash
python run_pipeline.py
```

### Useful flags

```bash
python run_pipeline.py --only step05        # re-run just one step (e.g. after fixing a query)
python run_pipeline.py --from-step step06   # resume a failed run from step06 onward
TARGET_MONTH=2026-03 python run_pipeline.py # backfill a specific month
```

## Which month gets reported

Set once, in `src/config.py`, and used everywhere else in the pipeline:

- If the `TARGET_MONTH` environment variable is set (format `"YYYY-MM"`,
  e.g. `"2026-06"`), that month is used — for backfills or re-runs.
- Otherwise it defaults to **last calendar month** relative to when the
  pipeline runs. The scheduled GitHub Actions job runs on the 2nd of each
  month, so by default it reports on the month that just closed.

Everything else is derived from that one value: the SQL date-range filters
in Step 1/2, the `dt.year == ... & dt.month == ...` filters in Step 10/11,
the `"01-06-2026"`-style `Date` column, and every output filename.

> **One inherited assumption worth knowing about:** the original Step 1
> notebook always excluded January from its year-to-date pull
> (`dt.month > 1`), even though the reasoning for that wasn't documented
> anywhere in the notebook. This pipeline preserves that behaviour
> (`config.EXCLUDE_JANUARY`) for parity, except when the target month
> itself *is* January (excluding it then would zero out the data). If
> that exclusion wasn't actually intentional, just set
> `EXCLUDE_JANUARY = False` in `src/config.py`.

## GitHub Actions setup (automated monthly run)

### 1. Push this repo to GitHub

```bash
git init
git add .
git commit -m "CLIRNET Content Analysis BI pipeline"
git branch -M main
git remote add origin https://github.com/<your-org>/<your-repo>.git
git push -u origin main
```

### 2. Create a GitHub Environment for secrets

Using a named **Environment** (rather than plain repo secrets) keeps this
pipeline's database credentials isolated from anything else in the repo,
and lets you optionally require manual approval before a scheduled run
touches production databases.

1. Repo → **Settings** → **Environments** → **New environment**
2. Name it `production` (must match `environment: production` in
   `.github/workflows/monthly_pipeline.yml`)
3. *(Optional but recommended)* Under **Deployment protection rules**, add
   yourself as a required reviewer if you want to approve each run.

### 3. Add every secret to that Environment

Inside the `production` environment page → **Add secret**, add each of
these (same names as `.env.example`, values from your actual databases):

```
CRM_DB_HOST, CRM_DB_USER, CRM_DB_PASSWORD, CRM_DB_DATABASE, CRM_DB_REPLICA_DATABASE
SHORTLINK_DB_HOST, SHORTLINK_DB_USER, SHORTLINK_DB_PASSWORD, SHORTLINK_DB_DATABASE
NOTIFICATION_SHORTLINK_DB_HOST, NOTIFICATION_SHORTLINK_DB_USER, NOTIFICATION_SHORTLINK_DB_PASSWORD, NOTIFICATION_SHORTLINK_DB_DATABASE
CLIRNET_DB_HOST, CLIRNET_DB_USER, CLIRNET_DB_PASSWORD, CLIRNET_DB_DATABASE
CLICKHOUSE_HOST, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD
EMAIL_FROM, EMAIL_PASSWORD, EMAIL_SMTP_SERVER, EMAIL_SMTP_PORT, EMAIL_TO, EMAIL_SIGNOFF_NAME
```

Or via the GitHub CLI, once you have `gh` installed and authenticated:

```bash
gh secret set CRM_DB_HOST --env production --body "aws.connect.psdb.cloud"
gh secret set CRM_DB_USER --env production --body "..."
# ...repeat for every variable in .env.example
```

### 4. Verify the schedule

The workflow (`.github/workflows/monthly_pipeline.yml`) runs at
`30 3 2 * *` — 03:30 UTC on the 2nd of every month, which is 09:00 AM
**Asia/Kolkata (IST)** on the same date (GitHub Actions cron has no
timezone setting, so the schedule is always written in UTC — IST is
UTC+5:30, and since 09:00 IST minus 5:30 still lands on 03:30 the same
calendar day, the day-of-month number matches in both timezones). Paste
the expression into [crontab.guru](https://crontab.guru) if you want to
change the day or time — just note that shifting the IST time earlier
than ~05:30 AM will push the equivalent UTC time into the previous day,
so you'd need to set the day-of-month one lower to compensate.

### 5. Test it manually before trusting the schedule

Repo → **Actions** → **Monthly Content Analysis BI Pipeline** → **Run
workflow**:
- Leave `target_month` blank to test against last month, or set it (e.g.
  `2026-06`) to backfill a specific month.
- Set `send_email` to `false` for your first test run — check the
  generated files in the workflow's **Artifacts** section before letting
  it actually email anyone.

## Output files

Written to `data/` (git-ignored; also uploaded as a GitHub Actions
artifact on every run):

| File | Emailed? | Description |
|---|---|---|
| `Speciality_month_diff_campaign_<Month>.csv` | ✅ | Engagement by speciality × months-since-publication |
| `Content_template_analysis_<Month>.csv` | ✅ | Engagement by content/channel/template |
| `All_content_data_Current.xlsx` | ✅ | Full current snapshot of every content type + reach/popularity |
| `data_month_<Month>.xlsx` | — | Supplementary click-rate summary (Step 10; kept for parity with the original pipeline, not part of the 3 emailed deliverables) |
| `*.parquet` | — | Intermediate files each step passes to the next |

## Email format

Subject: `Content Analysis BI – <Month> <Year> Data`

```
Hi Team,

Please find the requested updated data for the Content Analysis BI for <Month> <Year> attached.

The following data has been shared:
Speciality_month_diff_campaign – <Month>
Content_template_analysis – <Month>
All content data – Current

Thanks and Regards,
<EMAIL_SIGNOFF_NAME>
```

Recipients come from `EMAIL_TO` (comma-separated for multiple addresses).

## Handling very large data

CLIRNET's tables — especially `user_data_index`, the physician master
table — are large and growing. A few things in this pipeline exist
specifically for that:

- **Streaming writes**: Step 6 never holds the full table in memory; it
  writes each page straight to Parquet (`db.stream_offset_paginated_to_parquet`).
- **Primary-key pagination**: Step 5's biggest content tables page by
  `id > last_id` instead of `LIMIT/OFFSET`, which stays fast even on huge
  tables (OFFSET pagination gets slower the further in you page; PK
  pagination doesn't).
- **Automatic retry/reconnect**: every DB call retries transient
  connection blips with backoff instead of failing the whole run.
- **Tunable batch sizes** (`.env.example`): if a step runs out of memory or
  hits a provider-side row/response-size limit, lower `SQL_PAGE_SIZE`,
  `ID_BATCH_SIZE`, or `SPEC_BATCH_SIZE` — no code changes needed.
- **Parquet everywhere** for intermediate files — far smaller and faster
  to read/write than CSV, with types preserved.

If any single table eventually grows past what fits comfortably through
pandas merges (Step 9/11's joins), the next step up would be swapping
those specific joins for [DuckDB](https://duckdb.org/) reading the
Parquet files directly (it can join larger-than-RAM data without loading
everything into a DataFrame first) — the Parquet-based intermediate
format this pipeline already uses would make that a localized change, not
a rewrite.

## Extending: adding a new content type

Add one entry to `CONTENT_TYPES` in `src/content_registry.py` — nothing
else in `step05_content_data.py` needs to change. See the existing 7
entries for the two supported query shapes (`STYLE_PAGED` for large
tables, `STYLE_SIMPLE` for smaller ones with a single `GROUP_CONCAT`
query).

## Security note

The original notebooks had 5 database passwords and an email app password
committed in plaintext across multiple `.ipynb` files. Since those files
existed with real credentials in them, **treat every one of those
credentials as compromised and rotate them** (generate new PlanetScale
passwords, a new ClickHouse password, and a new Office 365 app password)
before/after wiring up the secrets above — don't just copy the old values
over. Once rotated, they only need to exist as GitHub Environment secrets
and in your local `.env` (which is git-ignored).

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `EnvironmentError: Missing required environment variable ...` | `.env` isn't loaded into your shell, or a secret isn't set in the `production` Environment |
| Step 1 or 2 returns 0 rows | Check `TARGET_MONTH` resolved to the month you expect — the log line `Reporting month: ...` at the top of every run shows it |
| `smtplib.SMTPAuthenticationError` | `EMAIL_PASSWORD` needs to be an Office 365 **app password**, not the account's normal login password |
| A step is slow / times out | Lower `SQL_PAGE_SIZE` / `ID_BATCH_SIZE` in `.env`, or raise the workflow's `timeout-minutes` |
| GitHub Actions run shows secrets as empty | Confirm secrets were added to the `production` **Environment**, not just repo-level "Actions secrets" |


