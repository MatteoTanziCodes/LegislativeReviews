# Legislative Reviews

Build Canada legislative review tracker and local review pipeline for the Canadian federal legislative corpus.

## What This Project Does

This project has two responsibilities:

1. Process legislation locally with Python and Claude.
2. Publish frontend-ready results to Cloudflare R2 so the public dashboard can display them.

The hosted website is now intentionally read-only. It does not trigger reviews or run Python jobs.

## Final Production Model

This is the deployment model the repo now assumes:

- Local machine:
  - stores the full legislative dataset
  - runs the Python review pipeline
  - exports `review-summary.json` and `review-details.json`
  - uploads those JSON files to Cloudflare R2
- Cloudflare:
  - hosts the Next.js dashboard
  - reads the published JSON artifacts from R2
  - optionally stages public release daily through environment variables

The website does not need your local machine to stay online after results are published.

## What Data Lives Where

Local only:

- raw dataset
- processed parquet inputs and outputs
- resume journals and manifests

Hosted remotely in R2 for the website:

- `review-summary.json`
- `review-details.json`

Optional:

- if you want off-machine backup of the full processed review outputs, copy `processed/reviews_*.parquet`, `*.journal.jsonl`, and `*.manifest.json` into a separate backup bucket or another object store
- this is not required for the website itself

## One Command To Run Reviews

The easiest operator command is now:

```bash
python scripts/run_local_review_release.py
```

or:

```bash
npm run review
```

Useful helper scripts:

```bash
npm run smoke-test
npm run preprocess
npm run refresh-laws
```

If you omit the API key or domain, the script prompts for them.

By default, the command now checks Hugging Face for a newer `a2aj/canadian-laws` parquet revision, syncs the local source snapshot if needed, and then rebuilds the processed local corpus before it starts reviews.
The expensive classification steps are incremental by default: existing unchanged documents are reused, and only new or changed documents are reclassified and rescored.

Examples:

Run one domain:

```bash
python scripts/run_local_review_release.py --domain transport_infrastructure
```

Run all domains:

```bash
python scripts/run_local_review_release.py --domain all
```

Run all domains with a small test limit per domain:

```bash
python scripts/run_local_review_release.py --domain all --limit 10
```

Refresh the processed local corpus only:

```bash
python scripts/run_local_review_release.py --preprocess-only
```

Pull the latest remote parquet snapshot first, then rebuild the local corpus:

```bash
python scripts/run_local_review_release.py --refresh-source --preprocess-only
```

Skip the Hugging Face update check and just reuse the local source snapshot:

```bash
python scripts/run_local_review_release.py --domain transport_infrastructure --skip-source-sync
```

Skip preprocessing and reuse the existing processed artifacts:

```bash
python scripts/run_local_review_release.py --domain transport_infrastructure --skip-preprocess
```

Force a full reclassification pass for every document:

```bash
python scripts/run_local_review_release.py --domain transport_infrastructure --reclassify-all
```

Pass the Claude key directly:

```bash
python scripts/run_local_review_release.py --domain transport_infrastructure --api-key YOUR_KEY
```

What this command does:

1. Ensures the Claude API key is available.
2. Checks whether the remote parquet snapshot changed and syncs it locally when needed.
3. Rebuilds `documents_en.parquet`, `sections_en.parquet`, and classifier inputs.
4. Reuses existing classifications and domain scores for unchanged documents, and recomputes only new or changed documents by default.
5. Builds reviewer-ready parquet inputs for the chosen domain.
6. Runs the review pipeline.
7. Exports `review-summary.json` and `review-details.json`.
8. Publishes those JSON artifacts to R2 if the R2 env vars are configured.

## Staged Daily Release

The site can reveal processed results gradually instead of all at once.

Set:

- `LEGISLATIVE_REVIEW_DAILY_RELEASE=all` to show everything immediately
- `LEGISLATIVE_REVIEW_DAILY_RELEASE=200` to reveal 200 reviewed rows per day
- optionally `LEGISLATIVE_REVIEW_ROLLOUT_START_DATE=2026-04-04T00:00:00-04:00` to pin the first release day
- optionally `LEGISLATIVE_REVIEW_ROLLOUT_TIMEZONE=America/Toronto` to control calendar-day boundaries

Important:

- the full reviewed dataset can already be present in R2
- the API applies the visibility gate at runtime
- no separate cron job is required
- if no explicit rollout start date is set, the dashboard uses the exported summary `lastUpdated` timestamp as day one

## Prerequisites

Operator machine:

- Node 20+
- Python 3.10+
- access to the legislative dataset
- Anthropic API key
- Cloudflare account
- Wrangler CLI access

## Initial Setup For A New Owner

### 1. Clone and install

```bash
git clone <repo-url>
cd LegislativeReviews
npm ci
pip install -r requirements.txt
```

### 2. Create `.env`

Copy [.env.example](/d:/Programming/Projects/LegislativeReviews/.env.example) to `.env`.

Minimum local processing values:

- `CLAUDE_API_KEY`
- `LEGISLATIVE_REVIEW_DATA_ROOT`

Default project-local value:

- `LEGISLATIVE_REVIEW_DATA_ROOT=docs\canadian-laws`

Minimum remote publishing values:

- `CLOUDFLARE_R2_ACCOUNT_ID`
- `CLOUDFLARE_R2_BUCKET`
- `CLOUDFLARE_R2_ENDPOINT`
- `CLOUDFLARE_R2_ACCESS_KEY_ID`
- `CLOUDFLARE_R2_SECRET_ACCESS_KEY`

Optional:

- `LEGISLATIVE_REVIEW_PROCESSED_DIR`
- `FASTEMBED_THREADS`
- `LEGISLATIVE_REVIEW_DAILY_RELEASE`
- `LEGISLATIVE_REVIEW_ROLLOUT_START_DATE`
- `LEGISLATIVE_REVIEW_ROLLOUT_TIMEZONE`

### 3. Create Cloudflare R2 buckets

Staging:

```bash
npx wrangler r2 bucket create legislative-review-data-staging
```

Production:

```bash
npx wrangler r2 bucket create legislative-review-data-production
```

### 4. Confirm Wrangler config

[wrangler.jsonc](/d:/Programming/Projects/LegislativeReviews/wrangler.jsonc) is already set up for:

- `legislativereviews-staging` -> `legislative-review-data-staging`
- `legislativereviews` -> `legislative-review-data-production`

If your Cloudflare account or bucket names differ, update that file before deploying.

### 5. Deploy the frontend

Staging:

```bash
npm run deploy:staging
```

Production:

```bash
npm run deploy:production
```

### 6. Publish your first dataset

Run a small local test:

```bash
python scripts/run_local_review_release.py --domain transport_infrastructure --limit 5
```

If the R2 env vars are present, the command will publish the frontend artifacts automatically.

If you also want to pull the latest source snapshot first:

```bash
python scripts/run_local_review_release.py --refresh-source --domain transport_infrastructure --limit 5
```

Before a long run, validate the local setup and R2 access:

```bash
npm run smoke-test
```

## Daily Operator Workflow

The normal operator workflow is:

1. Optionally refresh the raw laws snapshot.
2. Run a local review batch.
3. Let the script rebuild the processed local corpus before review generation.
4. Let the script publish the refreshed JSON to R2.
5. The Cloudflare site updates automatically.
6. The daily release gate controls how much of the already-published reviewed set is visible.

Typical commands:

Process one domain fully:

```bash
python scripts/run_local_review_release.py --domain transport_infrastructure
```

Process every supported domain:

```bash
python scripts/run_local_review_release.py --domain all
```

Refresh the laws snapshot first, then process one domain:

```bash
python scripts/run_local_review_release.py --refresh-source --domain transport_infrastructure
```

Refresh only the local processed corpus without running reviews:

```bash
python scripts/run_local_review_release.py --preprocess-only
```

If you already have a finished review parquet and only want to republish website artifacts:

```bash
python scripts/export_frontend_review_data.py --review-output-path <path-to-review-parquet>
```

## Local Development

Run the site locally:

```bash
npm run dev
```

Open:

```text
http://localhost:3000/legislative-reviews
```

The local app reads `src/data/review-summary.json` and `src/data/review-details.json` when Cloudflare bindings are not available.

## Deployment Commands

Build:

```bash
npm run build
```

Preview:

```bash
npm run preview:staging
npm run preview:production
```

Deploy:

```bash
npm run deploy:staging
npm run deploy:production
```

## Source Data Layout

The default local source layout is:

```text
docs/canadian-laws/
  default/train/*.parquet
  processed/
  metadata.json
```

The review scripts now default to this project-local path. Only set `LEGISLATIVE_REVIEW_DATA_ROOT` or `LEGISLATIVE_REVIEW_PROCESSED_DIR` if the next owner stores the corpus elsewhere.

## Recommended Remote Data Readiness

If the goal is "the website must not depend on a local machine being online", the correct readiness setup is:

1. Keep the raw and processed legislative corpus local.
2. Treat R2 as the remote source of truth for website data.
3. Publish `review-summary.json` and `review-details.json` after each local run.
4. Deploy the frontend once to Cloudflare.
5. Let the site read from R2 only.

That is the simplest reliable production model for handoff.

If you want full off-machine backup later, add a second backup location for the review parquet outputs, but that is optional and separate from the website.

For the hosted site itself, the only data that must live remotely is:

- `review-summary.json`
- `review-details.json`

Everything else can remain local to the operator machine.

## Troubleshooting

If the site shows no data:

- verify the correct R2 bucket is configured in Cloudflare
- verify `review-summary.json` and `review-details.json` exist in that bucket
- verify the Worker environment has the correct bucket binding

If local reviews fail immediately:

- verify `CLAUDE_API_KEY`
- verify `LEGISLATIVE_REVIEW_DATA_ROOT`
- verify `docs/canadian-laws/default/train/*.parquet` exists
- rerun `python scripts/run_local_review_release.py --preprocess-only`

If the site shows all results instead of daily rollout:

- verify `LEGISLATIVE_REVIEW_DAILY_RELEASE` is set in Cloudflare
- redeploy after changing Worker vars

## Files To Know

Primary operator script:

- [run_local_review_release.py](/d:/Programming/Projects/LegislativeReviews/scripts/run_local_review_release.py)

Release smoke test:

- [smoke_test_release_setup.py](/d:/Programming/Projects/LegislativeReviews/scripts/smoke_test_release_setup.py)

Per-domain pipeline:

- [run_review_frontend_pipeline.py](/d:/Programming/Projects/LegislativeReviews/scripts/run_review_frontend_pipeline.py)

Frontend artifact exporter:

- [export_frontend_review_data.py](/d:/Programming/Projects/LegislativeReviews/scripts/export_frontend_review_data.py)

Cloudflare config:

- [wrangler.jsonc](/d:/Programming/Projects/LegislativeReviews/wrangler.jsonc)

Environment template:

- [.env.example](/d:/Programming/Projects/LegislativeReviews/.env.example)
