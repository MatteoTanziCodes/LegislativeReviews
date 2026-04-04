# Legislative Reviews

Build Canada legislative review tracker and batch review pipeline for the Canadian federal legislative corpus.

## Architecture

Production is split into two runtimes:

1. Cloudflare Workers serves the Next.js dashboard.
2. A separate Python worker machine runs the review pipeline and publishes dashboard artifacts to Cloudflare R2.

The frontend reads two JSON artifacts from R2:

- `review-summary.json`
- `review-details.json`

Local development still works with mirrored files in `src/data/`.

## Frontend Deployment

Prerequisites:

- Node 20.x
- Wrangler access to your Cloudflare account
- Environment-specific R2 buckets, or update [wrangler.jsonc](/d:/Programming/Projects/LegislativeReviews/wrangler.jsonc)

Install and deploy:

```bash
npm ci
npx wrangler login
npx wrangler r2 bucket create legislative-review-data
npm run deploy
```

Environment layout in [wrangler.jsonc](/d:/Programming/Projects/LegislativeReviews/wrangler.jsonc):

- default/local: `core` + `legislative-review-data`
- staging: `core-staging` + `legislative-review-data-staging`
- production: `core-production` + `legislative-review-data-production`

Create the environment buckets:

```bash
npx wrangler r2 bucket create legislative-review-data-staging
npx wrangler r2 bucket create legislative-review-data-production
```

Preview or deploy by environment:

```bash
npm run preview:staging
npm run deploy:staging
npm run deploy:production
```

Each environment is configured with:

- `LEGISLATIVE_REVIEW_DATA_BUCKET`
- `LEGISLATIVE_REVIEW_SUMMARY_KEY=review-summary.json`
- `LEGISLATIVE_REVIEW_DETAILS_KEY=review-details.json`

## Python Worker Setup

Prerequisites:

- Python 3.10+
- Access to the raw and processed parquet paths under a configurable dataset root
- Anthropic API key
- Cloudflare R2 API credentials for the Python publisher

Create an environment and install dependencies:

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

On Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Copy [.env.example](/d:/Programming/Projects/LegislativeReviews/.env.example) to `.env` and fill in:

- `LEGISLATIVE_REVIEW_DATA_ROOT`
- optionally `LEGISLATIVE_REVIEW_PROCESSED_DIR`
- `CLAUDE_API_KEY`
- `CLOUDFLARE_R2_ACCOUNT_ID`
- `CLOUDFLARE_R2_BUCKET`
- `CLOUDFLARE_R2_ENDPOINT`
- `CLOUDFLARE_R2_ACCESS_KEY_ID`
- `CLOUDFLARE_R2_SECRET_ACCESS_KEY`

## Batch Review Commands

The Python scripts now derive dataset paths from:

- `LEGISLATIVE_REVIEW_DATA_ROOT`
- `LEGISLATIVE_REVIEW_PROCESSED_DIR` if you need processed artifacts in a separate mount

If you do not set them, the scripts fall back to the original Windows development path.

Run the end-to-end review pipeline for one domain:

```bash
python scripts/run_review_frontend_pipeline.py --domain transport_infrastructure
```

Resume behavior is enabled by default. If the worker stops mid-run, restarting the same command resumes from the last durable success using the existing review parquet plus a journal file written beside it. Use `--no-resume` only when you intentionally want to restart the batch from scratch.

Run a smaller batch:

```bash
python scripts/run_review_frontend_pipeline.py --domain transport_infrastructure --limit 50
```

During review, [review_documents.py](/d:/Programming/Projects/LegislativeReviews/scripts/review_documents.py) checkpoints:

- the parquet review output
- a per-review resume journal beside the parquet output
- local mirrored frontend JSON
- R2 dashboard JSON, if `CLOUDFLARE_R2_*` variables are configured

## Local Dashboard Development

Run the app locally:

```bash
npm run dev
```

Open:

```text
http://localhost:3000/legislative-reviews
```

The dashboard polls `/api/legislative-reviews` every few seconds. In local development that route falls back to `src/data/review-summary.json` and `src/data/review-details.json` if Cloudflare bindings are unavailable.

## Background Runner

An example systemd service is included at [legislative-reviews.service](/d:/Programming/Projects/LegislativeReviews/deploy/systemd/legislative-reviews.service).

Typical Linux install:

```bash
sudo cp deploy/systemd/legislative-reviews.service /etc/systemd/system/legislative-reviews.service
sudo systemctl daemon-reload
sudo systemctl enable legislative-reviews
sudo systemctl start legislative-reviews
sudo systemctl status legislative-reviews
journalctl -u legislative-reviews -f
```

Adjust:

- `User`
- `WorkingDirectory`
- `EnvironmentFile`
- `ExecStart`

before enabling the service.

## Notes

- Cloudflare Workers is the correct place for the dashboard, not for the long-running Python batch process.
- The dashboard is now production-ready for shared storage via R2.
- The Python publisher uses atomic local writes and uploads `review-details.json` before `review-summary.json` to reduce transient mismatch windows.
