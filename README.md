# Legislative Reviews

Build Canada legislative review tracker and batch review pipeline for the Canadian federal legislative corpus.

## Architecture

Production is split into two runtimes:

1. Cloudflare Workers serves the Next.js dashboard.
2. GitHub Actions runs the Python review pipeline and publishes dashboard artifacts to Cloudflare R2.

The frontend reads two JSON artifacts from R2:

- `review-summary.json`
- `review-details.json`

The admin workflow state also lives in R2:

- `review-admin-state.json`

Public users should use:

- `/legislative-reviews`

Operators should use:

- `/legislative-reviews/admin`

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

- default/local: `legislativereviews` + `legislative-review-data`
- staging: `legislativereviews-staging` + `legislative-review-data-staging`
- production: `legislativereviews-production` + `legislative-review-data-production`

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

Set the dashboard admin secret in each Worker environment:

```bash
npx wrangler secret put LEGISLATIVE_REVIEW_ADMIN_TOKEN --env staging
npx wrangler secret put LEGISLATIVE_REVIEW_ADMIN_TOKEN --env production
npx wrangler secret put LEGISLATIVE_REVIEW_SESSION_SECRET --env staging
npx wrangler secret put LEGISLATIVE_REVIEW_SESSION_SECRET --env production
npx wrangler secret put GITHUB_REVIEW_WORKFLOW_TOKEN --env staging
npx wrangler secret put GITHUB_REVIEW_WORKFLOW_TOKEN --env production
```

Each environment is configured with:

- `LEGISLATIVE_REVIEW_DATA_BUCKET`
- `LEGISLATIVE_REVIEW_SUMMARY_KEY=review-summary.json`
- `LEGISLATIVE_REVIEW_DETAILS_KEY=review-details.json`
- `LEGISLATIVE_REVIEW_ADMIN_STATE_KEY=review-admin-state.json`
- `LEGISLATIVE_REVIEW_ADMIN_TOKEN` as a Worker secret
- optionally `LEGISLATIVE_REVIEW_SESSION_SECRET` as a Worker secret for admin session signing
- `GITHUB_REVIEW_WORKFLOW_OWNER`
- `GITHUB_REVIEW_WORKFLOW_REPO`
- `GITHUB_REVIEW_WORKFLOW_ID=review-pipeline.yml`
- `GITHUB_REVIEW_WORKFLOW_REF=main`
- `GITHUB_REVIEW_WORKFLOW_ENVIRONMENT=staging|production`
- `GITHUB_REVIEW_WORKFLOW_TOKEN` as a Worker secret with permission to dispatch Actions workflows in this repository

## GitHub Actions Review Runner

Prerequisites:

- Python 3.10+
- A GitHub Actions runner with access to the raw and processed parquet paths under a configurable dataset root
- Anthropic API key
- Cloudflare R2 API credentials for the publisher

The repo now includes [review-pipeline.yml](/d:/Programming/Projects/LegislativeReviews/.github/workflows/review-pipeline.yml), which is the production execution path for review runs. The dashboard dispatches that workflow from `/legislative-reviews/admin`.

The workflow currently uses a self-hosted runner with the label `legislative-reviews` because the review corpus lives outside the repository. If you later move the review input parquet into remote storage, you can switch the workflow to a GitHub-hosted runner.

Set up the self-hosted runner on the machine that already has access to your legislative dataset:

```bash
./config.sh --labels legislative-reviews
./run.sh
```

Then configure GitHub environment variables and secrets for both `staging` and `production`.

Environment variables:

- `LEGISLATIVE_REVIEW_DATA_ROOT`
- optionally `LEGISLATIVE_REVIEW_PROCESSED_DIR`
- `FASTEMBED_THREADS`
- `CLOUDFLARE_R2_ACCOUNT_ID`
- `CLOUDFLARE_R2_BUCKET`
- `CLOUDFLARE_R2_ENDPOINT`
- optionally `CLOUDFLARE_R2_SUMMARY_KEY`
- optionally `CLOUDFLARE_R2_DETAILS_KEY`
- optionally `CLOUDFLARE_R2_ADMIN_STATE_KEY`

Environment secrets:

- `CLAUDE_API_KEY`
- `CLOUDFLARE_R2_ACCESS_KEY_ID`
- `CLOUDFLARE_R2_SECRET_ACCESS_KEY`

You can still run the pipeline manually on the runner machine for debugging:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Copy [.env.example](/d:/Programming/Projects/LegislativeReviews/.env.example) to `.env` on the runner machine and fill in:

- `LEGISLATIVE_REVIEW_DATA_ROOT`
- optionally `LEGISLATIVE_REVIEW_PROCESSED_DIR`
- `CLAUDE_API_KEY`
- `CLOUDFLARE_R2_ACCOUNT_ID`
- `CLOUDFLARE_R2_BUCKET`
- `CLOUDFLARE_R2_ENDPOINT`
- `CLOUDFLARE_R2_ACCESS_KEY_ID`
- `CLOUDFLARE_R2_SECRET_ACCESS_KEY`
- optionally `CLOUDFLARE_R2_ADMIN_STATE_KEY`

## Batch Review Commands

The Python scripts now derive dataset paths from:

- `LEGISLATIVE_REVIEW_DATA_ROOT`
- `LEGISLATIVE_REVIEW_PROCESSED_DIR` if you need processed artifacts in a separate mount

If you do not set them, the scripts fall back to the original Windows development path.

Run the end-to-end review pipeline for one domain:

```bash
python scripts/run_review_frontend_pipeline.py --domain transport_infrastructure
```

Resume behavior is enabled by default. If the review runner stops mid-run, restarting the same command resumes from the last durable success using the existing review parquet plus a journal file written beside it. Use `--no-resume` only when you intentionally want to restart the batch from scratch.

Run a smaller batch:

```bash
python scripts/run_review_frontend_pipeline.py --domain transport_infrastructure --limit 50
```

During review, [review_documents.py](/d:/Programming/Projects/LegislativeReviews/scripts/review_documents.py) checkpoints:

- the parquet review output
- a per-review resume journal beside the parquet output
- local mirrored frontend JSON
- R2 dashboard JSON, if `CLOUDFLARE_R2_*` variables are configured

The workflow updater script writes audit state to:

- `review-admin-state.json`

## Local Dashboard Development

Run the app locally:

```bash
npm run dev
```

Open:

```text
http://localhost:3000/legislative-reviews
```

Admin access:

```text
http://localhost:3000/legislative-reviews/admin
```

The dashboard polls `/api/legislative-reviews` every few seconds. In local development that route falls back to local JSON artifacts in `src/data/` if Cloudflare bindings are unavailable. If you also set the GitHub workflow environment variables in `.env`, the local admin screen can dispatch the real GitHub Actions workflow.

## Notes

- Cloudflare Workers is the correct place for the dashboard, not for the long-running Python batch process.
- GitHub Actions is now the production control plane for review execution.
- The dashboard is production-ready for shared storage via R2 and exposes an admin panel for dispatching and auditing GitHub Actions review runs.
- The Python publisher uses atomic local writes and uploads `review-details.json` before `review-summary.json` to reduce transient mismatch windows.
