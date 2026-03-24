import { readFile } from "node:fs/promises";
import path from "node:path";
import { getCloudflareContext } from "@opennextjs/cloudflare";
import type {
	ReviewDashboardPayload,
	ReviewDetail,
} from "@/components/legislative-reviews/review-data";
import type { ReviewSummary } from "@/components/legislative-reviews/review-metrics";

const SUMMARY_PATH = path.join(process.cwd(), "src", "data", "review-summary.json");
const DETAILS_PATH = path.join(process.cwd(), "src", "data", "review-details.json");
const DEFAULT_SUMMARY_OBJECT_KEY = "review-summary.json";
const DEFAULT_DETAILS_OBJECT_KEY = "review-details.json";
const READ_ATTEMPTS = 3;
const READ_RETRY_DELAY_MS = 75;

type DashboardStorageEnv = CloudflareEnv & {
	LEGISLATIVE_REVIEW_DATA_BUCKET?: R2Bucket;
	LEGISLATIVE_REVIEW_SUMMARY_KEY?: string;
	LEGISLATIVE_REVIEW_DETAILS_KEY?: string;
};

function delay(ms: number) {
	return new Promise((resolve) => {
		setTimeout(resolve, ms);
	});
}

async function loadLocalDashboardPayload(): Promise<ReviewDashboardPayload> {
	const [summaryText, detailsText] = await Promise.all([
		readFile(SUMMARY_PATH, "utf-8"),
		readFile(DETAILS_PATH, "utf-8"),
	]);

	return {
		summary: JSON.parse(summaryText) as ReviewSummary,
		reviews: JSON.parse(detailsText) as ReviewDetail[],
	};
}

async function loadDashboardPayloadFromR2(
	env: DashboardStorageEnv,
): Promise<ReviewDashboardPayload> {
	const bucket = env.LEGISLATIVE_REVIEW_DATA_BUCKET;
	if (!bucket) {
		throw new Error("LEGISLATIVE_REVIEW_DATA_BUCKET binding is unavailable.");
	}

	const summaryObjectKey =
		env.LEGISLATIVE_REVIEW_SUMMARY_KEY ?? DEFAULT_SUMMARY_OBJECT_KEY;
	const detailsObjectKey =
		env.LEGISLATIVE_REVIEW_DETAILS_KEY ?? DEFAULT_DETAILS_OBJECT_KEY;

	let lastError: unknown;

	for (let attempt = 1; attempt <= READ_ATTEMPTS; attempt += 1) {
		try {
			const [summaryObject, detailsObject] = await Promise.all([
				bucket.get(summaryObjectKey),
				bucket.get(detailsObjectKey),
			]);

			if (!summaryObject || !detailsObject) {
				throw new Error(
					"Legislative review dashboard artifacts are missing from R2.",
				);
			}

			const [summary, reviews] = await Promise.all([
				summaryObject.json<ReviewSummary>(),
				detailsObject.json<ReviewDetail[]>(),
			]);

			if (summary.reviewedCount === reviews.length || attempt === READ_ATTEMPTS) {
				return {
					summary,
					reviews,
				};
			}
		} catch (error) {
			lastError = error;
		}

		if (attempt < READ_ATTEMPTS) {
			await delay(READ_RETRY_DELAY_MS);
		}
	}

	throw lastError ?? new Error("Unable to load legislative review dashboard data.");
}

export async function loadReviewDashboardPayload(): Promise<ReviewDashboardPayload> {
	let bucketEnv: DashboardStorageEnv | null = null;

	try {
		const { env } = await getCloudflareContext({ async: true });
		bucketEnv = env as DashboardStorageEnv;
	} catch {
		// Fall back to local mirrored artifacts in local dev and non-Workers runtimes.
	}

	if (bucketEnv?.LEGISLATIVE_REVIEW_DATA_BUCKET) {
		return loadDashboardPayloadFromR2(bucketEnv);
	}

	return loadLocalDashboardPayload();
}
