import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import path from "node:path";
import { getCloudflareContext } from "@opennextjs/cloudflare";
import type {
	ReviewAdminState,
	ReviewDashboardPayload,
	ReviewDetail,
	ReviewRunRequest,
} from "@/components/legislative-reviews/review-data";
import type { ReviewSummary } from "@/components/legislative-reviews/review-metrics";

const SUMMARY_PATH = path.join(process.cwd(), "src", "data", "review-summary.json");
const DETAILS_PATH = path.join(process.cwd(), "src", "data", "review-details.json");
const ADMIN_STATE_PATH = path.join(
	process.cwd(),
	"src",
	"data",
	"review-admin-state.json",
);
const CONTROL_PATH = path.join(
	process.cwd(),
	"src",
	"data",
	"review-control.json",
);

const DEFAULT_SUMMARY_OBJECT_KEY = "review-summary.json";
const DEFAULT_DETAILS_OBJECT_KEY = "review-details.json";
const DEFAULT_ADMIN_STATE_OBJECT_KEY = "review-admin-state.json";
const DEFAULT_CONTROL_OBJECT_KEY = "review-control.json";
const DEFAULT_TOTAL_COUNT = 5796;
const DEFAULT_DAILY_CAPACITY = 200;
const READ_ATTEMPTS = 3;
const READ_RETRY_DELAY_MS = 75;

type DashboardStorageEnv = CloudflareEnv & {
	LEGISLATIVE_REVIEW_ADMIN_STATE_KEY?: string;
	LEGISLATIVE_REVIEW_ADMIN_TOKEN?: string;
	LEGISLATIVE_REVIEW_CONTROL_KEY?: string;
	LEGISLATIVE_REVIEW_DATA_BUCKET?: R2Bucket;
	LEGISLATIVE_REVIEW_DETAILS_KEY?: string;
	LEGISLATIVE_REVIEW_SUMMARY_KEY?: string;
};

function delay(ms: number) {
	return new Promise((resolve) => {
		setTimeout(resolve, ms);
	});
}

function createDefaultAdminState(): ReviewAdminState {
	return {
		currentDomain: null,
		currentLimit: null,
		lastCommand: null,
		lastError: null,
		recentEvents: [],
		workerHost: null,
		workerPid: null,
		workerStatus: "idle",
	};
}

function createEmptySummary(adminState?: ReviewAdminState): ReviewSummary {
	const pipelineStatus =
		adminState?.workerStatus === "running" || adminState?.workerStatus === "pending"
			? "in_progress"
			: adminState?.workerStatus === "error"
				? "error"
				: "idle";

	return {
		totalCount: DEFAULT_TOTAL_COUNT,
		reviewedCount: 0,
		dailyCapacity: DEFAULT_DAILY_CAPACITY,
		lastUpdated: adminState?.lastCompletedAt ?? adminState?.lastHeartbeatAt,
		pipelineStatus,
		decisionCounts: {
			retain: 0,
			amend: 0,
			repeal_candidate: 0,
			escalate: 0,
		},
		averageConfidenceByDecision: {
			retain: 0,
			amend: 0,
			repeal_candidate: 0,
			escalate: 0,
		},
	};
}

function createEmptyDashboardPayload(
	adminState: ReviewAdminState = createDefaultAdminState(),
): ReviewDashboardPayload {
	return {
		adminState,
		reviews: [],
		summary: createEmptySummary(adminState),
	};
}

function isMissingDashboardArtifactError(error: unknown): boolean {
	if (!(error instanceof Error)) {
		return false;
	}

	const message = error.message.toLowerCase();
	return (
		message.includes("missing from r2") ||
		message.includes("enoent") ||
		message.includes("no such file") ||
		message.includes("binding is unavailable")
	);
}

async function getDashboardStorageEnv(): Promise<DashboardStorageEnv | null> {
	if (process.env.NODE_ENV === "development") {
		return null;
	}

	try {
		const { env } = await getCloudflareContext({ async: true });
		return env as DashboardStorageEnv;
	} catch {
		return null;
	}
}

async function readLocalJson<T>(filePath: string): Promise<T | null> {
	try {
		const payload = await readFile(filePath, "utf-8");
		return JSON.parse(payload) as T;
	} catch (error) {
		if (isMissingDashboardArtifactError(error)) {
			return null;
		}
		throw error;
	}
}

async function writeLocalJson(filePath: string, payload: unknown): Promise<void> {
	await mkdir(path.dirname(filePath), { recursive: true });
	const tempPath = `${filePath}.${process.pid}.tmp`;
	await writeFile(
		tempPath,
		`${JSON.stringify(payload, null, 2)}\n`,
		"utf-8",
	);
	await rename(tempPath, filePath);
}

async function readR2Json<T>(
	bucket: R2Bucket,
	objectKey: string,
): Promise<T | null> {
	const object = await bucket.get(objectKey);
	if (!object) {
		return null;
	}
	return object.json<T>();
}

async function writeR2Json(
	bucket: R2Bucket,
	objectKey: string,
	payload: unknown,
): Promise<void> {
	await bucket.put(
		objectKey,
		`${JSON.stringify(payload, null, 2)}\n`,
		{
			httpMetadata: {
				cacheControl: "no-store, no-cache, must-revalidate, max-age=0",
				contentType: "application/json; charset=utf-8",
			},
		},
	);
}

async function loadLocalDashboardPayload(): Promise<ReviewDashboardPayload> {
	const [summary, reviews, adminState] = await Promise.all([
		readLocalJson<ReviewSummary>(SUMMARY_PATH),
		readLocalJson<ReviewDetail[]>(DETAILS_PATH),
		readLocalJson<ReviewAdminState>(ADMIN_STATE_PATH),
	]);

	const effectiveAdminState = adminState ?? createDefaultAdminState();
	if (!summary || !reviews) {
		return createEmptyDashboardPayload(effectiveAdminState);
	}

	return {
		adminState: effectiveAdminState,
		reviews,
		summary,
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
	const adminStateObjectKey =
		env.LEGISLATIVE_REVIEW_ADMIN_STATE_KEY ?? DEFAULT_ADMIN_STATE_OBJECT_KEY;

	let lastError: unknown;

	for (let attempt = 1; attempt <= READ_ATTEMPTS; attempt += 1) {
		try {
			const [summary, reviews, adminState] = await Promise.all([
				readR2Json<ReviewSummary>(bucket, summaryObjectKey),
				readR2Json<ReviewDetail[]>(bucket, detailsObjectKey),
				readR2Json<ReviewAdminState>(bucket, adminStateObjectKey),
			]);

			const effectiveAdminState = adminState ?? createDefaultAdminState();
			if (!summary || !reviews) {
				return createEmptyDashboardPayload(effectiveAdminState);
			}

			if (summary.reviewedCount === reviews.length || attempt === READ_ATTEMPTS) {
				return {
					adminState: effectiveAdminState,
					reviews,
					summary,
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

export async function loadReviewAdminState(): Promise<ReviewAdminState> {
	const env = await getDashboardStorageEnv();
	if (env?.LEGISLATIVE_REVIEW_DATA_BUCKET) {
		const adminState =
			(await readR2Json<ReviewAdminState>(
				env.LEGISLATIVE_REVIEW_DATA_BUCKET,
				env.LEGISLATIVE_REVIEW_ADMIN_STATE_KEY ?? DEFAULT_ADMIN_STATE_OBJECT_KEY,
			)) ?? createDefaultAdminState();
		return adminState;
	}

	return (
		(await readLocalJson<ReviewAdminState>(ADMIN_STATE_PATH)) ??
		createDefaultAdminState()
	);
}

export async function loadReviewControlRequest(): Promise<ReviewRunRequest | null> {
	const env = await getDashboardStorageEnv();
	if (env?.LEGISLATIVE_REVIEW_DATA_BUCKET) {
		return readR2Json<ReviewRunRequest>(
			env.LEGISLATIVE_REVIEW_DATA_BUCKET,
			env.LEGISLATIVE_REVIEW_CONTROL_KEY ?? DEFAULT_CONTROL_OBJECT_KEY,
		);
	}

	return readLocalJson<ReviewRunRequest>(CONTROL_PATH);
}

export async function persistReviewAdminState(
	adminState: ReviewAdminState,
): Promise<void> {
	const env = await getDashboardStorageEnv();
	if (env?.LEGISLATIVE_REVIEW_DATA_BUCKET) {
		await writeR2Json(
			env.LEGISLATIVE_REVIEW_DATA_BUCKET,
			env.LEGISLATIVE_REVIEW_ADMIN_STATE_KEY ?? DEFAULT_ADMIN_STATE_OBJECT_KEY,
			adminState,
		);
		return;
	}

	await writeLocalJson(ADMIN_STATE_PATH, adminState);
}

export async function persistReviewControlRequest(
	request: ReviewRunRequest,
): Promise<void> {
	const env = await getDashboardStorageEnv();
	if (env?.LEGISLATIVE_REVIEW_DATA_BUCKET) {
		await writeR2Json(
			env.LEGISLATIVE_REVIEW_DATA_BUCKET,
			env.LEGISLATIVE_REVIEW_CONTROL_KEY ?? DEFAULT_CONTROL_OBJECT_KEY,
			request,
		);
		return;
	}

	await writeLocalJson(CONTROL_PATH, request);
}

export async function getReviewAdminToken(): Promise<string | null> {
	const env = await getDashboardStorageEnv();
	return env?.LEGISLATIVE_REVIEW_ADMIN_TOKEN ?? process.env.LEGISLATIVE_REVIEW_ADMIN_TOKEN ?? null;
}

export async function loadReviewDashboardPayload(): Promise<ReviewDashboardPayload> {
	const bucketEnv = await getDashboardStorageEnv();

	if (bucketEnv?.LEGISLATIVE_REVIEW_DATA_BUCKET) {
		try {
			return await loadDashboardPayloadFromR2(bucketEnv);
		} catch (error) {
			if (!isMissingDashboardArtifactError(error)) {
				throw error;
			}

			console.warn(
				"Legislative review dashboard artifacts are unavailable in R2; returning empty dashboard payload.",
				error,
			);
			return createEmptyDashboardPayload();
		}
	}

	try {
		return await loadLocalDashboardPayload();
	} catch (error) {
		if (!isMissingDashboardArtifactError(error)) {
			throw error;
		}

		console.warn(
			"Local legislative review dashboard artifacts are unavailable; returning empty dashboard payload.",
			error,
		);
		return createEmptyDashboardPayload();
	}
}
