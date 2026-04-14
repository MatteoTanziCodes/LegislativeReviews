import { getCloudflareContext } from "@opennextjs/cloudflare";
import type {
	ReviewDashboardPayload,
	ReviewDetail,
} from "@/components/legislative-reviews/review-data";
import {
	REVIEW_DECISIONS,
	type ReviewDecision,
	type ReviewSummary,
} from "@/components/legislative-reviews/review-metrics";

const DEFAULT_SUMMARY_OBJECT_KEY = "review-summary.json";
const DEFAULT_DETAILS_OBJECT_KEY = "review-details.json";
const DEFAULT_TOTAL_COUNT = 0;
const DEFAULT_DAILY_CAPACITY = 200;
const DEFAULT_ROLLOUT_TIMEZONE = "America/Toronto";
const READ_ATTEMPTS = 3;
const READ_RETRY_DELAY_MS = 75;

type DashboardStorageEnv = CloudflareEnv & {
	LEGISLATIVE_REVIEW_DATA_BUCKET?: R2Bucket;
	LEGISLATIVE_REVIEW_DAILY_RELEASE?: string;
	LEGISLATIVE_REVIEW_DETAILS_KEY?: string;
	LEGISLATIVE_REVIEW_ROLLOUT_START_DATE?: string;
	LEGISLATIVE_REVIEW_ROLLOUT_TIMEZONE?: string;
	LEGISLATIVE_REVIEW_SUMMARY_KEY?: string;
};

type ReviewRolloutConfig = {
	dailyRelease: number;
	startDate: string | null;
	timeZone: string;
};

function delay(ms: number) {
	return new Promise((resolve) => {
		setTimeout(resolve, ms);
	});
}

function createEmptySummary(): ReviewSummary {
	return {
		totalCount: DEFAULT_TOTAL_COUNT,
		reviewedCount: 0,
		dailyCapacity: DEFAULT_DAILY_CAPACITY,
		pipelineStatus: "idle",
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

function createEmptyDashboardPayload(): ReviewDashboardPayload {
	return {
		adminState: {
			currentDomain: null,
			currentLimit: null,
			lastCommand: null,
			lastError: null,
			lastRunHtmlUrl: null,
			lastRunId: null,
			recentEvents: [],
			workerHost: null,
			workerPid: null,
			workerStatus: "idle",
		},
		reviews: [],
		summary: createEmptySummary(),
	};
}

function getDashboardEnvValue(
	env: DashboardStorageEnv | null,
	key: keyof DashboardStorageEnv,
): string | null {
	const envValue = env?.[key];
	if (typeof envValue === "string" && envValue.trim()) {
		return envValue.trim();
	}

	const processValue = process.env[String(key)];
	return processValue?.trim() || null;
}

function parseReviewRolloutConfig(
	env: DashboardStorageEnv | null,
): ReviewRolloutConfig | null {
	const rawDailyRelease = getDashboardEnvValue(
		env,
		"LEGISLATIVE_REVIEW_DAILY_RELEASE",
	);
	if (!rawDailyRelease || rawDailyRelease.toLowerCase() === "all") {
		return null;
	}

	const parsedDailyRelease = Number.parseInt(rawDailyRelease, 10);
	if (!Number.isInteger(parsedDailyRelease) || parsedDailyRelease <= 0) {
		console.warn(
			"Invalid LEGISLATIVE_REVIEW_DAILY_RELEASE value. Falling back to full visibility.",
			rawDailyRelease,
		);
		return null;
	}

	return {
		dailyRelease: parsedDailyRelease,
		startDate: getDashboardEnvValue(env, "LEGISLATIVE_REVIEW_ROLLOUT_START_DATE"),
		timeZone:
			getDashboardEnvValue(env, "LEGISLATIVE_REVIEW_ROLLOUT_TIMEZONE") ??
			DEFAULT_ROLLOUT_TIMEZONE,
	};
}

function buildCalendarDateKey(date: Date, timeZone: string): string {
	const formatter = new Intl.DateTimeFormat("en-CA", {
		timeZone,
		year: "numeric",
		month: "2-digit",
		day: "2-digit",
	});
	const parts = formatter.formatToParts(date);
	const year = parts.find((part) => part.type === "year")?.value;
	const month = parts.find((part) => part.type === "month")?.value;
	const day = parts.find((part) => part.type === "day")?.value;
	if (!year || !month || !day) {
		throw new Error(`Unable to build rollout date key for timezone ${timeZone}.`);
	}
	return `${year}-${month}-${day}`;
}

function calculateDayDifference(startKey: string, endKey: string): number {
	const [startYear, startMonth, startDay] = startKey.split("-").map(Number);
	const [endYear, endMonth, endDay] = endKey.split("-").map(Number);
	const startValue = Date.UTC(startYear, startMonth - 1, startDay);
	const endValue = Date.UTC(endYear, endMonth - 1, endDay);
	return Math.floor((endValue - startValue) / 86_400_000);
}

function buildDecisionCounts(
	reviews: ReviewDetail[],
): Record<ReviewDecision, number> {
	const decisionCounts = Object.fromEntries(
		REVIEW_DECISIONS.map((decision) => [decision, 0]),
	) as Record<ReviewDecision, number>;

	for (const review of reviews) {
		decisionCounts[review.decision] += 1;
	}

	return decisionCounts;
}

function buildAverageConfidenceByDecision(
	reviews: ReviewDetail[],
	decisionCounts: Record<ReviewDecision, number>,
): Record<ReviewDecision, number> {
	const confidenceSums = Object.fromEntries(
		REVIEW_DECISIONS.map((decision) => [decision, 0]),
	) as Record<ReviewDecision, number>;

	for (const review of reviews) {
		confidenceSums[review.decision] += review.decisionConfidence;
	}

	return Object.fromEntries(
		REVIEW_DECISIONS.map((decision) => [
			decision,
			decisionCounts[decision] === 0
				? 0
				: Number(
						(confidenceSums[decision] / decisionCounts[decision]).toFixed(3),
					),
		]),
	) as Record<ReviewDecision, number>;
}

function applyReviewRollout(
	payload: ReviewDashboardPayload,
	rolloutConfig: ReviewRolloutConfig | null,
): ReviewDashboardPayload {
	if (!rolloutConfig) {
		return payload;
	}

	const availableReviewCount = payload.reviews.length;
	if (availableReviewCount === 0) {
		return payload;
	}

	const rolloutStartDate = rolloutConfig.startDate ?? payload.summary.lastUpdated ?? null;
	if (!rolloutStartDate) {
		console.warn(
			"LEGISLATIVE_REVIEW_DAILY_RELEASE is set but no rollout start date is available. Falling back to full visibility.",
		);
		return payload;
	}

	const rolloutStart = new Date(rolloutStartDate);
	if (Number.isNaN(rolloutStart.getTime())) {
		console.warn(
			"Invalid rollout start date. Falling back to full visibility.",
			rolloutStartDate,
		);
		return payload;
	}

	const currentDayKey = buildCalendarDateKey(new Date(), rolloutConfig.timeZone);
	const startDayKey = buildCalendarDateKey(rolloutStart, rolloutConfig.timeZone);
	const dayDifference = calculateDayDifference(startDayKey, currentDayKey);
	const visibleReviewCount =
		dayDifference < 0
			? 0
			: Math.min(
					availableReviewCount,
					(dayDifference + 1) * rolloutConfig.dailyRelease,
				);
	const visibleReviews = payload.reviews.slice(0, visibleReviewCount);
	const decisionCounts = buildDecisionCounts(visibleReviews);
	const averageConfidenceByDecision = buildAverageConfidenceByDecision(
		visibleReviews,
		decisionCounts,
	);

	return {
		...payload,
		reviews: visibleReviews,
		summary: {
			...payload.summary,
			averageConfidenceByDecision,
			dailyCapacity: rolloutConfig.dailyRelease,
			decisionCounts,
			reviewedCount: visibleReviewCount,
			rollout: {
				availableReviewCount,
				dailyRelease: rolloutConfig.dailyRelease,
				startDate: rolloutStartDate,
				timeZone: rolloutConfig.timeZone,
			},
		},
	};
}

export function isMissingDashboardArtifactError(error: unknown): boolean {
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
		const [{ readFile }, pathModule] = await Promise.all([
			import("node:fs/promises"),
			import("node:path"),
		]);
		const resolvedPath = pathModule.join(process.cwd(), "src", "data", filePath);
		const payload = await readFile(resolvedPath, "utf-8");
		return JSON.parse(payload) as T;
	} catch (error) {
		if (isMissingDashboardArtifactError(error)) {
			return null;
		}
		throw error;
	}
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

async function loadLocalDashboardPayload(): Promise<ReviewDashboardPayload> {
	const [summary, reviews] = await Promise.all([
		readLocalJson<ReviewSummary>(DEFAULT_SUMMARY_OBJECT_KEY),
		readLocalJson<ReviewDetail[]>(DEFAULT_DETAILS_OBJECT_KEY),
	]);

	if (!summary || !reviews) {
		return createEmptyDashboardPayload();
	}

	return {
		adminState: createEmptyDashboardPayload().adminState,
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

	let lastError: unknown;

	for (let attempt = 1; attempt <= READ_ATTEMPTS; attempt += 1) {
		try {
			const [summary, reviews] = await Promise.all([
				readR2Json<ReviewSummary>(bucket, summaryObjectKey),
				readR2Json<ReviewDetail[]>(bucket, detailsObjectKey),
			]);

			if (!summary || !reviews) {
				throw new Error("Legislative review dashboard artifacts are missing from R2.");
			}

			if (summary.reviewedCount === reviews.length || attempt === READ_ATTEMPTS) {
				return {
					adminState: createEmptyDashboardPayload().adminState,
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

export async function loadReviewDashboardPayload(): Promise<ReviewDashboardPayload> {
	const bucketEnv = await getDashboardStorageEnv();
	const rolloutConfig = parseReviewRolloutConfig(bucketEnv);

	if (bucketEnv?.LEGISLATIVE_REVIEW_DATA_BUCKET) {
		return applyReviewRollout(
			await loadDashboardPayloadFromR2(bucketEnv),
			rolloutConfig,
		);
	}

	try {
		return applyReviewRollout(await loadLocalDashboardPayload(), rolloutConfig);
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
