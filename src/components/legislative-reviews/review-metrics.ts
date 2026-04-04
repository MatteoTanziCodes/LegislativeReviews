export const REVIEW_DECISIONS = [
	"retain",
	"amend",
	"repeal_candidate",
	"escalate",
] as const;

export type ReviewDecision = (typeof REVIEW_DECISIONS)[number];
export type ReviewPipelineStatus =
	| "in_progress"
	| "idle"
	| "complete"
	| "error";

export type ReviewProgressSnapshot = {
	totalCount: number;
	reviewedCount: number;
	dailyCapacity: number;
};

export type ReviewSummary = {
	averageConfidenceByDecision?: Partial<Record<ReviewDecision, number>>;
	dailyCapacity?: number;
	decisionCounts?: Partial<Record<ReviewDecision, number>>;
	lastUpdated?: string;
	pipelineStatus?: ReviewPipelineStatus;
	reviewedCount: number;
	totalCount: number;
};

export type ReviewProgressMetrics = ReviewProgressSnapshot & {
	estimatedDaysRemaining: number;
	percentReviewed: number;
	remainingCount: number;
};

export type ReviewRunStatus = {
	label: string;
	pulse: boolean;
	tone: "accent" | "muted" | "danger";
};

export function normalizeDecisionCounts(
	decisionCounts: ReviewSummary["decisionCounts"],
): Record<ReviewDecision, number> {
	return {
		retain: decisionCounts?.retain ?? 0,
		amend: decisionCounts?.amend ?? 0,
		repeal_candidate: decisionCounts?.repeal_candidate ?? 0,
		escalate: decisionCounts?.escalate ?? 0,
	};
}

export function deriveReviewProgressMetrics(
	snapshot: ReviewProgressSnapshot,
): ReviewProgressMetrics {
	const totalCount = Math.max(0, snapshot.totalCount);
	const reviewedCount = Math.min(Math.max(0, snapshot.reviewedCount), totalCount);
	const remainingCount = Math.max(0, totalCount - reviewedCount);
	const percentReviewed = totalCount === 0 ? 0 : (reviewedCount / totalCount) * 100;
	const estimatedDaysRemaining =
		snapshot.dailyCapacity > 0 ? Math.ceil(remainingCount / snapshot.dailyCapacity) : 0;

	return {
		...snapshot,
		reviewedCount,
		remainingCount,
		percentReviewed,
		estimatedDaysRemaining,
	};
}

export function deriveReviewRunStatus(
	summary: ReviewSummary,
): ReviewRunStatus {
	if (summary.pipelineStatus === "error") {
		return {
			label: "Review pipeline error",
			pulse: false,
			tone: "danger",
		};
	}

	if (summary.pipelineStatus === "idle") {
		return {
			label: "Review queue idle",
			pulse: false,
			tone: "muted",
		};
	}

	if (
		summary.pipelineStatus === "complete" ||
		summary.reviewedCount >= summary.totalCount
	) {
		return {
			label: "Review batch complete",
			pulse: false,
			tone: "muted",
		};
	}

	return {
		label: "Reviews in progress",
		pulse: true,
		tone: "accent",
	};
}
