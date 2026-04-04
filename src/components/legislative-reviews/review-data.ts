import type {
	ReviewDecision,
	ReviewSummary,
} from "@/components/legislative-reviews/review-metrics";

export type ReviewDetail = {
	administrativeBurdenScore: number;
	citationEn: string | null;
	decision: ReviewDecision;
	decisionConfidence: number;
	documentId: string;
	evidenceSectionKeys: string[];
	operationalRelevanceScore: number;
	prosperityAlignmentScore: number;
	rationale: string;
	repealRiskScore: number;
	reviewModel: string;
	titleEn: string;
};

export type ReviewAdminEvent = {
	level: "info" | "error";
	message: string;
	timestamp: string;
};

export type ReviewRunRequest = {
	commandId: string;
	domain: string;
	limit?: number | null;
	requestedAt: string;
	requestedBy: string;
	status: "pending" | "running" | "complete" | "error";
};

export type ReviewAdminState = {
	currentDomain?: string | null;
	currentLimit?: number | null;
	lastCommand?: ReviewRunRequest | null;
	lastCompletedAt?: string;
	lastError?: string | null;
	lastHeartbeatAt?: string;
	lastRequestedAt?: string;
	lastStartedAt?: string;
	recentEvents: ReviewAdminEvent[];
	workerHost?: string | null;
	workerPid?: number | null;
	workerStatus: "idle" | "pending" | "running" | "complete" | "error" | "offline";
};

export type ReviewDashboardPayload = {
	adminState: ReviewAdminState;
	reviews: ReviewDetail[];
	summary: ReviewSummary;
};

export function sanitizeDashboardPayloadForPublic(
	payload: ReviewDashboardPayload,
): ReviewDashboardPayload {
	return {
		...payload,
		adminState: {
			currentDomain: null,
			currentLimit: null,
			lastCommand: null,
			lastCompletedAt: payload.adminState.lastCompletedAt,
			lastError: null,
			lastHeartbeatAt: payload.adminState.lastHeartbeatAt,
			lastRequestedAt: undefined,
			lastStartedAt: undefined,
			recentEvents: [],
			workerHost: null,
			workerPid: null,
			workerStatus: payload.adminState.workerStatus,
		},
	};
}
