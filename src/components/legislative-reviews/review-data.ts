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

export type ReviewDashboardPayload = {
	reviews: ReviewDetail[];
	summary: ReviewSummary;
};
