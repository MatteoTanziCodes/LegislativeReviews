import type { Metadata } from "next";
import { LiveDashboard } from "@/components/legislative-reviews/live-dashboard";
import type {
	ReviewDashboardPayload,
	ReviewDetail,
} from "@/components/legislative-reviews/review-data";
import type { ReviewSummary } from "@/components/legislative-reviews/review-metrics";
import reviewDetailsData from "@/data/review-details.json";
import reviewSummaryData from "@/data/review-summary.json";

export const metadata: Metadata = {
	title: "Legislative Reviews | Build Canada",
	description:
		"Progress dashboard for the Canadian legislative modernization review pipeline.",
};

const initialData: ReviewDashboardPayload = {
	reviews: reviewDetailsData as ReviewDetail[],
	summary: reviewSummaryData as ReviewSummary,
};

export default function LegislativeReviewsPage() {
	return <LiveDashboard initialData={initialData} />;
}
