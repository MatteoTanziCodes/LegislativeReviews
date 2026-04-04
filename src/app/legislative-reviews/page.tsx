import type { Metadata } from "next";
import { LiveDashboard } from "@/components/legislative-reviews/live-dashboard";
import { sanitizeDashboardPayloadForPublic } from "@/components/legislative-reviews/review-data";
import { loadReviewDashboardPayload } from "@/lib/legislative-review-storage";

export const metadata: Metadata = {
	title: "Legislative Reviews | Build Canada",
	description:
		"Progress dashboard for the Canadian legislative modernization review pipeline.",
};

export const dynamic = "force-dynamic";
export const revalidate = 0;

export default async function LegislativeReviewsPage() {
	const initialData = await loadReviewDashboardPayload();
	return (
		<LiveDashboard
			initialData={sanitizeDashboardPayloadForPublic(initialData)}
			showAdminPanel={false}
		/>
	);
}
