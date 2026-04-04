import type { Metadata } from "next";
import { LiveDashboard } from "@/components/legislative-reviews/live-dashboard";
import { ReviewAdminLogin } from "@/components/legislative-reviews/review-admin-login";
import { isAdminAuthenticated } from "@/lib/review-admin-auth";
import { loadReviewDashboardPayload } from "@/lib/legislative-review-storage";

export const metadata: Metadata = {
	title: "Legislative Reviews Admin | Build Canada",
	description: "Admin workflow controls for the legislative review pipeline.",
};

export const dynamic = "force-dynamic";
export const revalidate = 0;

export default async function LegislativeReviewsAdminPage() {
	const authenticated = await isAdminAuthenticated();
	if (!authenticated) {
		return <ReviewAdminLogin />;
	}

	const initialData = await loadReviewDashboardPayload();
	return <LiveDashboard initialData={initialData} showAdminPanel />;
}
