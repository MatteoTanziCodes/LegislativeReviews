import { NextResponse } from "next/server";
import { sanitizeDashboardPayloadForPublic } from "@/components/legislative-reviews/review-data";
import { loadReviewDashboardPayload } from "@/lib/legislative-review-storage";

export const dynamic = "force-dynamic";
export const revalidate = 0;

function jsonError(message: string, status: number) {
	return NextResponse.json(
		{ error: message },
		{
			status,
			headers: {
				"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
			},
		},
	);
}

export async function GET() {
	try {
		const payload = await loadReviewDashboardPayload();
		return NextResponse.json(sanitizeDashboardPayloadForPublic(payload), {
			headers: {
				"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
			},
		});
	} catch (error) {
		console.error("Unable to load legislative review dashboard data.", error);
		return jsonError("Unable to load legislative review dashboard data.", 500);
	}
}

export async function POST() {
	return jsonError("Review workflow controls are disabled in the hosted app.", 405);
}
