import { NextResponse } from "next/server";
import { loadReviewDashboardPayload } from "@/lib/legislative-review-storage";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export async function GET() {
	try {
		const payload = await loadReviewDashboardPayload();
		return NextResponse.json(payload, {
			headers: {
				"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
			},
		});
	} catch (error) {
		const message =
			error instanceof Error
				? error.message
				: "Unable to load legislative review dashboard data.";

		return NextResponse.json(
			{ error: message },
			{
				status: 500,
				headers: {
					"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
				},
			},
		);
	}
}
