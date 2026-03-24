import { readFile } from "node:fs/promises";
import path from "node:path";
import { NextResponse } from "next/server";
import type {
	ReviewDashboardPayload,
	ReviewDetail,
} from "@/components/legislative-reviews/review-data";
import type { ReviewSummary } from "@/components/legislative-reviews/review-metrics";

export const dynamic = "force-dynamic";
export const revalidate = 0;

const SUMMARY_PATH = path.join(process.cwd(), "src", "data", "review-summary.json");
const DETAILS_PATH = path.join(process.cwd(), "src", "data", "review-details.json");
const READ_ATTEMPTS = 3;
const READ_RETRY_DELAY_MS = 75;

function delay(ms: number) {
	return new Promise((resolve) => {
		setTimeout(resolve, ms);
	});
}

async function loadDashboardPayload(): Promise<ReviewDashboardPayload> {
	let lastError: unknown;

	for (let attempt = 1; attempt <= READ_ATTEMPTS; attempt += 1) {
		try {
			const [summaryText, detailsText] = await Promise.all([
				readFile(SUMMARY_PATH, "utf-8"),
				readFile(DETAILS_PATH, "utf-8"),
			]);
			const summary = JSON.parse(summaryText) as ReviewSummary;
			const reviews = JSON.parse(detailsText) as ReviewDetail[];

			if (summary.reviewedCount === reviews.length || attempt === READ_ATTEMPTS) {
				return {
					summary,
					reviews,
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

export async function GET() {
	try {
		const payload = await loadDashboardPayload();
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
