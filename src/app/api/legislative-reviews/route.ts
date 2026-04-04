import { NextRequest, NextResponse } from "next/server";
import type {
	ReviewAdminEvent,
	ReviewAdminState,
	ReviewRunRequest,
} from "@/components/legislative-reviews/review-data";
import { sanitizeDashboardPayloadForPublic } from "@/components/legislative-reviews/review-data";
import { isAdminAuthenticated, verifyAdminToken } from "@/lib/review-admin-auth";
import {
	dispatchReviewWorkflow,
	ReviewWorkflowConfigError,
} from "@/lib/review-github-actions";
import {
	loadReviewAdminState,
	loadReviewDashboardPayload,
	persistReviewAdminState,
} from "@/lib/legislative-review-storage";

export const dynamic = "force-dynamic";
export const revalidate = 0;

const ALLOWED_DOMAINS = [
	"business_commerce",
	"criminal_public_safety",
	"environment_resources",
	"governance_administrative",
	"health_social_services",
	"indigenous_crown_relations",
	"labor_employment",
	"other",
	"rights_privacy_access",
	"tax_finance",
	"transport_infrastructure",
] as const;

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

function appendAdminEvent(
	adminState: ReviewAdminState,
	event: ReviewAdminEvent,
): ReviewAdminState {
	return {
		...adminState,
		recentEvents: [event, ...adminState.recentEvents].slice(0, 12),
	};
}

function validateRequestedDomain(candidate: unknown): string | null {
	if (typeof candidate !== "string") {
		return null;
	}

	return ALLOWED_DOMAINS.includes(
		candidate as (typeof ALLOWED_DOMAINS)[number],
	)
		? candidate
		: null;
}

export async function GET() {
	try {
		const payload = await loadReviewDashboardPayload();
		const isAdmin = await isAdminAuthenticated();
		return NextResponse.json(isAdmin ? payload : sanitizeDashboardPayloadForPublic(payload), {
			headers: {
				"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
			},
		});
	} catch (error) {
		console.error("Unable to load legislative review dashboard data.", error);
		return jsonError("Unable to load legislative review dashboard data.", 500);
	}
}

export async function POST(request: NextRequest) {
	try {
		const sessionAuthenticated = await isAdminAuthenticated();
		const headerToken = request.headers.get("x-review-admin-token")?.trim();
		const headerAuthenticated = headerToken
			? await verifyAdminToken(headerToken)
			: false;
		if (!sessionAuthenticated && !headerAuthenticated) {
			return jsonError("Unauthorized.", 401);
		}

		const payload = (await request.json()) as {
			action?: string;
			domain?: unknown;
			limit?: unknown;
		};
		if (payload.action !== "request_run") {
			return jsonError("Unsupported admin action.", 400);
		}

		const domain = validateRequestedDomain(payload.domain);
		if (!domain) {
			return jsonError("A valid review domain is required.", 400);
		}

		let limit: number | null = null;
		if (payload.limit !== undefined && payload.limit !== null && payload.limit !== "") {
			if (
				typeof payload.limit !== "number" ||
				!Number.isInteger(payload.limit) ||
				payload.limit <= 0
			) {
				return jsonError("Limit must be a positive integer.", 400);
			}
			limit = payload.limit;
		}

		const adminState = await loadReviewAdminState();
		const busy =
			adminState.workerStatus === "pending" ||
			adminState.workerStatus === "running";
		if (busy) {
			return jsonError(
				"A review workflow is already pending or active. Wait for it to finish before requesting another batch.",
				409,
			);
		}

		const now = new Date().toISOString();
		const nextRequest: ReviewRunRequest = {
			commandId: crypto.randomUUID(),
			domain,
			limit,
			requestedAt: now,
			requestedBy: "dashboard",
			status: "pending",
		};

		let dispatchResult;
		try {
			dispatchResult = await dispatchReviewWorkflow({
				commandId: nextRequest.commandId,
				domain,
				limit,
				requestedAt: now,
			});
		} catch (error) {
			const message =
				error instanceof Error
					? error.message
					: "Unable to dispatch the review workflow.";
			const nextAdminState = appendAdminEvent(
				{
					...adminState,
					lastError: message,
					lastRequestedAt: now,
					workerStatus: "error",
				},
				{
					level: "error",
					message: `GitHub Actions dispatch failed for ${domain}${limit ? ` (limit ${limit})` : ""}. ${message}`,
					timestamp: now,
				},
			);
			await persistReviewAdminState(nextAdminState);
			return jsonError(
				message,
				error instanceof ReviewWorkflowConfigError ? 503 : 502,
			);
		}

		const nextAdminState = appendAdminEvent(
			{
				...adminState,
				currentDomain: domain,
				currentLimit: limit,
				lastCommand: nextRequest,
				lastError: null,
				lastRequestedAt: now,
				lastRunHtmlUrl: dispatchResult.htmlUrl ?? adminState.lastRunHtmlUrl ?? null,
				lastRunId: dispatchResult.workflowRunId ?? adminState.lastRunId ?? null,
				workerStatus: "pending",
			},
			{
				level: "info",
				message: `GitHub Actions review workflow dispatched for ${domain}${limit ? ` (limit ${limit})` : ""}.`,
				timestamp: now,
			},
		);

		await persistReviewAdminState(nextAdminState);

		return NextResponse.json(
			{
				adminState: nextAdminState,
				ok: true,
				request: nextRequest,
			},
			{
				headers: {
					"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
				},
			},
		);
	} catch (error) {
		console.error("Unable to process legislative review admin action.", error);
		return jsonError("Unable to process legislative review admin action.", 500);
	}
}
