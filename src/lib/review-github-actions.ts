import { getCloudflareContext } from "@opennextjs/cloudflare";

const DEFAULT_WORKFLOW_ID = "review-pipeline.yml";
const DEFAULT_WORKFLOW_REF = "main";
const DEFAULT_WORKFLOW_ENVIRONMENT = "production";
const GITHUB_API_VERSION = "2026-03-10";

type ReviewGitHubWorkflowEnv = CloudflareEnv & {
	GITHUB_REVIEW_WORKFLOW_ENVIRONMENT?: string;
	GITHUB_REVIEW_WORKFLOW_ID?: string;
	GITHUB_REVIEW_WORKFLOW_OWNER?: string;
	GITHUB_REVIEW_WORKFLOW_REF?: string;
	GITHUB_REVIEW_WORKFLOW_REPO?: string;
	GITHUB_REVIEW_WORKFLOW_TOKEN?: string;
};

type ReviewWorkflowDispatchConfig = {
	environment: string;
	owner: string;
	ref: string;
	repo: string;
	token: string;
	workflowId: string;
};

export class ReviewWorkflowConfigError extends Error {}

export type ReviewWorkflowDispatchResult = {
	htmlUrl: string | null;
	workflowRunId: number | null;
};

async function getReviewWorkflowEnv(): Promise<ReviewGitHubWorkflowEnv | null> {
	if (process.env.NODE_ENV === "development") {
		return null;
	}

	try {
		const { env } = await getCloudflareContext({ async: true });
		return env as ReviewGitHubWorkflowEnv;
	} catch {
		return null;
	}
}

function trimToNull(value: string | null | undefined) {
	const normalized = value?.trim();
	return normalized ? normalized : null;
}

export async function getReviewWorkflowDispatchConfig(): Promise<ReviewWorkflowDispatchConfig> {
	const env = await getReviewWorkflowEnv();

	const owner =
		trimToNull(env?.GITHUB_REVIEW_WORKFLOW_OWNER) ??
		trimToNull(process.env.GITHUB_REVIEW_WORKFLOW_OWNER);
	const repo =
		trimToNull(env?.GITHUB_REVIEW_WORKFLOW_REPO) ??
		trimToNull(process.env.GITHUB_REVIEW_WORKFLOW_REPO);
	const workflowId =
		trimToNull(env?.GITHUB_REVIEW_WORKFLOW_ID) ??
		trimToNull(process.env.GITHUB_REVIEW_WORKFLOW_ID) ??
		DEFAULT_WORKFLOW_ID;
	const ref =
		trimToNull(env?.GITHUB_REVIEW_WORKFLOW_REF) ??
		trimToNull(process.env.GITHUB_REVIEW_WORKFLOW_REF) ??
		DEFAULT_WORKFLOW_REF;
	const environment =
		trimToNull(env?.GITHUB_REVIEW_WORKFLOW_ENVIRONMENT) ??
		trimToNull(process.env.GITHUB_REVIEW_WORKFLOW_ENVIRONMENT) ??
		DEFAULT_WORKFLOW_ENVIRONMENT;
	const token =
		trimToNull(env?.GITHUB_REVIEW_WORKFLOW_TOKEN) ??
		trimToNull(process.env.GITHUB_REVIEW_WORKFLOW_TOKEN);

	const missing: string[] = [];
	if (!owner) {
		missing.push("GITHUB_REVIEW_WORKFLOW_OWNER");
	}
	if (!repo) {
		missing.push("GITHUB_REVIEW_WORKFLOW_REPO");
	}
	if (!token) {
		missing.push("GITHUB_REVIEW_WORKFLOW_TOKEN");
	}

	if (missing.length > 0) {
		throw new ReviewWorkflowConfigError(
			`GitHub review workflow dispatch is not configured. Missing: ${missing.join(", ")}.`,
		);
	}

	return {
		environment,
		owner: owner!,
		ref,
		repo: repo!,
		token: token!,
		workflowId,
	};
}

function buildDispatchErrorMessage(
	status: number,
	payload: unknown,
): string {
	if (!payload || typeof payload !== "object") {
		return `GitHub workflow dispatch failed with status ${status}.`;
	}

	const message =
		typeof (payload as { message?: unknown }).message === "string"
			? (payload as { message: string }).message.trim()
			: "";
	const errors = Array.isArray((payload as { errors?: unknown }).errors)
		? (payload as { errors: unknown[] }).errors
				.map((entry) =>
					typeof entry === "string"
						? entry
						: typeof entry === "object" &&
							  entry !== null &&
							  typeof (entry as { message?: unknown }).message === "string"
							? (entry as { message: string }).message
							: null,
				  )
				.filter((entry): entry is string => Boolean(entry))
		: [];

	const details = [message, ...errors].filter(Boolean).join(" ");
	return details || `GitHub workflow dispatch failed with status ${status}.`;
}

export async function dispatchReviewWorkflow(payload: {
	commandId: string;
	domain: string;
	limit: number | null;
	requestedAt: string;
}): Promise<ReviewWorkflowDispatchResult> {
	const config = await getReviewWorkflowDispatchConfig();

	const dispatchUrl = new URL(
		`https://api.github.com/repos/${config.owner}/${config.repo}/actions/workflows/${config.workflowId}/dispatches`,
	);
	dispatchUrl.searchParams.set("return_run_details", "true");

	const response = await fetch(dispatchUrl, {
		method: "POST",
		headers: {
			Accept: "application/vnd.github+json",
			Authorization: `Bearer ${config.token}`,
			"Content-Type": "application/json",
			"X-GitHub-Api-Version": GITHUB_API_VERSION,
		},
		body: JSON.stringify({
			ref: config.ref,
			inputs: {
				command_id: payload.commandId,
				deployment_environment: config.environment,
				domain: payload.domain,
				limit: payload.limit !== null ? String(payload.limit) : "",
				requested_at: payload.requestedAt,
			},
		}),
	});

	if (!response.ok) {
		const errorPayload = await response.json().catch(() => null);
		throw new Error(buildDispatchErrorMessage(response.status, errorPayload));
	}

	if (response.status === 204) {
		return {
			htmlUrl: null,
			workflowRunId: null,
		};
	}

	const responsePayload = (await response.json().catch(() => null)) as
		| {
				html_url?: unknown;
				workflow_run_id?: unknown;
		  }
		| null;

	return {
		htmlUrl:
			responsePayload && typeof responsePayload.html_url === "string"
				? responsePayload.html_url
				: null,
		workflowRunId:
			responsePayload && typeof responsePayload.workflow_run_id === "number"
				? responsePayload.workflow_run_id
				: null,
	};
}
