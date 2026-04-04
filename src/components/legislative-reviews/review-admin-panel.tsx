"use client";

import { useMemo, useState, useTransition } from "react";
import type { ReviewAdminState } from "@/components/legislative-reviews/review-data";
import type { ReviewSummary } from "@/components/legislative-reviews/review-metrics";

type ReviewAdminPanelProps = {
	adminState: ReviewAdminState;
	onLogout: () => Promise<void>;
	onRequestRun: (payload: {
		domain: string;
		limit: number | null;
	}) => Promise<void>;
	summary: ReviewSummary;
};

const DOMAIN_OPTIONS = [
	"transport_infrastructure",
	"governance_administrative",
	"environment_resources",
	"business_commerce",
	"tax_finance",
	"criminal_public_safety",
	"health_social_services",
	"indigenous_crown_relations",
	"labor_employment",
	"rights_privacy_access",
	"other",
] as const;

function formatDomainLabel(domain: string) {
	return domain
		.split("_")
		.map((segment) => segment.charAt(0).toUpperCase() + segment.slice(1))
		.join(" ");
}

function formatOptionalTimestamp(value?: string | null) {
	if (!value) {
		return "Unavailable";
	}

	const parsed = new Date(value);
	if (Number.isNaN(parsed.getTime())) {
		return value;
	}

	return parsed.toLocaleString("en-CA", {
		dateStyle: "medium",
		timeStyle: "short",
	});
}

function deriveWorkerStatusPresentation(adminState: ReviewAdminState) {
	if (adminState.workerStatus === "error") {
		return {
			description: "The most recent GitHub Actions review run reported an error.",
			label: "Run failed",
			tone: "danger" as const,
		};
	}

	if (adminState.workerStatus === "running") {
		return {
			description: "A review batch is actively running in GitHub Actions.",
			label: "Run in progress",
			tone: "accent" as const,
		};
	}

	if (adminState.workerStatus === "pending") {
		return {
			description: "A review request has been dispatched and is waiting for a GitHub runner to start.",
			label: "Run queued",
			tone: "accent" as const,
		};
	}

	if (adminState.workerStatus === "complete") {
		return {
			description: "The most recent GitHub Actions review run completed successfully.",
			label: "Run complete",
			tone: "muted" as const,
		};
	}

	return {
		description: "No active GitHub Actions review run is in progress right now.",
		label: "Idle",
		tone: "muted" as const,
	};
}

export function ReviewAdminPanel({
	adminState,
	onLogout,
	onRequestRun,
	summary,
}: ReviewAdminPanelProps) {
	const [domain, setDomain] = useState<string>(
		adminState.currentDomain ?? "transport_infrastructure",
	);
	const [limitInput, setLimitInput] = useState("");
	const [feedback, setFeedback] = useState<string | null>(null);
	const [feedbackTone, setFeedbackTone] = useState<"muted" | "danger">("muted");
	const [isPending, startTransition] = useTransition();

	const workerStatus = useMemo(
		() => deriveWorkerStatusPresentation(adminState),
		[adminState],
	);
	const toneClasses = {
		accent: "border-accent/25 bg-accent-soft text-accent-ink",
		danger: "border-[#b2483d]/25 bg-[#f0ddda] text-[#8d261e]",
		muted: "border-border bg-surface-strong text-foreground/78",
	}[workerStatus.tone];

	return (
		<section className="py-8">
			<div className="rounded-[2rem] border border-border bg-surface p-6 shadow-[0_16px_48px_rgba(21,19,15,0.05)] sm:p-8">
				<div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
					<div className="min-w-0">
						<p className="font-mono text-[0.68rem] uppercase tracking-[0.28em] text-muted">
							Workflow Admin
						</p>
						<h2 className="mt-3 max-w-2xl text-balance font-display text-2xl tracking-[-0.06em] text-foreground sm:text-3xl">
							Control and audit the GitHub Actions review workflow.
						</h2>
					</div>
					<div className="flex flex-wrap items-center gap-3">
						<div className={`rounded-full border px-4 py-3 font-mono text-[0.62rem] uppercase tracking-[0.24em] ${toneClasses}`}>
							{workerStatus.label}
						</div>
						{adminState.lastRunHtmlUrl ? (
							<a
								href={adminState.lastRunHtmlUrl}
								target="_blank"
								rel="noreferrer"
								className="rounded-full border border-border bg-background px-4 py-3 font-mono text-[0.62rem] uppercase tracking-[0.24em] text-muted transition hover:border-accent/40 hover:text-accent-ink"
							>
								Open Latest Run
							</a>
						) : null}
						<button
							type="button"
							onClick={() => {
								startTransition(async () => {
									try {
										await onLogout();
									} catch (error) {
										setFeedbackTone("danger");
										setFeedback(
											error instanceof Error
												? error.message
												: "Unable to close the admin session.",
										);
									}
								});
							}}
							className="rounded-full border border-border bg-background px-4 py-3 font-mono text-[0.62rem] uppercase tracking-[0.24em] text-muted transition hover:border-accent/40 hover:text-accent-ink"
						>
							Log Out
						</button>
					</div>
				</div>

				<div className="mt-4 rounded-[1.5rem] border border-border bg-background/65 px-5 py-4 text-sm leading-7 text-muted">
					{workerStatus.description}
				</div>

				<div className="mt-6 grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
					<div className="min-w-0 rounded-[1.5rem] border border-border bg-background/65 px-5 py-5">
						<p className="font-mono text-[0.62rem] uppercase tracking-[0.24em] text-muted">
							Active Domain
						</p>
						<p className="mt-3 break-words font-display text-xl tracking-[-0.05em] text-foreground sm:text-2xl">
							{adminState.currentDomain
								? formatDomainLabel(adminState.currentDomain)
								: "No active domain"}
						</p>
					</div>
					<div className="min-w-0 rounded-[1.5rem] border border-border bg-background/65 px-5 py-5">
						<p className="font-mono text-[0.62rem] uppercase tracking-[0.24em] text-muted">
							Last Status Update
						</p>
						<p className="mt-3 text-balance font-display text-xl tracking-[-0.05em] text-foreground sm:text-2xl">
							{formatOptionalTimestamp(adminState.lastHeartbeatAt)}
						</p>
					</div>
					<div className="min-w-0 rounded-[1.5rem] border border-border bg-background/65 px-5 py-5">
						<p className="font-mono text-[0.62rem] uppercase tracking-[0.24em] text-muted">
							Last Completed
						</p>
						<p className="mt-3 text-balance font-display text-xl tracking-[-0.05em] text-foreground sm:text-2xl">
							{formatOptionalTimestamp(adminState.lastCompletedAt)}
						</p>
					</div>
					<div className="min-w-0 rounded-[1.5rem] border border-border bg-background/65 px-5 py-5">
						<p className="font-mono text-[0.62rem] uppercase tracking-[0.24em] text-muted">
							Reviewed Decisions
						</p>
						<p className="mt-3 font-display text-xl tracking-[-0.05em] text-foreground sm:text-2xl">
							{summary.reviewedCount}
						</p>
					</div>
				</div>

				<div className="mt-8 grid gap-6 lg:grid-cols-[minmax(0,0.95fr)_minmax(0,1.05fr)]">
					<form
						className="min-w-0 rounded-[1.75rem] border border-border bg-background/70 p-5 sm:p-6"
						onSubmit={(event) => {
							event.preventDefault();
							setFeedback(null);

							const normalizedLimit = limitInput.trim();
							const parsedLimit =
								normalizedLimit.length === 0 ? null : Number.parseInt(normalizedLimit, 10);
							if (
								normalizedLimit.length > 0 &&
								(parsedLimit === null || !Number.isInteger(parsedLimit) || parsedLimit <= 0)
							) {
								setFeedbackTone("danger");
								setFeedback("Limit must be a positive integer.");
								return;
							}

							startTransition(async () => {
								try {
									await onRequestRun({
										domain,
										limit: parsedLimit,
									});
									setFeedbackTone("muted");
									setFeedback(
										`GitHub Actions workflow dispatched for ${formatDomainLabel(domain)}.`,
									);
								} catch (error) {
									setFeedbackTone("danger");
									setFeedback(
										error instanceof Error
											? error.message
											: "Unable to submit the review request.",
									);
								}
							});
						}}
					>
						<div>
							<p className="font-mono text-[0.62rem] uppercase tracking-[0.24em] text-muted">
								Request a Review Run
							</p>
							<h3 className="mt-2 max-w-xl text-balance font-display text-xl tracking-[-0.05em] text-foreground sm:text-2xl">
								Dispatch the next GitHub Actions batch from the dashboard.
							</h3>
						</div>

						<div className="mt-5 grid gap-4">
							<label className="grid gap-2">
								<span className="font-mono text-[0.62rem] uppercase tracking-[0.24em] text-muted">
									Domain
								</span>
								<select
									value={domain}
									onChange={(event) => setDomain(event.target.value)}
									className="rounded-2xl border border-border bg-surface px-4 py-3 text-sm text-foreground outline-none transition focus:border-accent/50"
								>
									{DOMAIN_OPTIONS.map((option) => (
										<option key={option} value={option}>
											{formatDomainLabel(option)}
										</option>
									))}
								</select>
							</label>

							<label className="grid gap-2">
								<span className="font-mono text-[0.62rem] uppercase tracking-[0.24em] text-muted">
									Optional Limit
								</span>
								<input
									type="number"
									min={1}
									step={1}
									value={limitInput}
									onChange={(event) => setLimitInput(event.target.value)}
									className="rounded-2xl border border-border bg-surface px-4 py-3 text-sm text-foreground outline-none transition focus:border-accent/50"
									placeholder="Leave blank for full domain batch"
								/>
							</label>
						</div>

						<button
							type="submit"
							disabled={isPending}
							className="mt-6 inline-flex items-center justify-center rounded-full bg-accent px-5 py-3 font-mono text-[0.66rem] uppercase tracking-[0.24em] text-white transition hover:bg-accent-ink disabled:cursor-not-allowed disabled:opacity-60"
						>
							{isPending ? "Submitting Request" : "Request Review Run"}
						</button>

						{feedback ? (
							<p
								className={`mt-4 text-sm leading-7 ${
									feedbackTone === "danger" ? "text-[#8d261e]" : "text-muted"
								}`}
							>
								{feedback}
							</p>
						) : null}
					</form>

					<div className="min-w-0 rounded-[1.75rem] border border-border bg-background/70 p-5 sm:p-6">
						<div className="flex flex-col gap-2 border-b border-border pb-4">
							<p className="font-mono text-[0.62rem] uppercase tracking-[0.24em] text-muted">
								Audit Trail
							</p>
							<h3 className="max-w-xl text-balance font-display text-xl tracking-[-0.05em] text-foreground sm:text-2xl">
								Recent automation activity and operator actions.
							</h3>
							<p className="font-mono text-[0.58rem] uppercase tracking-[0.22em] text-muted">
								Most recent first. Scroll for older events.
							</p>
						</div>

						<div className="mt-5 max-h-[16rem] overflow-y-auto pr-2 sm:max-h-[18rem]">
							<div className="grid gap-4">
							{adminState.recentEvents.length === 0 ? (
								<div className="rounded-[1.25rem] border border-dashed border-border px-4 py-6 text-sm text-muted">
									No workflow events have been recorded yet. Dispatch a run to begin
									collecting GitHub Actions audit history.
								</div>
							) : (
								adminState.recentEvents.map((event, index) => (
									<div
										key={`${event.timestamp}-${event.message}-${index}`}
										className="min-w-0 rounded-[1.25rem] border border-border bg-surface px-4 py-4"
									>
										<div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
											<p className="font-mono text-[0.6rem] uppercase tracking-[0.22em] text-muted">
												{event.level === "error" ? "Error" : "Event"}
											</p>
											<p className="font-mono text-[0.6rem] uppercase tracking-[0.22em] text-muted">
												{formatOptionalTimestamp(event.timestamp)}
											</p>
										</div>
										<p className="mt-3 break-words text-sm leading-7 text-foreground/84">
											{event.message}
										</p>
									</div>
								))
							)}
							</div>
						</div>
					</div>
				</div>
			</div>
		</section>
	);
}
