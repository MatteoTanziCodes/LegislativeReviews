"use client";

import Image from "next/image";
import Link from "next/link";
import { useEffect, useMemo, useState, useTransition } from "react";
import { ProgressDonut } from "@/components/legislative-reviews/progress-donut";
import type {
	ReviewDashboardPayload,
} from "@/components/legislative-reviews/review-data";
import { ProgressRail } from "@/components/legislative-reviews/progress-rail";
import { ReviewAdminPanel } from "@/components/legislative-reviews/review-admin-panel";
import { ReviewSignalsSummary } from "@/components/legislative-reviews/review-signals-summary";
import { ReviewOutcomesPanel } from "@/components/legislative-reviews/review-outcomes-panel";
import {
	deriveReviewProgressMetrics,
	deriveReviewRunStatus,
	normalizeDecisionCounts,
	type ReviewRunStatus,
} from "@/components/legislative-reviews/review-metrics";

const DEFAULT_DAILY_CAPACITY = 200;
const POLL_INTERVAL_MS = 5000;

async function fetchReviewDashboardPayload(): Promise<ReviewDashboardPayload> {
	const response = await fetch("/api/legislative-reviews", {
		cache: "no-store",
	});

	if (!response.ok) {
		throw new Error(`Dashboard sync failed with status ${response.status}.`);
	}

	return (await response.json()) as ReviewDashboardPayload;
}

async function requestReviewRun(payload: {
	domain: string;
	limit: number | null;
}): Promise<void> {
	const response = await fetch("/api/legislative-reviews", {
		method: "POST",
		headers: {
			"Content-Type": "application/json",
		},
		body: JSON.stringify({
			action: "request_run",
			domain: payload.domain,
			limit: payload.limit,
		}),
	});

	if (!response.ok) {
		const errorPayload = (await response.json().catch(() => null)) as
			| { error?: string }
			| null;
		throw new Error(
			errorPayload?.error ?? `Review request failed with status ${response.status}.`,
		);
	}
}

async function logoutAdminSession(): Promise<void> {
	const response = await fetch("/api/legislative-reviews/admin/session", {
		method: "DELETE",
	});
	if (!response.ok) {
		throw new Error("Unable to close admin session.");
	}
	window.location.href = "/legislative-reviews/admin";
}

export function LiveDashboard({
	initialData,
	showAdminPanel = false,
}: {
	initialData: ReviewDashboardPayload;
	showAdminPanel?: boolean;
}) {
	const [dashboardData, setDashboardData] =
		useState<ReviewDashboardPayload>(initialData);
	const [syncError, setSyncError] = useState<string | null>(null);
	const [, startTransition] = useTransition();

	useEffect(() => {
		let isCancelled = false;
		let nextPollId: number | undefined;

		const poll = async () => {
			try {
				const nextData = await fetchReviewDashboardPayload();
				if (isCancelled) {
					return;
				}

				startTransition(() => {
					setDashboardData(nextData);
				});
				setSyncError(null);
			} catch (error) {
				if (!isCancelled) {
					setSyncError(
						error instanceof Error
							? error.message
							: "Dashboard sync failed.",
					);
				}
			} finally {
				if (!isCancelled) {
					nextPollId = window.setTimeout(poll, POLL_INTERVAL_MS);
				}
			}
		};

		void poll();

		return () => {
			isCancelled = true;
			if (nextPollId !== undefined) {
				window.clearTimeout(nextPollId);
			}
		};
	}, [startTransition]);

	const metrics = deriveReviewProgressMetrics({
		totalCount: dashboardData.summary.totalCount,
		reviewedCount: dashboardData.summary.reviewedCount,
		dailyCapacity:
			dashboardData.summary.dailyCapacity ?? DEFAULT_DAILY_CAPACITY,
	});
	const decisionCounts = normalizeDecisionCounts(
		dashboardData.summary.decisionCounts,
	);
	const reviewRunStatus = useMemo<ReviewRunStatus>(() => {
		if (syncError) {
			return {
				label: "Live sync stalled",
				pulse: false,
				tone: "danger",
			};
		}

		if (dashboardData.adminState.workerStatus === "running") {
			return {
				label: "Reviews in progress",
				pulse: true,
				tone: "accent",
			};
		}

		if (dashboardData.adminState.workerStatus === "pending") {
			return {
				label: "Review request queued",
				pulse: true,
				tone: "accent",
			};
		}

		if (dashboardData.adminState.workerStatus === "error") {
			return {
				label: "Review worker error",
				pulse: false,
				tone: "danger",
			};
		}

		return deriveReviewRunStatus(dashboardData.summary);
	}, [dashboardData.adminState.workerStatus, dashboardData.summary, syncError]);
	const statusToneClasses = {
		accent: {
			dot: "bg-accent",
			ping: "bg-accent/35",
			text: "text-accent-ink",
		},
		muted: {
			dot: "bg-foreground/55",
			ping: "bg-foreground/15",
			text: "text-foreground/80",
		},
		danger: {
			dot: "bg-[#b2483d]",
			ping: "bg-[#b2483d]/30",
			text: "text-[#8d261e]",
		},
	}[reviewRunStatus.tone];

	return (
		<main className="relative min-h-screen overflow-hidden bg-background text-foreground">
			<div className="absolute inset-0 bg-[radial-gradient(circle_at_top_left,rgba(139,35,50,0.17),transparent_34%),radial-gradient(circle_at_bottom_right,rgba(147,47,47,0.12),transparent_32%)]" />
			<div className="absolute inset-0 opacity-55 [background-image:linear-gradient(to_right,rgba(21,19,15,0.08)_1px,transparent_1px),linear-gradient(to_bottom,rgba(21,19,15,0.08)_1px,transparent_1px)] [background-size:36px_36px]" />

			<div className="relative mx-auto flex min-h-screen w-full max-w-7xl flex-col px-6 py-8 sm:px-10 lg:px-14 lg:py-12">
				<header className="flex flex-col gap-4 border-b border-border pb-6 sm:flex-row sm:flex-wrap sm:items-center sm:justify-between">
					<div className="flex items-center">
						<Link
							href="https://buildcanada.com"
							className="block shrink-0"
							aria-label="Build Canada"
						>
							<span className="flex items-center bg-[#932f2f] px-4 py-3 shadow-[0_12px_28px_rgba(114,28,40,0.16)]">
								<Image
									src="/build-canada-wordmark.svg"
									alt="Build Canada"
									width={86}
									height={40}
									className="h-7 w-auto sm:h-8"
									priority
								/>
							</span>
						</Link>
					</div>

					<div className="flex flex-col gap-2 text-left sm:items-end sm:text-right">
						<div className="inline-flex items-center gap-3 rounded-full border border-border bg-surface px-4 py-3 font-mono text-[0.68rem] uppercase tracking-[0.28em] shadow-[0_12px_28px_rgba(114,28,40,0.08)]">
							<span className="relative flex h-3 w-3">
								<span
									className={`absolute inline-flex h-full w-full rounded-full ${statusToneClasses.ping} ${
										reviewRunStatus.pulse ? "animate-ping" : ""
									}`}
								/>
								<span
									className={`relative inline-flex h-3 w-3 rounded-full ${statusToneClasses.dot}`}
								/>
							</span>
							<span className={statusToneClasses.text}>
								{reviewRunStatus.label}
							</span>
						</div>
						{dashboardData.summary.lastUpdated ? (
							<p className="font-mono text-[0.68rem] uppercase tracking-[0.28em] text-muted">
								Last Updated: {dashboardData.summary.lastUpdated}
							</p>
						) : null}
						{syncError ? (
							<p className="font-mono text-[0.64rem] uppercase tracking-[0.24em] text-[#8d261e]">
								Showing last successful snapshot.
							</p>
						) : null}
					</div>
				</header>

				<section className="grid gap-10 border-b border-border py-10 lg:grid-cols-[minmax(0,1.2fr)_minmax(18rem,24rem)] lg:items-start lg:gap-14">
					<div className="max-w-4xl">
						<p className="font-mono text-[0.68rem] uppercase tracking-[0.28em] text-muted">
							Legislative Review Tracker
						</p>
						<h1 className="mt-5 font-display text-[clamp(3.4rem,9vw,6.7rem)] leading-[0.92] tracking-[-0.08em] text-foreground">
							The Canadian legislative corpus,
							<span className="mt-2 block font-sans text-[0.83em] italic tracking-[-0.06em] text-foreground/78">
								reviewed for modernization.
							</span>
						</h1>
						<p className="mt-6 max-w-2xl text-pretty text-lg leading-8 text-muted sm:text-xl">
							A rolling review of federal legislation using a prosperity-first
							mandate. Documents are processed sequentially and evaluated as
							retain, amend, repeal candidate, or escalate.
						</p>
					</div>

					<ProgressDonut
						percentReviewed={metrics.percentReviewed}
						reviewedCount={metrics.reviewedCount}
						remainingCount={metrics.remainingCount}
					/>
				</section>

				{showAdminPanel ? (
					<ReviewAdminPanel
						adminState={dashboardData.adminState}
						onLogout={logoutAdminSession}
						onRequestRun={async (payload) => {
							await requestReviewRun(payload);
							const refreshed = await fetchReviewDashboardPayload();
							startTransition(() => {
								setDashboardData(refreshed);
							});
							setSyncError(null);
						}}
						summary={dashboardData.summary}
					/>
				) : null}

				<ReviewOutcomesPanel
					decisionCounts={decisionCounts}
					reviews={dashboardData.reviews}
				/>

				<ReviewSignalsSummary reviews={dashboardData.reviews} />

				<div className="pt-8">
					<ProgressRail
						dailyCapacity={metrics.dailyCapacity}
						estimatedDaysRemaining={metrics.estimatedDaysRemaining}
						percentReviewed={metrics.percentReviewed}
						reviewedCount={metrics.reviewedCount}
						totalCount={metrics.totalCount}
					/>
				</div>
			</div>
		</main>
	);
}
