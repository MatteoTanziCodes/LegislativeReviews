"use client";

import { useMemo, useState } from "react";
import type {
	ReviewDetail,
} from "@/components/legislative-reviews/review-data";
import type {
	ReviewDecision,
} from "@/components/legislative-reviews/review-metrics";

type ReviewOutcomesPanelProps = {
	decisionCounts: Record<ReviewDecision, number>;
	reviews: ReviewDetail[];
};

const decisionCards = [
	{
		key: "repeal_candidate",
		label: "Repeal Candidate",
		tone: "bg-accent text-white border-transparent",
	},
	{
		key: "retain",
		label: "Retain",
		tone: "bg-surface-strong text-foreground border-border",
	},
	{
		key: "amend",
		label: "Amend",
		tone: "bg-surface-strong text-foreground border-border",
	},
	{
		key: "escalate",
		label: "Escalate",
		tone: "bg-background text-foreground border-border",
	},
] as const satisfies ReadonlyArray<{
	key: ReviewDecision;
	label: string;
	tone: string;
}>;

const numberFormatter = new Intl.NumberFormat("en-CA");

function formatDecisionConfidence(value: number) {
	return `${(value * 100).toFixed(0)}%`;
}

export function ReviewOutcomesPanel({
	decisionCounts,
	reviews,
}: ReviewOutcomesPanelProps) {
	const [selectedDecision, setSelectedDecision] = useState<ReviewDecision | null>(
		null,
	);

	const filteredReviews = useMemo(
		() =>
			selectedDecision === null
				? []
				: reviews.filter((review) => review.decision === selectedDecision),
		[reviews, selectedDecision],
	);

	return (
		<section className="py-8">
			<div className="rounded-[2rem] border border-border bg-surface p-6 shadow-[0_16px_48px_rgba(21,19,15,0.05)] sm:p-8">
				<div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
					<div>
						<p className="font-mono text-[0.68rem] uppercase tracking-[0.28em] text-muted">
							Current Outcomes
						</p>
						<h2 className="mt-3 font-display text-3xl tracking-[-0.06em] text-foreground">
							Reviewed decisions to date.
						</h2>
					</div>
					<p className="max-w-xl text-sm leading-7 text-muted">
						Select a decision to drill into reviewed bills and inspect the AI
						rationale behind each recommendation.
					</p>
				</div>

				<div className="mt-6 grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
					{decisionCards.map((card) => {
						const isActive = selectedDecision === card.key;
						return (
							<button
								key={card.key}
								type="button"
								onClick={() =>
									setSelectedDecision((currentDecision) =>
										currentDecision === card.key ? null : card.key,
									)
								}
								className={`rounded-[1.5rem] border px-5 py-6 text-left shadow-[0_12px_32px_rgba(21,19,15,0.05)] transition-transform duration-200 hover:-translate-y-0.5 ${card.tone} ${
									isActive
										? "ring-2 ring-accent-ink/35"
										: "ring-1 ring-transparent"
								}`}
								aria-pressed={isActive}
							>
								<p
									className={`font-mono text-[0.62rem] uppercase tracking-[0.24em] ${
										card.key === "repeal_candidate"
											? "text-white/70"
											: "text-muted"
									}`}
								>
									{card.label}
								</p>
								<p
									className={`mt-4 font-display text-5xl leading-none tracking-[-0.06em] ${
										card.key === "repeal_candidate"
											? "text-white"
											: "text-foreground"
									}`}
								>
									{numberFormatter.format(decisionCounts[card.key])}
								</p>
								<p
									className={`mt-4 font-mono text-[0.62rem] uppercase tracking-[0.24em] ${
										card.key === "repeal_candidate"
											? "text-white/70"
											: "text-muted"
									}`}
								>
									{isActive ? "Active Drilldown" : "Open Drilldown"}
								</p>
								{isActive ? (
									<p
										className={`mt-1 font-mono text-[0.58rem] uppercase tracking-[0.22em] ${
											card.key === "repeal_candidate"
												? "text-white/65"
												: "text-muted"
										}`}
									>
										Click again to close
									</p>
								) : null}
							</button>
						);
					})}
				</div>

				<div className="mt-8 rounded-[1.75rem] border border-border bg-background/70 p-5 sm:p-6">
					<div className="flex flex-col gap-3 border-b border-border pb-5 sm:flex-row sm:items-end sm:justify-between">
						<div>
							<p className="font-mono text-[0.62rem] uppercase tracking-[0.24em] text-muted">
								Selected Decision
							</p>
							<h3 className="mt-2 font-display text-3xl tracking-[-0.06em] text-foreground">
								{selectedDecision === null
									? "No drilldown selected"
									: (
									decisionCards.find((card) => card.key === selectedDecision)
										?.label
								)}
							</h3>
						</div>
						<p className="font-mono text-[0.68rem] uppercase tracking-[0.24em] text-muted">
							{selectedDecision === null
								? "Select a decision card to open"
								: `${numberFormatter.format(filteredReviews.length)} reviewed bills`}
						</p>
					</div>

					<div className="mt-6 grid gap-5">
						{selectedDecision === null ? (
							<div className="rounded-[1.5rem] border border-dashed border-border px-5 py-8 text-center">
								<p className="font-mono text-[0.68rem] uppercase tracking-[0.28em] text-muted">
									Open a decision bucket to inspect bills, evidence keys, and AI reasoning.
								</p>
							</div>
						) : filteredReviews.length === 0 ? (
							<div className="rounded-[1.5rem] border border-dashed border-border px-5 py-8 text-center">
								<p className="font-mono text-[0.68rem] uppercase tracking-[0.28em] text-muted">
									No reviewed bills in this decision bucket yet.
								</p>
							</div>
						) : (
							filteredReviews.map((review) => (
								<article
									key={review.documentId}
									className="rounded-[1.5rem] border border-border bg-surface px-5 py-5 shadow-[0_14px_36px_rgba(21,19,15,0.05)]"
								>
									<div className="flex flex-col gap-4 border-b border-border pb-4 lg:flex-row lg:items-start lg:justify-between">
										<div className="max-w-3xl">
											<p className="font-mono text-[0.62rem] uppercase tracking-[0.24em] text-muted">
												{review.documentId}
											</p>
											<h4 className="mt-2 font-display text-2xl tracking-[-0.05em] text-foreground">
												{review.titleEn}
											</h4>
											<p className="mt-2 text-sm leading-7 text-muted">
												{review.citationEn ?? "Citation unavailable"}
											</p>
											<div className="mt-4">
												<p className="font-mono text-[0.6rem] uppercase tracking-[0.22em] text-muted">
													Evidence Sections
												</p>
												<div className="mt-3 flex flex-wrap gap-2">
													{review.evidenceSectionKeys.length === 0 ? (
														<span className="rounded-full border border-border bg-background px-3 py-1.5 font-mono text-[0.62rem] uppercase tracking-[0.22em] text-muted">
															None cited
														</span>
													) : (
														review.evidenceSectionKeys.map((sectionKey) => (
															<span
																key={`${review.documentId}-${sectionKey}`}
																className="rounded-full border border-accent/20 bg-accent-soft px-3 py-1.5 font-mono text-[0.62rem] uppercase tracking-[0.22em] text-accent-ink"
															>
																Section {sectionKey}
															</span>
														))
													)}
												</div>
											</div>
										</div>
										<div className="grid gap-3 font-mono text-[0.62rem] uppercase tracking-[0.24em] text-muted sm:grid-cols-2 lg:min-w-[18rem]">
											<div>
												<p>Decision Confidence</p>
												<p className="mt-2 font-display text-2xl tracking-[-0.04em] text-foreground">
													{formatDecisionConfidence(review.decisionConfidence)}
												</p>
											</div>
											<div>
												<p>Evidence Count</p>
												<p className="mt-2 font-display text-2xl tracking-[-0.04em] text-foreground">
													{review.evidenceSectionKeys.length}
												</p>
											</div>
										</div>
									</div>

									<div className="mt-5">
										<div>
											<p className="font-mono text-[0.62rem] uppercase tracking-[0.24em] text-muted">
												AI Reasoning
											</p>
											<p className="mt-3 text-base leading-8 text-foreground/88">
												{review.rationale}
											</p>
										</div>
										<div className="mt-5 border-t border-border pt-4">
											<p className="font-mono text-[0.6rem] uppercase tracking-[0.2em] text-muted">
												Model
											</p>
											<p className="mt-2 text-sm text-foreground/80">
												{review.reviewModel}
											</p>
										</div>
									</div>
								</article>
							))
						)}
					</div>
				</div>
			</div>
		</section>
	);
}
