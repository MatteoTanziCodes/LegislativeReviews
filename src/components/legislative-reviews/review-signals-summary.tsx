import type { ReviewDetail } from "@/components/legislative-reviews/review-data";

type ReviewSignalsSummaryProps = {
	reviews: ReviewDetail[];
};

type SignalCard = {
	description: string;
	key:
		| "operationalRelevanceScore"
		| "prosperityAlignmentScore"
		| "administrativeBurdenScore"
		| "repealRiskScore";
	label: string;
	scale: string;
};

const signalCards: SignalCard[] = [
	{
		key: "operationalRelevanceScore",
		label: "Operational Relevance",
		scale: "0-3 scale",
		description:
			"How clearly the reviewed law appears to govern a live federal program, institution, or regulatory regime.",
	},
	{
		key: "prosperityAlignmentScore",
		label: "Prosperity Alignment",
		scale: "-2 to 2 scale",
		description:
			"Whether the law appears to support productivity, competitiveness, and investment rather than act as a drag.",
	},
	{
		key: "administrativeBurdenScore",
		label: "Administrative Burden",
		scale: "0-3 scale",
		description:
			"How much red tape, duplication, or process complexity the reviewed law appears to create.",
	},
	{
		key: "repealRiskScore",
		label: "Repeal Risk",
		scale: "0-3 scale",
		description:
			"How risky repeal or major change appears given rights, benefits, offences, taxation, or core regulatory powers.",
	},
];

function getAverage(
	reviews: ReviewDetail[],
	key: SignalCard["key"],
): number {
	if (reviews.length === 0) {
		return 0;
	}

	const total = reviews.reduce((sum, review) => sum + review[key], 0);
	return total / reviews.length;
}

function formatAverage(value: number): string {
	return value === 0 ? "0.0" : value.toFixed(1);
}

export function ReviewSignalsSummary({
	reviews,
}: ReviewSignalsSummaryProps) {
	return (
		<section className="py-8">
			<div className="rounded-[2rem] border border-border bg-surface p-6 shadow-[0_16px_48px_rgba(21,19,15,0.05)] sm:p-8">
				<div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
					<div>
						<p className="font-mono text-[0.68rem] uppercase tracking-[0.28em] text-muted">
							Review Signals
						</p>
						<h2 className="mt-3 font-display text-3xl tracking-[-0.06em] text-foreground">
							How the reviewer is scoring the current reviewed set.
						</h2>
					</div>
					<p className="max-w-2xl text-sm leading-7 text-muted sm:text-base">
						These are aggregate signal averages across the reviewed bills currently
						loaded into the dashboard. They show the shape of the review reasoning
						without requiring a drilldown into individual documents.
					</p>
				</div>

				<div className="mt-6 grid gap-4 lg:grid-cols-2 xl:grid-cols-4">
					{signalCards.map((card) => {
						const average = getAverage(reviews, card.key);
						return (
							<article
								key={card.key}
								className="rounded-[1.5rem] border border-border bg-background/75 px-5 py-5 shadow-[0_12px_32px_rgba(21,19,15,0.05)]"
							>
								<p className="font-mono text-[0.62rem] uppercase tracking-[0.24em] text-muted">
									{card.label}
								</p>
								<p className="mt-3 font-display text-5xl leading-none tracking-[-0.06em] text-foreground">
									{formatAverage(average)}
								</p>
								<p className="mt-2 font-mono text-[0.58rem] uppercase tracking-[0.22em] text-accent-ink">
									{card.scale}
								</p>
								<p className="mt-4 text-sm leading-7 text-muted">
									{card.description}
								</p>
							</article>
						);
					})}
				</div>
			</div>
		</section>
	);
}
