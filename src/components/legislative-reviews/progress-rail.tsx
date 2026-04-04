type ProgressRailProps = {
	availableReviewCount?: number;
	dailyCapacity: number;
	estimatedDaysRemaining: number;
	percentReviewed: number;
	reviewedCount: number;
	rolloutActive?: boolean;
	totalCount: number;
};

export function ProgressRail({
	availableReviewCount,
	dailyCapacity,
	estimatedDaysRemaining,
	percentReviewed,
	reviewedCount,
	rolloutActive = false,
	totalCount,
}: ProgressRailProps) {
	const clampedPercent = Math.max(0, Math.min(percentReviewed, 100));
	const railWidth = clampedPercent === 0 ? "0%" : `${clampedPercent}%`;
	const headline = rolloutActive
		? `Results unlock in source order, ${dailyCapacity.toLocaleString()} documents per day.`
		: "All reviewed results are currently visible.";
	const description = rolloutActive
		? "The rail reflects the public release window, not the hidden processed backlog. Additional reviewed documents become visible each day until the staged publication completes."
		: "The rail reflects the currently published review set. When a staged release is disabled, the dashboard shows the full reviewed dataset immediately.";

	return (
		<section className="rounded-[2rem] border border-border bg-surface p-6 shadow-[0_24px_80px_rgba(21,19,15,0.08)] sm:p-8">
			<div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
				<div className="max-w-2xl">
					<p className="font-mono text-[0.68rem] uppercase tracking-[0.28em] text-muted">
						Sequential Review Rail
					</p>
					<h2 className="mt-3 font-display text-3xl leading-tight tracking-[-0.06em] text-foreground sm:text-4xl">
						{headline}
					</h2>
				</div>
				<p className="max-w-xl text-sm leading-7 text-muted sm:text-base">
					{description}
				</p>
			</div>

			<div className="mt-8">
				<div className="h-5 overflow-hidden rounded-full border border-border bg-background">
					<div
						className="h-full rounded-full bg-accent transition-[width] duration-700 ease-out"
						style={{ width: railWidth }}
					/>
				</div>

				<div className="mt-5 grid gap-4 border-t border-border pt-5 text-sm text-muted sm:grid-cols-2 xl:grid-cols-5">
					<div>
						<p className="font-mono text-[0.62rem] uppercase tracking-[0.24em]">
							Starting Point
						</p>
						<p className="mt-2 font-display text-2xl tracking-[-0.04em] text-foreground">
							0
						</p>
					</div>
					<div>
						<p className="font-mono text-[0.62rem] uppercase tracking-[0.24em]">
							Current Reviewed Count
						</p>
						<p className="mt-2 font-display text-2xl tracking-[-0.04em] text-foreground">
							{reviewedCount.toLocaleString()}
						</p>
					</div>
					{typeof availableReviewCount === "number" && rolloutActive ? (
						<div>
							<p className="font-mono text-[0.62rem] uppercase tracking-[0.24em]">
								Processed Results Ready
							</p>
							<p className="mt-2 font-display text-2xl tracking-[-0.04em] text-foreground">
								{availableReviewCount.toLocaleString()}
							</p>
						</div>
					) : null}
					<div>
						<p className="font-mono text-[0.62rem] uppercase tracking-[0.24em]">
							Total Count
						</p>
						<p className="mt-2 font-display text-2xl tracking-[-0.04em] text-foreground">
							{totalCount.toLocaleString()}
						</p>
					</div>
					<div>
						<p className="font-mono text-[0.62rem] uppercase tracking-[0.24em]">
							{rolloutActive
								? `Days Until Full Release @ ${dailyCapacity}/day`
								: "Days Remaining"}
						</p>
						<p className="mt-2 font-display text-2xl tracking-[-0.04em] text-foreground">
							{estimatedDaysRemaining.toLocaleString()}
						</p>
					</div>
				</div>
			</div>
		</section>
	);
}
