type ProgressRailProps = {
	dailyCapacity: number;
	estimatedDaysRemaining: number;
	percentReviewed: number;
	reviewedCount: number;
	totalCount: number;
};

export function ProgressRail({
	dailyCapacity,
	estimatedDaysRemaining,
	percentReviewed,
	reviewedCount,
	totalCount,
}: ProgressRailProps) {
	const clampedPercent = Math.max(0, Math.min(percentReviewed, 100));
	const railWidth = clampedPercent === 0 ? "0%" : `${clampedPercent}%`;

	return (
		<section className="rounded-[2rem] border border-border bg-surface p-6 shadow-[0_24px_80px_rgba(21,19,15,0.08)] sm:p-8">
			<div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
				<div className="max-w-2xl">
					<p className="font-mono text-[0.68rem] uppercase tracking-[0.28em] text-muted">
						Sequential Review Rail
					</p>
					<h2 className="mt-3 font-display text-3xl leading-tight tracking-[-0.06em] text-foreground sm:text-4xl">
						Progress advances in source order, 200 documents at a time.
					</h2>
				</div>
				<p className="max-w-xl text-sm leading-7 text-muted sm:text-base">
					The rail updates as the legislative review pipeline clears the corpus. No
					confidence ranking or document prioritization is shown here, only the
					rolling position of the review run.
				</p>
			</div>

			<div className="mt-8">
				<div className="h-5 overflow-hidden rounded-full border border-border bg-background">
					<div
						className="h-full rounded-full bg-accent transition-[width] duration-700 ease-out"
						style={{ width: railWidth }}
					/>
				</div>

				<div className="mt-5 grid gap-4 border-t border-border pt-5 text-sm text-muted sm:grid-cols-4">
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
							Days Remaining @ {dailyCapacity}/day
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
