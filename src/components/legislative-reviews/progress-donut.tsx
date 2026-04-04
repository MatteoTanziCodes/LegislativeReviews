type ProgressDonutProps = {
	percentReviewed: number;
	reviewedCount: number;
	remainingCount: number;
};

const VIEWBOX_SIZE = 240;
const STROKE_WIDTH = 18;
const RADIUS = (VIEWBOX_SIZE - STROKE_WIDTH) / 2;
const CIRCUMFERENCE = 2 * Math.PI * RADIUS;

export function ProgressDonut({
	percentReviewed,
	reviewedCount,
	remainingCount,
}: ProgressDonutProps) {
	const clampedPercent = Math.max(0, Math.min(percentReviewed, 100));
	const percentLabel =
		clampedPercent < 1 ? clampedPercent.toFixed(2) : clampedPercent.toFixed(1);
	const strokeDashoffset =
		CIRCUMFERENCE - (clampedPercent / 100) * CIRCUMFERENCE;

	return (
		<section className="rounded-[2rem] border border-border bg-surface p-6 shadow-[0_24px_80px_rgba(21,19,15,0.08)] sm:p-8">
			<div className="relative mx-auto aspect-square w-full max-w-[18rem]">
				<svg
					viewBox={`0 0 ${VIEWBOX_SIZE} ${VIEWBOX_SIZE}`}
					className="-rotate-90 overflow-visible"
					role="img"
					aria-label={`${percentLabel} percent of the corpus reviewed`}
				>
					<circle
						cx={VIEWBOX_SIZE / 2}
						cy={VIEWBOX_SIZE / 2}
						r={RADIUS}
						fill="none"
						stroke="rgba(21, 19, 15, 0.1)"
						strokeWidth={STROKE_WIDTH}
					/>
					<circle
						cx={VIEWBOX_SIZE / 2}
						cy={VIEWBOX_SIZE / 2}
						r={RADIUS}
						fill="none"
						stroke="var(--color-accent)"
						strokeLinecap="round"
						strokeWidth={STROKE_WIDTH}
						style={{
							strokeDasharray: `${CIRCUMFERENCE} ${CIRCUMFERENCE}`,
							strokeDashoffset,
						}}
					/>
				</svg>

				<div className="absolute inset-0 flex flex-col items-center justify-center">
					<p className="font-display text-[clamp(2.75rem,8vw,4.75rem)] leading-none tracking-[-0.08em] text-foreground">
						{percentLabel}%
					</p>
					<p className="mt-2 font-mono text-[0.68rem] uppercase tracking-[0.28em] text-muted">
						Reviewed
					</p>
				</div>
			</div>

			<div className="mt-8 grid gap-3">
				<div className="flex items-center justify-between gap-3 rounded-full border border-border bg-background/70 px-4 py-3">
					<div className="flex items-center gap-3">
						<span className="h-2.5 w-2.5 rounded-full bg-accent" />
						<span className="font-mono text-[0.68rem] uppercase tracking-[0.24em] text-muted">
							Reviewed
						</span>
					</div>
					<span className="font-display text-xl tracking-[-0.04em] text-foreground">
						{reviewedCount.toLocaleString()}
					</span>
				</div>
				<div className="flex items-center justify-between gap-3 rounded-full border border-border bg-background/70 px-4 py-3">
					<div className="flex items-center gap-3">
						<span className="h-2.5 w-2.5 rounded-full bg-foreground/15" />
						<span className="font-mono text-[0.68rem] uppercase tracking-[0.24em] text-muted">
							Remaining
						</span>
					</div>
					<span className="font-display text-xl tracking-[-0.04em] text-foreground">
						{remainingCount.toLocaleString()}
					</span>
				</div>
			</div>
		</section>
	);
}
