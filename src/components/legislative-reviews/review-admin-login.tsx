"use client";

import { useState, useTransition } from "react";

export function ReviewAdminLogin() {
	const [token, setToken] = useState("");
	const [error, setError] = useState<string | null>(null);
	const [isPending, startTransition] = useTransition();

	return (
		<main className="relative min-h-screen overflow-hidden bg-background text-foreground">
			<div className="absolute inset-0 bg-[radial-gradient(circle_at_top_left,rgba(139,35,50,0.17),transparent_34%),radial-gradient(circle_at_bottom_right,rgba(147,47,47,0.12),transparent_32%)]" />
			<div className="absolute inset-0 opacity-55 [background-image:linear-gradient(to_right,rgba(21,19,15,0.08)_1px,transparent_1px),linear-gradient(to_bottom,rgba(21,19,15,0.08)_1px,transparent_1px)] [background-size:36px_36px]" />

			<div className="relative mx-auto flex min-h-screen w-full max-w-2xl items-center px-6 py-10 sm:px-10">
				<div className="w-full rounded-[2rem] border border-border bg-surface p-8 shadow-[0_16px_48px_rgba(21,19,15,0.05)] sm:p-10">
					<p className="font-mono text-[0.68rem] uppercase tracking-[0.28em] text-muted">
						Legislative Reviews Admin
					</p>
					<h1 className="mt-4 font-display text-4xl tracking-[-0.06em] text-foreground">
						Authenticate to control the review worker.
					</h1>
					<p className="mt-4 text-base leading-8 text-muted">
						This path is for operator access only. Enter the admin token to open
						the workflow controls and audit surface.
					</p>
					<p className="mt-2 font-mono text-[0.64rem] uppercase tracking-[0.22em] text-muted">
						Use <span className="text-foreground">LEGISLATIVE_REVIEW_ADMIN_TOKEN</span>.
						Do not use <span className="text-foreground">LEGISLATIVE_REVIEW_SESSION_SECRET</span>.
					</p>

					<form
						className="mt-8 grid gap-4"
						onSubmit={(event) => {
							event.preventDefault();
							setError(null);
							startTransition(async () => {
								try {
									const response = await fetch(
										"/api/legislative-reviews/admin/session",
										{
											method: "POST",
											headers: {
												"Content-Type": "application/json",
											},
											body: JSON.stringify({ token }),
										},
									);
									const payload = (await response.json().catch(() => null)) as
										| { error?: string }
										| null;
									if (!response.ok) {
										throw new Error(
											payload?.error ?? "Unable to authenticate admin session.",
										);
									}
									window.location.reload();
								} catch (authError) {
									setError(
										authError instanceof Error
											? authError.message
											: "Unable to authenticate admin session.",
									);
								}
							});
						}}
					>
						<label className="grid gap-2">
							<span className="font-mono text-[0.62rem] uppercase tracking-[0.24em] text-muted">
								Admin Token
							</span>
							<input
								type="password"
								value={token}
								onChange={(event) => setToken(event.target.value)}
								className="rounded-2xl border border-border bg-background px-4 py-3 text-sm text-foreground outline-none transition focus:border-accent/50"
								placeholder="Enter LEGISLATIVE_REVIEW_ADMIN_TOKEN"
								autoComplete="off"
							/>
						</label>

						<button
							type="submit"
							disabled={isPending}
							className="inline-flex items-center justify-center rounded-full bg-accent px-5 py-3 font-mono text-[0.66rem] uppercase tracking-[0.24em] text-white transition hover:bg-accent-ink disabled:cursor-not-allowed disabled:opacity-60"
						>
							{isPending ? "Signing In" : "Sign In"}
						</button>

						{error ? (
							<p className="text-sm leading-7 text-[#8d261e]">{error}</p>
						) : null}
					</form>
				</div>
			</div>
		</main>
	);
}
