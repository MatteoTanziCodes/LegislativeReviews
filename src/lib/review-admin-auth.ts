import { createHmac, timingSafeEqual } from "node:crypto";
import { cookies } from "next/headers";
import { getCloudflareContext } from "@opennextjs/cloudflare";

const ADMIN_SESSION_COOKIE = "legislative_reviews_admin_session";
const SESSION_MAX_AGE_SECONDS = 60 * 60 * 12;

type DashboardAuthEnv = CloudflareEnv & {
	LEGISLATIVE_REVIEW_ADMIN_TOKEN?: string;
	LEGISLATIVE_REVIEW_SESSION_SECRET?: string;
};

function toBase64Url(input: string) {
	return Buffer.from(input, "utf-8").toString("base64url");
}

function signValue(value: string, secret: string) {
	return createHmac("sha256", secret).update(value).digest("base64url");
}

function safeEqual(left: string, right: string) {
	const leftBuffer = Buffer.from(left);
	const rightBuffer = Buffer.from(right);
	if (leftBuffer.length !== rightBuffer.length) {
		return false;
	}
	return timingSafeEqual(leftBuffer, rightBuffer);
}

async function getAuthEnv(): Promise<DashboardAuthEnv | null> {
	if (process.env.NODE_ENV === "development") {
		return null;
	}

	try {
		const { env } = await getCloudflareContext({ async: true });
		return env as DashboardAuthEnv;
	} catch {
		return null;
	}
}

export async function getReviewAdminToken(): Promise<string | null> {
	const env = await getAuthEnv();
	return (
		env?.LEGISLATIVE_REVIEW_ADMIN_TOKEN ??
		process.env.LEGISLATIVE_REVIEW_ADMIN_TOKEN ??
		null
	);
}

async function getReviewSessionSecret(): Promise<string | null> {
	const env = await getAuthEnv();
	return (
		env?.LEGISLATIVE_REVIEW_SESSION_SECRET ??
		process.env.LEGISLATIVE_REVIEW_SESSION_SECRET ??
		(await getReviewAdminToken())
	);
}

export async function verifyAdminToken(candidate: string): Promise<boolean> {
	const expectedToken = await getReviewAdminToken();
	if (!expectedToken) {
		return false;
	}
	return safeEqual(candidate.trim(), expectedToken);
}

export async function createAdminSessionToken(): Promise<string> {
	const sessionSecret = await getReviewSessionSecret();
	if (!sessionSecret) {
		throw new Error("Admin session secret is not configured.");
	}

	const expiresAt = Date.now() + SESSION_MAX_AGE_SECONDS * 1000;
	const payload = JSON.stringify({ exp: expiresAt });
	const encodedPayload = toBase64Url(payload);
	const signature = signValue(encodedPayload, sessionSecret);
	return `${encodedPayload}.${signature}`;
}

export async function isAdminAuthenticated(): Promise<boolean> {
	const sessionSecret = await getReviewSessionSecret();
	if (!sessionSecret) {
		return false;
	}

	const cookieStore = await cookies();
	const token = cookieStore.get(ADMIN_SESSION_COOKIE)?.value;
	if (!token) {
		return false;
	}

	const [encodedPayload, signature] = token.split(".");
	if (!encodedPayload || !signature) {
		return false;
	}

	const expectedSignature = signValue(encodedPayload, sessionSecret);
	if (!safeEqual(signature, expectedSignature)) {
		return false;
	}

	try {
		const payload = JSON.parse(
			Buffer.from(encodedPayload, "base64url").toString("utf-8"),
		) as { exp?: number };
		return typeof payload.exp === "number" && payload.exp > Date.now();
	} catch {
		return false;
	}
}

export function getAdminSessionCookieName() {
	return ADMIN_SESSION_COOKIE;
}

export function getAdminSessionMaxAgeSeconds() {
	return SESSION_MAX_AGE_SECONDS;
}
