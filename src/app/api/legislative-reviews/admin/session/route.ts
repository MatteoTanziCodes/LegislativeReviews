import { NextRequest, NextResponse } from "next/server";
import {
	createAdminSessionToken,
	getAdminSessionCookieName,
	getAdminSessionMaxAgeSeconds,
	verifyAdminToken,
} from "@/lib/review-admin-auth";

export const dynamic = "force-dynamic";
export const revalidate = 0;

function baseHeaders() {
	return {
		"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
	};
}

export async function POST(request: NextRequest) {
	try {
		const payload = (await request.json()) as { token?: string };
		if (typeof payload.token !== "string" || payload.token.trim().length === 0) {
			return NextResponse.json(
				{ error: "Admin token is required." },
				{ status: 400, headers: baseHeaders() },
			);
		}

		const isValid = await verifyAdminToken(payload.token);
		if (!isValid) {
			return NextResponse.json(
				{ error: "Invalid admin token." },
				{ status: 401, headers: baseHeaders() },
			);
		}

		const sessionToken = await createAdminSessionToken();
		const response = NextResponse.json({ ok: true }, { headers: baseHeaders() });
		response.cookies.set({
			name: getAdminSessionCookieName(),
			value: sessionToken,
			httpOnly: true,
			maxAge: getAdminSessionMaxAgeSeconds(),
			path: "/",
			sameSite: "lax",
			secure: process.env.NODE_ENV === "production",
		});
		return response;
	} catch (error) {
		console.error("Unable to create legislative review admin session.", error);
		return NextResponse.json(
			{ error: "Unable to create admin session." },
			{ status: 500, headers: baseHeaders() },
		);
	}
}

export async function DELETE() {
	const response = NextResponse.json({ ok: true }, { headers: baseHeaders() });
	response.cookies.set({
		name: getAdminSessionCookieName(),
		value: "",
		httpOnly: true,
		maxAge: 0,
		path: "/",
		sameSite: "lax",
		secure: process.env.NODE_ENV === "production",
	});
	return response;
}
