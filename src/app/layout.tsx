import type { Metadata } from "next";
import "./globals.css";
import { foundersGroteskMono, soehneBuch, soehneKraftig } from "./fonts";

export const metadata: Metadata = {
	title: "Build Canada",
	description: "Build Canada legislative review tracker.",
};

export default function RootLayout({
	children,
}: Readonly<{
	children: React.ReactNode;
}>) {
	return (
		<html lang="en">
			<head>
				<link rel="icon" href="/favicon.svg" type="image/svg+xml"></link>
			</head>
			<body
				className={`${foundersGroteskMono.variable} ${soehneBuch.variable} ${soehneKraftig.variable} antialiased`}
			>
				{children}
			</body>
		</html>
	);
}
