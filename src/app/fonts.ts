import localFont from "next/font/local";

export const foundersGroteskMono = localFont({
	src: [
		{
			path: "../../public/founders-grotesk-mono-light.woff2",
			weight: "300",
			style: "normal",
		},
		{
			path: "../../public/founders-grotesk-mono-regular.woff2",
			weight: "400",
			style: "normal",
		},
	],
	display: "swap",
	variable: "--font-founders-grotesk-mono",
});

export const soehneBuch = localFont({
	src: [
		{
			path: "../../public/soehne-buch.woff2",
			weight: "400",
			style: "normal",
		},
		{
			path: "../../public/soehne-buch-kursiv.woff2",
			weight: "400",
			style: "italic",
		},
	],
	display: "swap",
	variable: "--font-soehne-buch",
});

export const soehneKraftig = localFont({
	src: [
		{
			path: "../../public/soehne-kraftig.woff2",
			weight: "600",
			style: "normal",
		},
		{
			path: "../../public/soehne-kraftig-kursiv.woff2",
			weight: "600",
			style: "italic",
		},
	],
	display: "swap",
	variable: "--font-soehne-kraftig",
});
