declare global {
	interface CloudflareEnv {
		LEGISLATIVE_REVIEW_DATA_BUCKET?: R2Bucket;
		LEGISLATIVE_REVIEW_SUMMARY_KEY?: string;
		LEGISLATIVE_REVIEW_DETAILS_KEY?: string;
	}
}

export {};
