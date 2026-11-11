# Public review scope

This edition excludes original Git history, deployment secrets, private account configuration and operational logs. New commits use a GitHub noreply identity. Automated secret detection and additional contact-pattern review were performed on the exported files. This reduces exposure; it does not certify the application as secure.

Never commit .env files, seed phrases, wallet keys, API tokens or real customer data. Credentials previously exposed elsewhere must be revoked or rotated at the issuing service. Removing a value here does not invalidate it.

Some deterministic test fixtures and public contract addresses have narrowly annotated gitleaks:allow comments. These are documented non-secret values, not broad scanning exclusions.

Please do not put secrets in public issues. Use GitHub's private vulnerability reporting if enabled, or an existing private project channel. Project discussions with Upwork clients remain on Upwork.
