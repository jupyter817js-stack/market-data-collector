# Validation and limitations

Checked: 11 November 2026, using Go 1.27.4 on Windows.

- `go build ./...`: passed.
- `go test ./...`: failed in `internal/clob`. Three failures remain: `TestSignAuthRoundTrip` expects a non-zero random nonce; `TestBootstrapL2ParsesResponse` expects a different authentication request; `TestBootstrapL2RejectsNon2xx` expects a different error shape. These implementation/test expectations require reconciliation before use. Other tested packages passed.
- Database integration tests were disabled (`RUN_DB_TESTS=0`). No live provider, funded wallet, transaction, order or production database was used.
- No tests were removed or disabled to hide failures. This source snapshot is not production-ready validation of authentication or execution.
- Dependency vulnerability scanning was not performed for this Go module.

## Publication review

Private operational files and original history are excluded. The hardcoded provider API-key fallback was removed. Source was reviewed for credentials and direct contact information; the new Git history is scanned with Gitleaks before publication. Narrow inline exemptions identify deterministic test data, not operational secrets. Automated checks are not a security audit and cannot guarantee the absence of vulnerabilities.
