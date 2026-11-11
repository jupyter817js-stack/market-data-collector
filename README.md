# Market-data collection and comparison tools

Go/PostgreSQL code for collecting external market data, normalizing event and outcome identities, storing quote changes, and generating comparison and operational views.

## Code map

| Area | Directory |
| --- | --- |
| Provider adapters | internal/source |
| Normalization | internal/normalize |
| Persistence and views | internal/store and migrations |
| Replay | internal/replay |
| CLOB authentication/signing helpers | internal/clob |
| Operator commands | cmd |
| Offline fixtures | testdata |

The pipeline separates source records from normalized events and quotes. Collection, mapping and freshness checks provide context before records are compared.

## Development checks

The module declares Go 1.24+. From this directory:

~~~sh
go build ./...
go test ./...
~~~

Database integration tests may require an isolated PostgreSQL database. See VALIDATION.md for checks actually performed. The module path is retained from the original implementation to keep internal imports consistent.

## Configuration and scope

Real credentials, collector accounts, operational captures, and private deployment notes are excluded. Supply your own authorized provider access for any live collection. The provider API-key fallback has been removed from the public edition.

The repository includes both read-only collection tools and separately named execution-related commands. Do not treat every command as read-only: commands such as clob-trade-live, clob-auto-submit, clob-approve, and clob-executor can have external effects when configured. They were not run during preparation of this portfolio.

This is a code-review snapshot, not a turnkey service or evidence of live profitability.

## About this public edition

This is a source snapshot prepared for code review. Operational logs, account-specific configuration, runtime state, private keys, and the private repository history are excluded. The original implementation is preserved separately. This snapshot is not evidence of audited production readiness or investment performance.

Please use Upwork Messages for project discussions.
