# Changelog

## Unreleased

- Migrate Content Vault to the Plugin-first `lenso.content-vault@1` Capability.
- Add reservation, quarantine validation, immutable commit, owner-scoped reads, transactional claims, integrity observations, and terminal quarantine sweeping.
- Add fail-closed owner/idempotency and integrity evidence contracts, a Vault-issued internal transaction boundary, explicit grace-period cleanup, and exhaustive rejection acceptance.
- Add production S3-compatible storage composition with isolated quarantine/protected capabilities and real PostgreSQL + MinIO acceptance.
- Add resumable, fixed-part streaming ingestion and verified streaming reads with a 1 GiB default ceiling, exact-key cleanup evidence, and bounded object-store I/O.
- Resolve PostgreSQL and S3 credentials through `lenso.secrets@1`, validate an
  operator-installed schema during Plugin prepare, and remove the legacy Host,
  implicit environment discovery, and cron surfaces.
