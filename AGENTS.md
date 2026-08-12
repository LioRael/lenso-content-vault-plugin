# Agent instructions

This repository contains the first-party Lenso Content Vault Module.

- Keep the Module independent of any consuming business domain. Domain objects stay in the owner Module; this Module owns only upload sessions, immutable bytes, content references, claims, and integrity observations.
- Use only public Lenso authoring APIs from the `lenso` crate in production code.
- Do not expose protected object keys or add protected-blob deletion without a separately reviewed retention protocol.
- PostgreSQL acceptance tests require `CONTENT_VAULT_TEST_DATABASE_URL` and must target a database whose name starts with `content_vault_test`.
- Registry publication, immutable tags, GitHub Releases, and remote repository creation require explicit approval.
