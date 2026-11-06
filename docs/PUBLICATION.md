# Public export provenance

This standalone export preserves the Go implementation, schema, dependency pins, local deployment scripts, accepted test logs, successful synthetic runs and recorded development failures. The public examples use fictional accounts, a deliberate `localdemo` database password and explicit local test tokens. No production credential, user account data, private runtime, database volume or container state is included.

The original workspace and its accepted evidence remain unchanged. Export-only changes make the Docker instructions independent of the original machine, make hosted CI manual, include the historical process logs, and remove a pointer from `evidence/verification.json` into private hiring research. That pointer edit does not alter test results or source hashes. The older verification file describes its original run and source revision; the current CI configuration differs intentionally. No hosted CI result is claimed.

Publication verification is separate in `evidence/release-verification.json`. Go compilation and static checks do not reproduce PostgreSQL/NATS contention, race behavior or Docker execution. Reproduction of the historical integration suite still requires the real local services and the documented commands.

Review records describe automated artifact review, not employer endorsement or verification of personal authorship. This is a bounded local engineering demonstration, not a payment processor or evidence of production banking operations.
