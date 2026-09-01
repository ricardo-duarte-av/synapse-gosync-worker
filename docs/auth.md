# Authentication

Why tokens are validated by asking Synapse, not by reading a table.

Tokens are validated by asking Synapse's `/_matrix/client/v3/account/whoami`
over a unix socket, with an LRU cache and `singleflight`. **Not** by a `SELECT`
on `access_tokens`, even though that query is trivial and unhashed.

Three kinds of caller would be wrongly rejected by the table lookup:

- **appservice tokens** live in registration YAML and never reach the database;
- **delegated auth (MAS)** keeps tokens outside Synapse entirely;
- **guest tokens are macaroons**, verifiable only with `macaroon_secret_key` —
  which this worker must never hold, for the reasons in gopro's CLAUDE.md §8.
  There are **79 guest accounts** on this homeserver, so the case is real.

whoami also returns the **`device_id`**, which `/sync` cannot work without:
to-device messages and device-list updates are per-device, not per-user. Synapse
omits the field entirely for appservice accounts, which have no device
(`rest/client/account.py:877`).

A whoami failure means the answer is **unknown**, not a rejection: it must
surface as 502, never 401. Caching a 5xx as a rejection would turn a Synapse
hiccup into a wave of client logouts. Watch
`gosync_auth_verdicts_total{verdict="unavailable"}`.

---
