# Synapse gosync-worker

Working notes: what was decided, what was measured, and what bit us.

Deployment specifics and test credentials are in `.claude/deployment-notes.md`
(gitignored). Synapse's source is checked out at **`/home/daedric/synapse`**
(`v1.159.0-18-g3db77e80a5`) — read it there rather than fetching from GitHub;
`develop` is ahead of what runs here. Line numbers drift between versions;
grep the symbol name, which is stable.

## 1. Index

Start with the ground rules below, then whichever of these the task touches.

| Document | Read it when |
|---|---|
| [docs/comparability.md](docs/comparability.md) | Writing or interpreting any parity test. Nine reasons two `/sync` answers legitimately differ, what pinning fixes, and the three it cannot. **The most important document here.** |
| [docs/tokens.md](docs/tokens.md) | Touching stream tokens, or wondering why the `end` token is approximate before M5. |
| [docs/synapse-notes.md](docs/synapse-notes.md) | Implementing anything new. Findings that contradict a plain reading of the source, plus a map of where things live in Synapse. |
| [docs/auth.md](docs/auth.md) | Touching authentication. Why we ask Synapse rather than reading `access_tokens`. |
| [docs/decisions.md](docs/decisions.md) | Before deciding something that looks already-decided. |
| [docs/log.md](docs/log.md) | Catching up on what has been done, in order. |
| [docs/milestones.md](docs/milestones.md) | Picking up the next piece of work. |
| [deploy/grafana/README.md](deploy/grafana/README.md) | Watching a running worker. Which panels mislead, and the two things the dashboard cannot see. |

## 2. What this is

A Go reimplementation of Synapse's **classic** `/sync` worker, reading Synapse's
PostgreSQL directly and following Synapse's replication stream over Redis.
**Simplified sliding sync (MSC4186) is in scope and served** — see
§7 and docs/milestones.md. The older MSC3575 sliding sync is not: it shares a
URL prefix and an `unstable_features` flag with the simplified one and almost
nothing else.

It is the third worker in this family and the one `synapse-gopro-worker`
explicitly scoped out (its CLAUDE.md §12: *"the genuine remaining opportunity is
the client API, which is a different project… worth a decision rather than a
drift into it"*). Conventions, dependency choices, config shape, unix-socket
serving and live-test gating are reused from `synapse-gopro-worker` and
`synapse-media-worker` rather than reinvented.

**It is routed to, but only on its own hostname.** `gosync.aguiarvieira.pt` is
a second nginx host that sends the four endpoints here and everything else to
Synapse's own workers; `aguiarvieira.pt` is untouched, so no real client reaches
this worker by accident. Element Web and gomuks run against it on the owner's
main account (654 rooms), alongside `cmd/syncdiff` and the two test accounts.

That split is why `/login` needs rewriting on the second host: Synapse puts
`public_baseurl` in the login response's `well_known`, and a client obeys it and
goes straight back to the real hostname. See §6.

### Why, measured

`/opt/npm/data/logs/proxy-host-2_access.log`, 21h to 2026-08-31:

| Endpoint | Requests |
|---|---|
| `/_matrix/client/*/sync` | **558,398**, essentially all carrying `since=` |
| `org.matrix.simplified_msc3575/sync` | 11,844 — see §7 |
| `/events`, `/initialSync`, `/rooms/{id}/initialSync` | **0** |

All three legacy endpoints are nonetheless served and at parity: they were in
scope, and a scope with a hole in it is not a finished scope.

Roughly seven times the entire federation read volume gopro-worker was built
for. Two consequences drive everything:

- **Almost all real traffic is incremental**, so the early milestones could not
  be validated by watching production. They were driven from a test account, and
  since the split host exists, from real clients on a real one.
- **The three legacy endpoints serve nobody here.** They were warm-up targets;
  they are served and at parity because the scope said so, not because anything
  asks for them.

---

## 3. Ground rules

- **Parity is the contract.** A response must be indistinguishable from
  Synapse's. Anything that differs is a bug unless it is a deliberate deviation
  recorded in README.md.
- **When behaviour is uncertain, defer to Synapse** rather than approximating.
  A fallback is always correct; a guess is not.
- **Read the upstream source before changing behaviour.** Nearly every subtlety
  below was found by reading Synapse or querying the database, and almost none
  of them were guessable.
- **Test the transport you actually run.** Production listens on a unix socket;
  media-worker's notes record a bug that a TCP-only test run hid.
- **This worker reads, with one named exception.** Every query in
  `internal/store` is a `SELECT` and the role enforces it. To-device deletion
  (`internal/deviceinbox`) is the sole write: a separate package, a separate
  pool, a separate role granted `SELECT, DELETE` on `device_inbox` alone, and a
  startup check that refuses a role any broader. Do not widen it, and do not
  reach for that pool from anywhere else.
- **The comparator cannot see everything, and knowing its shape is part of
  using it.** It compares one request at a time, bodies only, with filters it
  wrote itself, and it never follows `next_batch` in a loop. So it is blind to
  anything about liveness, headers, or client-uploaded filters — and four
  defects in one evening were found by pointing real clients at it instead:
  CORS (no preflight, no header comparison), a typing hot loop (needs a client
  that syncs in a loop), a rejected filter (needs a filter a client wrote), and
  invites from ignored users (needs an account that ignores somebody). Prefer
  the comparator for equivalence; prefer a real client for liveness.
- **Anything derived from a token we supplied cannot be tested by supplying
  it.** Pinning hands the implementation its own answer. Three instances so
  far: `prev_batch`, the to-device wind-back, and `/events`' `end` token — in
  each case a deliberately broken build passed a pinned comparison. When a
  field is computed FROM the now token, the comparison has to stop pinning that
  field. See docs/comparability.md sources 3 and 9.
- **Scale and history find what behaviour does not.** The test accounts have 9
  and 30 rooms and ignore nobody. The main account has 654 rooms and 161 ignored
  users, and produced: a 209-second initial sync, 1.5GB of state maps, four
  invites from 2025, and a room whose newest 1,587 events are all rejected. None
  of those are behavioural differences a small account can express.
- **Never re-encode stored event JSON.** 14,654 events on this server contain
  escaped NUL characters that PostgreSQL `jsonb` cannot even cast. Responses are
  built by splicing `event_json.json` with `sjson`, not by unmarshal/marshal.

---

## 4. Commands

```sh
go build ./... && go test -race ./...

# Validate config and database access without serving.
go run ./cmd/gosync-worker -config gosync-worker.yaml -check
```

CI (`.github/workflows/docker.yml`) runs `go vet`, `go test -race` and a
`gofmt` check, then builds and publishes the container image. It gates the
image on the tests, so anything that fails locally fails the push. Build info
reaches the binary through `-ldflags` on `main.tag`, `main.commit` and
`main.buildTime`; keep those three names if the Dockerfile is touched, because
all three workers in this family share the workflow's shape.

Live tests gate on env vars and `t.Skip()` otherwise — they never fail CI and
run only against the real deployment. See `.claude/deployment-notes.md`.

## 5. Running the comparator

The reference worker must have `sync_response_cache_duration: 0`. Verify that
before trusting a run: a cached reference answer is reported as a *match*.

```sh
go build -o /tmp/gosync-worker ./cmd/gosync-worker
/tmp/gosync-worker -config gosync-worker.yaml &     # needs testing.allow_pin_now: true

go run ./cmd/syncdiff \
  -go-socket   /var/sockets/gosync-worker.sock \
  -ref-socket  /var/sockets/nginx/av-sync-worker-2.sock \
  -token-file  ~/.gosync-test-token \
  -limit 20
```

Add `-endpoint initial_sync` for the whole-account snapshot.

`-endpoint to_device -to-device 105 -homeserver https://aguiarvieira.pt` is the
one comparison that deliberately does **not** pin, because the pin hides the
only defect it is looking for. It sends the messages itself and asks both sides
twice. It writes to the homeserver and both sides delete, so point it at a test
account. See docs/comparability.md source 9.

With no `-rooms`, it compares every room the test account has joined. Do that:
the two rooms it was already in, not the seven purpose-built ones, are what
caught all four serialisation defects on 2026-09-01.

`-filter '<json>'` sends an inline sync filter, which is what a real client
does. Run at least `lazy_load_members` before believing a `/sync` change: the
default filter exercises none of the state-restriction path, and a filter that
shortens the timeline is the only thing that exercises `prev_batch` on an
untrimmed one.

**Check the comparator still bites** after changing it. Build the worker with
one field deliberately wrong and confirm syncdiff names it and exits non-zero. A
comparator that always says "ok" is worse than none.

## 6. Driving it with a real client

Worth doing, and the last four defects came from it. Four things to get right
first, because three of them fail in ways that look like our bug and one of them
is destructive.

**The login response overrides the hostname.** Synapse puts `public_baseurl`
into `/login`'s `well_known`, so a client logging in against the second host is
told to go back to `aguiarvieira.pt` and does. Rewrite it on that host:

```nginx
location ~ ^/_matrix/client/(api/v1|r0|v3|unstable)/login$ {
	include /data/nginx/custom/matrix-config.conf;
	sub_filter_types application/json;
	sub_filter_once off;
	sub_filter "https://aguiarvieira.pt" "https://gosync.aguiarvieira.pt";
	proxy_pass http://av-client-requests-lc;
}
```

Do NOT add a bare `proxy_set_header` to that block. nginx inherits those from
the enclosing level only when the level defines none, so one `proxy_set_header`
silently drops `X-Forwarded-For`, and Synapse's login handler then calls `.host`
on a `UNIXAddress` and 500s. The `include` carries the four headers, and
`Accept-Encoding ""` with them, which `sub_filter` needs anyway.

**`socket_mode` must be `0666`.** nginx runs in another container as a different
uid. The example config's `0660` is right for the comparator and wrong the
moment nginx has to connect.

**`allow_pin_now` must be false.** It accepts a window that has not happened
yet. It is for the comparator, never for a host a client can reach.

**`to_device` deletes, and the blast radius is exactly one device.** Deletion is
per `(user_id, device_id)` and bounded by that client's own `since`, so a fresh
device is safe and an existing one is not: get it wrong and real messages become
undecryptable. Use a new login. With `to_device.enabled: false` nothing is lost
either — Synapse keeps the messages, and the device receives them when it next
syncs against the real host — so that is the setting for a first pass.

Also true and visible to other people: **we never write presence**, so an
account syncing only against this worker appears permanently offline.

### Comparing against a real account

`cmd/syncdiff` is read-only with one exception each way: skip `-endpoint
to_device` (it sends and deletes) and `-endpoint events` (it marks the account
online — that endpoint has no `set_presence`). Everything else writes nothing.

Two flags make a large account legible:

```sh
-filter '{"presence":{"not_types":["*"]},"room":{"state":{"lazy_load_members":true},"timeline":{"limit":1}}}'
```

`not_types: ["*"]` blocks presence, which is otherwise unpinnable noise that
buries every real difference on an account with thousands of co-occupants. The
timeline limit keeps the comparator from holding two 200MB documents as Go maps.

## 7. Sliding sync (MSC4186)

Served on both paths: `/_matrix/client/unstable/org.matrix.simplified_msc3575/sync`,
which every client uses today, and `/_matrix/client/v4/sync`, which the MSC
settles on. **POST**, unlike everything else here.

Four things about it differ from the rest of the worker, and each is a trap.

**It writes, and reading writes.** Per-connection state — what this connection
has already been told — is not derivable from a token. Even loading it prunes
forked positions and bumps a timestamp. `internal/slidingstore` is the only
package that writes it, behind role `gosync_ss`, which owns the `gosync` schema
and has **nothing in `public`**. Startup refuses a role that can read
`public.events`. `internal/store` is still 100% `SELECT`.

**Our `pos` is not Synapse's.** The tables are ours, so a client cannot move
between `gosync.aguiarvieira.pt` and `aguiarvieira.pt` mid-connection; it gets
`M_UNKNOWN_POS` and re-bootstraps. That is deliberate — docs/decisions.md — and
`M_UNKNOWN_POS` is a supported path rather than an error.

**`pos` and `timeout` come from the QUERY STRING.** MSC4186 puts them in the
body; Synapse reads them from the query. We accept both, query first, so
today's clients and the stable endpoint's clients both work.

**A full-range list is returned UNSORTED by Synapse.** Comparing the order of
one is comparing nothing; comparing a partial range's order is the only test of
the sort. See docs/comparability.md source 10.

Verification is a lockstep comparison rather than a single request: the response
is a delta against per-connection state, so nothing about it means anything
until several rounds have run. Six rounds on each of the three accounts —
including widening the window mid-sequence and re-sending `pos=0/<token>` —
agree with `av-sync-worker-2` exactly.
