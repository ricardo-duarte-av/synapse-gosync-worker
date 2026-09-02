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
| [docs/comparability.md](docs/comparability.md) | Writing or interpreting any parity test. Explains why two `/sync` answers legitimately differ, what pinning fixes, and the two things pinning cannot fix. **The most important document here.** |
| [docs/tokens.md](docs/tokens.md) | Touching stream tokens, or wondering why the `end` token is approximate before M5. |
| [docs/synapse-notes.md](docs/synapse-notes.md) | Implementing anything new. Findings that contradict a plain reading of the source, plus a map of where things live in Synapse. |
| [docs/auth.md](docs/auth.md) | Touching authentication. Why we ask Synapse rather than reading `access_tokens`. |
| [docs/decisions.md](docs/decisions.md) | Before deciding something that looks already-decided. |
| [docs/log.md](docs/log.md) | Catching up on what has been done, in order. |
| [docs/milestones.md](docs/milestones.md) | Picking up the next piece of work. |

## 2. What this is

A Go reimplementation of Synapse's **classic** `/sync` worker, reading Synapse's
PostgreSQL directly and following Synapse's replication stream over Redis.
Sliding sync and simplified sliding sync are out of scope.

It is the third worker in this family and the one `synapse-gopro-worker`
explicitly scoped out (its CLAUDE.md §12: *"the genuine remaining opportunity is
the client API, which is a different project… worth a decision rather than a
drift into it"*). Conventions, dependency choices, config shape, unix-socket
serving and live-test gating are reused from `synapse-gopro-worker` and
`synapse-media-worker` rather than reinvented.

**Nothing is routed to this worker.** It is driven deliberately by
`cmd/syncdiff` and the test account.

### Why, measured

`/opt/npm/data/logs/proxy-host-2_access.log`, 21h to 2026-08-31:

| Endpoint | Requests |
|---|---|
| `/_matrix/client/*/sync` | **558,398**, essentially all carrying `since=` |
| `org.matrix.simplified_msc3575/sync` | 11,844 (out of scope) |
| `/events`, `/initialSync`, `/rooms/{id}/initialSync` | **0** |

Roughly seven times the entire federation read volume gopro-worker was built
for. Two consequences drive everything:

- **Almost all real traffic is incremental**, so the early milestones cannot be
  validated by watching production. They have to be driven from the test account.
- **The three legacy endpoints serve nobody here.** They are warm-up targets,
  not deliverables.

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
- **Never re-encode stored event JSON.** 14,654 events on this server contain
  escaped NUL characters that PostgreSQL `jsonb` cannot even cast. Responses are
  built by splicing `event_json.json` with `sjson`, not by unmarshal/marshal.

---

## 7. Commands

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

## 8. Running the comparator

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
