# agent-events

A small Jenkins plugin that records build and **executor allocation** events in
a local SQLite database and streams them over HTTP, resumable after a dropped
connection. One subscription tells the whole story of a build: started, which
agents it took, when each gave its executor back, and the final result.

```json
{"seq":1,"event":"build_started","at":1790230560347,"job":"mixed-pipeline","job_id":"7bc2f917-…","url":"job/mixed-pipeline/16/","build":16}
{"seq":2,"event":"executor_allocated","at":1790230561419,"job":"mixed-pipeline","job_id":"7bc2f917-…","url":"job/mixed-pipeline/16/","build":16,"node":"lab-agent","node_id":"48b0ea38-…","executor":0,"task":"part of mixed-pipeline #16"}
{"seq":3,"event":"executor_released","at":1790230575792,"job":"mixed-pipeline","job_id":"7bc2f917-…","url":"job/mixed-pipeline/16/","build":16,"node":"lab-agent","node_id":"48b0ea38-…","executor":0,"task":"part of mixed-pipeline #16","outcome":"completed","duration_ms":14396}
{"seq":4,"event":"build_ended","at":1790230576172,"job":"mixed-pipeline","job_id":"7bc2f917-…","url":"job/mixed-pipeline/16/","build":16,"status":"FAILURE"}
```

## Why it exists

Jenkins publishes no event when an executor is taken or handed back. It will
tell you a build left the queue `ALLOCATED` - an executor was assigned - but
never *which* one, so the only ways to learn where a build ran are polling
`/computer/api/json` while it runs or reading the log afterwards. Neither works
for watching a fleet live, and neither leaves a record to analyse later.

Jenkins also has no stable identity for a job or an agent, only a name, and
names change. The plugin gives each one an id that survives renames and moves,
so history about a job is still about that job after it is renamed.

## The events

Four, built on two Jenkins core extension points. An event's name says what
happened; a property appears only when it carries something the name does not.

| event | when | source |
| --- | --- | --- |
| `build_started` | a build began executing | `RunListener` |
| `build_ended` | it finished, with `status` | `RunListener` |
| `executor_allocated` | an executor took up work | `ExecutorListener` |
| `executor_released` | it handed the work back | `ExecutorListener` |

Queue movement is not recorded: a queue item that never becomes a build is not a
build, so a build cancelled while still queued leaves no trace.

Properties, all optional except `seq`, `event` and `at`:

| property | meaning |
| --- | --- |
| `seq` | position in the record; what a reconnect resumes from |
| `event` | what happened |
| `at` | when Jenkins says it happened, epoch milliseconds |
| `job` | full job name, as it is now |
| `job_id` | the job's stable id |
| `url` | path to the build, relative to the Jenkins root; absent once the job is deleted |
| `build` | build number |
| `status` | the build's result, on `build_ended` only |
| `node` | agent the executor belongs to, as it is named now; `built-in` for Jenkins itself (`master` on installs never migrated) |
| `node_id` | the agent's stable id |
| `executor` | executor number on that node |
| `task` | what the executor is working on, as Jenkins names it |
| `outcome` | `completed` or `problems`, on `executor_released` |
| `duration_ms` | how long the executor held the work: release time minus the executor's own start time, which is how Jenkins measures it |
| `problem` | the failure Jenkins reported, when there was one |
| `node_hidden` | set when the agent was withheld for lack of permission |

`at`, `build`, `executor` and `duration_ms` are JSON numbers; the rest are
strings.

Names are resolved when an event is *read*, not when it happened: replaying a
build from before a rename shows the job under its new name and `url`. `job_id`
and `node_id` are what to key on.

`node` is the agent's own name, so it matches what you would write in
`node('lab-agent')`. Jenkins's built-in node has no name of its own and reports
as its label, `built-in`.

## What `outcome` means, and what it does not

`ExecutorListener` knows nothing about what kind of job it is serving. That is
deliberate: one meaning for every event whatever ran - a Pipeline `node` block,
a freestyle build, a matrix configuration, or anything else that occupies an
executor.

It distinguishes exactly two endings, and so does this plugin:

* `completed` - the task came back with no error raised
* `problems` - Jenkins reported a `Throwable` for the task, summarized in
  `problem`

**`completed` is not a pass.** A freestyle build that exits 1, a Pipeline branch
that calls `error`, and an aborted build all release their executor cleanly and
so all report `completed`; the runs end `FAILURE`, `FAILURE` and `ABORTED`
respectively. Whether the *build* passed is a separate question, answered by
`status` on `build_ended`. Ask an executor event where work ran and for how
long; ask `build_ended` whether it passed.

In lab testing `problems` never fired: killing an agent mid-build did not
release the executor at all (a Pipeline `node` block waits for the agent to
reconnect), and aborting released it as `completed`. It is implemented from the
listener's contract rather than from an observed case.

## What is tracked

* **Static agents and the built-in node only.** Work on an ephemeral agent - an
  `EphemeralNode` or a cloud agent (`AbstractCloudSlave`) - is not recorded:
  such agents come and go by the thousand and would fill the nodes table with
  names that are never seen again. The build itself is still recorded; only its
  allocations there are not.
* **Flyweight executors are skipped.** A Pipeline's own task sits on a
  flyweight executor for the whole run, occupying no agent capacity. Its
  lifetime is the build's lifetime, which `build_started` and `build_ended`
  already report.
* **Pairing a release with its allocation.** A build can occupy two executors
  on the same agent - two parallel Pipeline branches, say - so both events
  carry `executor`; the pair (`node`, `executor`) is one executor slot.
* **Nothing before installation.** There is no backfill from existing build
  history.

## Storage

Everything lives in `$JENKINS_HOME/agent-events/events.db`, an SQLite database
bundled with the plugin (xerial `sqlite-jdbc`, which is why the `.hpi` is 12 MB:
it carries native libraries for every platform Jenkins runs on). There is no
in-memory copy of events: the stream reads the same rows the listeners write.

| table | one row per |
| --- | --- |
| `jobs` | job ever seen: `id` (UUID), current `full_name`, `deleted_at` |
| `nodes` | static agent ever seen: `id` (UUID), current `name`, `deleted_at` |
| `builds` | build: job, number, start and end times, result |
| `allocations` | executor a build occupied: node, executor number, task, allocation and release times, outcome, problem |
| `meta`, `sequence` | epoch, last sequence number issued |

An event is not stored twice. Each row carries the sequence numbers of the
changes it went through (`started_seq` and `ended_seq` on a build,
`allocated_seq` and `released_seq` on an allocation), and "events after N" is a
query over those columns.

**Identity follows Jenkins.** An id is minted the first time a job or agent is
seen and kept through:

* a job rename or move between folders, including everything inside a renamed
  or moved folder: Jenkins reports each of those items itself
  (`ItemListener.onLocationChanged`), as it deletes a folder's contents one item
  at a time
* an agent rename (`NodeListener.onUpdated`)

Deleting a job or agent marks it `deleted_at` and keeps its history. If a new
job is later created under the same name, it is a different job with a new id.

**Writes are synchronous**, inside the listener callback, on one connection in
WAL mode. There is no queue, so there is nothing to lose on a crash and no
second representation of an event waiting to be written. Measured on the lab
(eMMC storage, the slowest disk likely to host a Jenkins):

| write | p50 | p99 | max |
| --- | --- | --- | --- |
| build started | 0.30 ms | 4.0 ms | 54 ms |
| executor allocated | 0.34 ms | 2.7 ms | 65 ms |
| executor released | 0.28 ms | 2.8 ms | 58 ms |
| build ended | 0.28 ms | 0.9 ms | 52 ms |

The tail is SQLite checkpointing the WAL. Eight threads writing flat out managed
about 1250 writes a second between them. A write that fails is logged and
dropped; it never fails the build.

**Retention** is off: at a few hundred bytes a row, most instances can keep
everything. To keep only recent history, start Jenkins with

```
-Dcom.varjo.jenkins.agentevents.Store.retentionDays=90
```

Once a day, builds older than that are deleted along with their allocations.
The property is read on every run, so it can be changed from the script console
without a restart. Jobs and agents are never pruned.

Readers need nothing but the plugin: the stream is the interface, and the
database file is an implementation detail that may change between versions.

**The schema is versioned.** Each version is a migration script in
`src/main/resources/com/varjo/jenkins/agentevents/schema/`: `1.sql` creates the
initial schema, and each later `n.sql` takes a database from version `n - 1` to
`n`. The version a database is at is its `PRAGMA user_version`. On startup the
plugin runs every script the database has not had yet, in order, each in the
same transaction as the version bump, so a failed migration leaves the file as
it was. A database written by a newer plugin is refused rather than guessed at,
which makes a downgrade fail loudly instead of corrupting history.

A script that has shipped is never edited. A schema change is a new script plus
a bump of `Store.SCHEMA_VERSION`.

## The stream

```
GET /agent-events/stream            everything stored, then live
GET /agent-events/stream?since=417  everything after 417, then live
```

One JSON object per line: an event's properties, `seq` first. Blank lines are
keepalives, every 15 seconds. Two control lines:

```json
{"type":"hello","epoch":"5f2c...","oldest":1,"latest":418}
{"type":"gap","requested_since":9,"oldest_available":57}
```

`hello` opens every stream. `gap` says the record is incomplete - the requested
sequence has been pruned - so a client learns it missed something instead of
quietly missing it. A reader that falls behind is simply behind: it catches up
from the database, and nothing is buffered for it.

History survives a Jenkins restart, and so does the numbering: a client that
reconnects after a restart resumes from its cursor with nothing missed.
**`epoch`** identifies the database, and changes only if it is deleted and
recreated. A client that sees a new epoch drops its cursor; the server also
notices `since > latest` and answers with a gap followed by everything it has.

### Who may see what

Filtering happens per reader, in the reading user's own authentication.

* **`Item.READ` on the job** - without it the event is not sent at all.
* **`Computer.EXTENDED_READ` on the agent** - without it `node`, `node_id` and
  `executor` are removed and `node_hidden` is set, so the reader still learns
  that an executor was taken, just not where.
* **Deleted jobs and agents** can no longer be checked, so only administrators
  see them: failing closed is the only safe way to be wrong about a permission.

Jenkins core has no per-agent read permission (`Computer` defines CONFIGURE,
EXTENDED_READ, DELETE, CREATE, DISCONNECT, CONNECT, BUILD and nothing finer), so
EXTENDED_READ is the closest thing to "may see this agent's details".

Verified with two accounts on the same replay - `admin`, and a `watcher` holding
`Item.READ` but no permission on any agent:

```
=== admin ===
  seq=4  executor_allocated  node=lab-agent  executor=1        hidden=-
  seq=6  executor_released   node=built-in   executor=1        hidden=-
=== watcher ===
  seq=4  executor_allocated  node=(absent)   executor=(absent) hidden=true
  seq=6  executor_released   node=(absent)   executor=(absent) hidden=true
```

### Why NDJSON, and why no nginx change

A reader needs only a line and a JSON parse, and the per-reader filtering above
means events cannot be broadcast anyway, so a framed pub/sub format would buy
nothing here.

The response carries `X-Accel-Buffering: no`, which nginx honours per response.
Without it a proxy buffers the whole response and releases it only when the
connection closes, holding events back for as long as the stream lives; with it,
the endpoint needs no proxy configuration of its own.

A stream holds one request thread for its lifetime. For a handful of monitors
that is a fair trade for the simplicity; it is not the right shape for hundreds
of browser clients.

## What it logs

Nothing at all unless something goes wrong or you ask.

| Level | When |
| --- | --- |
| `FINE` | a client opened a stream: which user, which cursor |
| `FINE` | that stream closed: how long it lasted, how many events it received |
| `FINE` | retention pruned old builds |
| `INFO` | the database was created or migrated, once per schema version |
| `WARNING` | an event, rename or deletion could not be written - it is missing from the record |
| `WARNING` | a stream could not read the database |
| `WARNING` | the database could not be opened or migrated; nothing is recorded until Jenkins restarts |

Client traffic is `FINE` rather than `INFO` because a client reconnects on every
network blip and the system log is read at `INFO`. To follow clients, add a
logger for `com.varjo.jenkins.agentevents` at `FINE` in *Manage Jenkins > System
Log*:

```
FINE  c.v.j.a.StreamRootAction#doStream: stream opened for admin (since=0)
FINE  c.v.j.a.StreamRootAction#doStream: stream closed for admin after 54s, 9 events sent
```

Two details worth knowing. `?probe=1` requests - which is how a client asks
whether this endpoint exists at all - get the `hello` line and nothing else: no
stream held, no log entry, because it is not really a connection. And a client
that dies without closing its socket is reaped within about two keepalive
intervals, roughly 30 seconds: a one-byte keepalive to a dead peer is buffered
locally, so the failure only surfaces on the write after that. Each live stream
holds one request thread until then.

## Build and install

```sh
mvn -DskipTests package          # -> target/agent-events.hpi
```

Then *Manage Jenkins > Plugins > Advanced > Deploy Plugin*, or drop the `.hpi`
into `$JENKINS_HOME/plugins/`. A fresh install needs no restart; replacing a
loaded version does.

**No plugin dependencies** - `Plugin-Dependencies` is absent from the manifest.
Everything comes from Jenkins core except SQLite, which is bundled inside the
`.hpi`. Built against Jenkins 2.528.1, two LTS lines below ci-sandbox, so it
loads there and on anything newer.

Verified on Jenkins 2.568.3 against real inbound agents, for:

* a Pipeline across two different agents, two parallel branches on one agent
  (told apart by `executor`), freestyle builds that pass and fail, an aborted
  build
* the same job id through a rename, a move into a folder and a rename of that
  folder, with replay showing the new `url`; the same agent id through a rename
  and back
* a deleted job: marked deleted, its builds kept, visible to `admin` only
* a Pipeline split across an ephemeral agent and a static one: the static
  allocation recorded, the ephemeral one not, and the ephemeral agent never
  added to `nodes`
* a client connected across a Jenkins restart: same epoch, history intact,
  reconnect with no gap
* pruning: allocations deleted with their builds, and a reader asking for a
  pruned range sent a `gap`
* the agent-name gate with two accounts
