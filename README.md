# agent-events

A small Jenkins plugin that streams build and **executor allocation** events
over HTTP, resumable after a dropped connection. One subscription tells the
whole story of a build: queued, started, which agents it took, when each gave
its executor back, and the final result.

```json
{"seq":1,"event":"build_started","at":1790230560347,"job":"mixed-pipeline","url":"job/mixed-pipeline/16/","build":16}
{"seq":2,"event":"executor_allocated","at":1790230561419,"job":"mixed-pipeline","build":16,"node":"lab-agent","executor":0,"task":"part of mixed-pipeline #16"}
{"seq":3,"event":"executor_allocated","at":1790230561421,"job":"mixed-pipeline","build":16,"node":"built-in","executor":0,"task":"part of mixed-pipeline #16"}
{"seq":4,"event":"executor_released","at":1790230566821,"job":"mixed-pipeline","build":16,"node":"built-in","executor":0,"outcome":"completed","duration_ms":5414}
{"seq":5,"event":"executor_released","at":1790230575792,"job":"mixed-pipeline","build":16,"node":"lab-agent","executor":0,"outcome":"completed","duration_ms":14396}
{"seq":6,"event":"build_ended","at":1790230576172,"job":"mixed-pipeline","build":16,"status":"FAILURE"}
```

## Why it exists

Jenkins publishes no event when an executor is taken or handed back. It will
tell you a build left the queue `ALLOCATED` - an executor was assigned - but
never *which* one, so the only ways to learn where a build ran are polling
`/computer/api/json` while it runs or reading the log afterwards. Neither works
for watching a fleet live.

The stream also has to survive a dropped connection. Every event carries a
sequence number and a bounded backlog is kept, so a client reconnects with
`?since=<last seq>` and is sent exactly what it missed rather than silently
skipping it.

## The events

Four, built on two Jenkins core extension points. An event's name says what
happened; a property appears only when it carries something the name does not.

| event | when | source |
| --- | --- | --- |
| `build_started` | a build began executing | `RunListener` |
| `build_ended` | it finished, with `status` | `RunListener` |
| `executor_allocated` | an executor took up work | `ExecutorListener` |
| `executor_released` | it handed the work back | `ExecutorListener` |

Jenkins also offers listeners for queue movement and for job configuration.
Neither is used: a queue item that never becomes a build is not a build, and a
configuration change is not activity. The cost is that a build cancelled while
still queued produces no events at all - it never became a build - and that
creating or deleting a job is invisible. Adding either back is one small
listener if the need appears.

Properties, all optional except `seq`, `event` and `at`:

| property | meaning |
| --- | --- |
| `seq` | position in the log; what a reconnect resumes from |
| `event` | what happened |
| `at` | when Jenkins says it happened, epoch milliseconds |
| `job` | full job name |
| `url` | path to the job or build, relative to the Jenkins root |
| `build` | build number |
| `status` | the build's result, on `build_ended` only |
| `node` | agent the executor belongs to; `built-in` for Jenkins itself |
| `executor` | executor number on that node |
| `task` | what the executor is working on, as Jenkins names it |
| `outcome` | `completed` or `problems`, on `executor_released` |
| `duration_ms` | how long the executor held the work |
| `problem` | the failure Jenkins reported, when there was one |
| `node_hidden` | set when `node` was withheld for lack of permission |

`at`, `build`, `executor` and `duration_ms` are JSON numbers; the rest are
strings.

## The two events this plugin adds


Both go into the same log as the build events, so one stream carries the whole
story in order, and each is gated on `Item.READ` for the job it belongs to.

`node` is the agent's own name, so it matches what you would write in
`node('lab-agent')`; Jenkins's built-in node has no name and reports as
`built-in`.

## What `outcome` means, and what it does not

Everything published here comes from `ExecutorListener`, which knows nothing
about what kind of job it is serving. That is deliberate: one meaning for every
event whatever ran - a Pipeline `node` block, a freestyle build, a matrix
configuration, or anything else that occupies an executor.

`ExecutorListener` distinguishes exactly two endings, and so does this plugin:

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

## Pairing a release with its allocation

A build can occupy two executors on the same agent - two parallel Pipeline
branches, say. `node` alone is then ambiguous, so both events also carry
`executor`; the pair (`node`, `executor`) is one executor slot. Verified with two branches on one agent: allocations on
`lab-agent#0` and `lab-agent#1`, releases on the same two.

## Flyweight executors are skipped

A Pipeline's own task sits on a flyweight executor for the whole run, occupying
no agent capacity. Its lifetime is the build's lifetime, which `build_started`
and `build_ended` already report, so publishing it would duplicate every
build.

## The stream

The recent history of Jenkins activity is kept in memory, numbered, and served
as newline-delimited JSON:

```
GET /agent-events/stream            everything still retained, then live
GET /agent-events/stream?since=417  everything after 417, then live
```

One JSON object per line: an event's properties, `seq` first. Blank lines are
keepalives, every 15 seconds. Two control lines:

```json
{"type":"hello","epoch":"5f2c...","oldest":1,"latest":418}
{"type":"gap","requested_since":9,"oldest_available":57}
```

`hello` opens every stream. `gap` says the record is incomplete - either the
requested sequence has fallen out of the backlog, or the reader fell too far
behind - so a client learns it missed something instead of quietly missing it.

**`epoch` matters.** Sequence numbers live in memory, so a Jenkins restart
starts them over. A client holding cursor 400 would otherwise ask for "events
after 400" forever and skip the new 1..400. The epoch changes on restart, which
tells the client to drop its cursor; the server also notices `since > latest`
and answers with a gap plus whatever it still holds.

Capacity is 2000 events, or `-Dcom.varjo.jenkins.agentevents.EventLog.capacity=N`.
A reader that falls more than 1000 events behind
(`...EventLog.readerBacklog=N`) is sent a gap and disconnected rather than
allowed to consume memory.

### Who may see what

Filtering happens per reader, in the reading user's own authentication - which
is the thing a published bus cannot do, because a message there carries one ACL
and is dispatched whole or not at all - no way to vary per reader which fields
they may see.

* **`Item.READ` on the job** - without it the event is not sent at all.
* **`Computer.EXTENDED_READ` on the agent** - without it `node` and `executor`
  are removed and `node_hidden` is set, so the reader
  still learns that an executor was taken, just not where.

Jenkins core has no per-agent read permission (`Computer` defines CONFIGURE,
EXTENDED_READ, DELETE, CREATE, DISCONNECT, CONNECT, BUILD and nothing finer), so
EXTENDED_READ is the closest thing to "may see this agent's details". An agent
that no longer exists cannot be checked and is hidden: failing closed is the only
safe way to be wrong about a permission.

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

Almost nothing, and nothing at all unless you ask. The plugin writes no files:
the event log is memory only.

| Level | When |
| --- | --- |
| `FINE` | a client opened a stream: which user, which cursor, how much is being replayed |
| `FINE` | that stream closed: how long it lasted, how many events it received |
| `FINE` | once at startup, that the log is listening on the `job` channel |
| `WARNING` | a reader was disconnected for falling behind - it lost events, so it is said out loud |
| `WARNING` | an event could not be published to the bus |

Client traffic is `FINE` rather than `INFO` because a client reconnects on every
network blip and the system log is read at `INFO`. To follow clients, add a
logger for `com.varjo.jenkins.agentevents` at `FINE` in *Manage Jenkins > System
Log*:

```
FINE  c.v.j.a.StreamRootAction#doStream: stream opened for admin (since=none, replaying 0)
FINE  c.v.j.a.StreamRootAction#doStream: stream closed for admin after 54s, 9 events sent
```

Two details worth knowing. `?probe=1` requests - which is how a client asks
whether this endpoint exists at all - get the `hello` line and nothing else: no
reader attached, no stream held, no log entry, because it is not really a
connection. And a client that dies without closing its socket is reaped within
about two keepalive intervals, roughly 30 seconds: a one-byte keepalive to a
dead peer is buffered locally, so the failure only surfaces on the write after
that. Each live stream holds one request thread until then.

## Build and install

```sh
mvn -DskipTests package          # -> target/agent-events.hpi
```

Then *Manage Jenkins > Plugins > Advanced > Deploy Plugin*, or drop the `.hpi`
into `$JENKINS_HOME/plugins/`. A fresh install needs no restart; replacing a
loaded version does.

**No plugin dependencies** - `Plugin-Dependencies` is absent from the manifest
and everything comes from Jenkins core. Built against Jenkins 2.528.1, two LTS
lines below ci-sandbox, so it loads there and on anything newer.

Verified on Jenkins 2.568.3 against a real inbound agent, for: a Pipeline across
two different agents, two parallel branches on one agent (distinguished by
`executor`), freestyle builds that pass and fail, an aborted build, replay across a disconnect, replay across a Jenkins
restart, and the agent-name gate with two accounts.
