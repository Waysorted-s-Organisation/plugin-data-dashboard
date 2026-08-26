# Audit — plugin telemetry changes and their linkage to the dashboard

**Scope:** the 15 commits merged into `feature/plugin-analytics-dashboard` since the last full audit baseline (`bbdc3b5` → `34cbcf5`), plus how each new server-side number reaches a pixel on a dashboard page.
**Baseline:** local branch is level with `origin/fix/auth-funnel-classification`, whose content is byte-identical to `origin/feature/plugin-analytics-dashboard` — there is nothing newer to pull. `npm test` was 53/53 green before this change and is 58/58 after.

---

## What the new commits actually changed

`src/product-intelligence.js` grew from ~250 to ~1250 lines. The whole of that growth is one idea: **stop deriving product truth from billing records alone, and read the plugin's own telemetry as evidence.** Concretely it added an identity-link resolver, a per-identity active-days aggregation, a per-identity top-tool aggregation, a per-tool telemetry aggregation, signed-out visitor rows, a reporting timezone, and a short-lived core cache.

The linkage into the UI is sound and complete for what was built — every new server field has a consumer:

| Server | Endpoint | Page |
|---|---|---|
| `pluginActivityDaysByIdentity` → `distinctDays` | `/summary`, `/users`, `/lifecycle` | returning-user counts, `returning` segment, cohort returns |
| `pluginTopToolByIdentity` → `topTool` | `/users` | "Latest tool" column |
| `toolActivityFromTelemetry` | `/tools` | the `coverage: "telemetry"` cards |
| `identityLinksByAnonymousId` | all of the above | pre-signup history stitched onto the account |
| `anonymousVisitorRows` | `/users` | signed-out visitor rows |
| `telemetryHealth` | `/summary`, `/data-health` | coverage chips |

No orphan fields, no page reading something the server stopped sending. The retired-page redirects and the `NON_TOOL_SURFACES` filtering are correct.

---

## The reported defect: jobs read 0 / untracked

**Root cause.** The users table's job column is `completedJobs`, and it was `facts.committed.length` — committed rows in `usagereservations`, and nothing else. Every other column on that page had been taught to read telemetry during these 15 commits. This one was not. So a person reads as **0 jobs** whenever:

- the run finished but the reservation never reached `committed` (still held, released, expired);
- the reservation exists but lost its `user` link (`data-health` already counts these as `reservationsWithoutUser`);
- the tool charged nothing at the time of the run;
- they were signed out — `anonymousVisitorRows` hard-coded `completedJobs: 0`, so every visitor row said 0 by construction.

**Second cause, for the "untracked" half.** `completedJobs` was a *lifetime* number while `pluginTopTools` was aggregated over the *selected window only* (`loadCore` passed `range.currentStart..range.now`). One row therefore described two different periods: someone last active five weeks ago showed a non-zero lifetime job count next to "No tool use recorded". `lastActiveAt` had the same split — a lifetime `lastLoginDate` combined with a window-truncated plugin timestamp.

**Third cause, identity.** `src/server.js:282` substitutes the literal string `"unknown-device"` when the plugin sends no device id. The read path treated that placeholder as a real identity, so every unattributed device merged into one "visitor" — and because `identityLinksByAnonymousId` also indexes device ids, a single `identity_linked` event without a device id would map `"unknown-device"` to a real account and pour all of that traffic onto one person.

### What was changed

- `pluginTopToolByIdentity` → `pluginToolUseByIdentity`, which returns per-identity **completed tool jobs** alongside the top tool, from one aggregation pass. `tool_action_completed` is the count; `feature_used` is a fallback for tools that report a discrete invocation instead of a start/complete pair — a fallback, never an addend.
- Tool use is now indexed over the **lifetime lookback and the window**, matching the two kinds of column. The lifetime columns stop disagreeing with each other.
- `userFacts` gains `observedJobs` / `observedJobsInRange`; rows gain `creditedJobs`, `observedJobs`, `completedJobs` and `completedJobsSource` (`"credited"` | `"observed"` | `null`).
- **The two counts are never summed.** Once a tool charges credits, one run emits both a committed reservation and a `tool_action_completed`; adding them would report one job as two. Committed reservations stay authoritative when they exist; telemetry answers only when they do not.
- `anonymousKey()` rejects the ingest placeholders, in the activity aggregation, the tool-use aggregation and the identity-link resolver.
- `engaged` now counts work, not billing. `toolKeys` includes the observed tool, so the "Tool used" filter can find telemetry-only tools. `activityAvailable` folds in the tool-use aggregation, so a read failure renders "Not measured" rather than a confident 0.
- UI: column renamed "Credited jobs" → "Tool jobs", each number labelled with its provenance, the drawer gains a Tool jobs stat and explains an empty credited timeline, and the "no credits charged" tag becomes "from plugin activity".
- `scripts/diagnose-user-coverage.js` gains a **"Why job counts read zero"** section (see below).
- Five new tests in `test/tool-jobs.test.js` pin all of the above, including the no-double-count rule.

### Run this against production first

```bash
node scripts/diagnose-user-coverage.js
```

It now prints the reservation status distribution, reservations with a missing or dangling `user`, the credit-ledger reasons actually present, and — the important one — **telemetry identities that do not join to any `users` document**. The dashboard joins telemetry to accounts on `users._id` only. If the plugin sends anything else in `user.userId` (an auth subject, an email on newer builds, a Waysorted profile id), that counter will be non-zero and those people read as zero jobs no matter what the read path does. That is a plugin-side fix, not a dashboard one, and this is the number that tells you which case you are in.

---

## "Signed-out visitor" rows under a hard login gate

The plugin cannot be used without logging in, yet the users table is full of signed-out visitors completing real jobs in real tools. Two production facts explain it, and both are plugin-side realities the dashboard had modelled wrongly.

### 1. The device id identifies a launch, not a machine

The visitor ids in production are `device_1787730142122_8`, `device_1787726317271_j`, `device_1787715258581_s`. Decoding the embedded epoch:

| id | minted | row said last active |
|---|---|---|
| `device_1787730142122_8` | 2026-08-26 07:42:22Z | 10 minutes ago |
| `device_1787726317271_j` | 2026-08-26 06:38:37Z | 1 hour ago |
| `device_1787715258581_s` | 2026-08-26 03:34:18Z | 4 hours ago |

Each id was minted at the moment of its own row's activity, and the gaps between the ids match the gaps between the rows (63.7 min against "10 min vs 1 hr"; 3.07 hr against "1 hr vs 4 hr"). A device id that persisted per install would carry an old timestamp shared across rows. **The plugin generates it at launch and never persists it.**

That directly falsifies commit `1d2c00f` ("Anchor anonymous identity on the device, not the pseudonymous id"). The device is not an anchor — it is the least stable key in the payload.

### 2. Events start before authentication resolves

`normalizeUser` marks an event anonymous when it carries no `userId`/`email`. Events flow from plugin boot, before the stored token is validated and the user object populated, so the first events of *every* launch are identity-free. Nothing stitched them to the account.

`identity_linked` cannot cover this. It fires when a signed-out session *signs in* — which under a hard login gate never happens, because the person is already authenticated and the plugin simply does not know it yet.

### The fix

`accountsBySessionId()` maps each `sessionId` to the account that appeared anywhere in it, over all time. The session id is the strongest join available: written on every event, scoped to one launch on one machine, and if any event in it carries an account then all of it does. Both aggregations now group by session and resolve session-first, anonymous-id-second. A session containing two different accounts is left unattributed rather than guessed at.

Consequence: a signed-in person's pre-auth events, their jobs and their active days move onto the account, the phantom visitor disappears, and launches on different days register as returns even though every launch had a different device id.

What remains anonymous is a genuine measurement limit — someone who opened the plugin and never signed in, counted once per launch because no stable key exists for them. `identityHealth()` measures it (distinct devices ÷ distinct sessions; at or near 1 means per-launch ids) and states it on the Data Health page rather than letting it quietly inflate a headcount. **The real fix is plugin-side: persist the device id in `figma.clientStorage`.**

## Other inconsistencies closed in the same pass

- **Activation meant two different things.** The users table counted any tool use; `periodSummary.activatedUsers` and the journey cohort table still counted only a first *credited* reservation. All three now agree, and the page copy on Summary and Journey says so.
- **The summary's "Completed tool jobs" was committed reservations only** — the same under-report the users column had. It now folds in observed jobs, deduplicated per account with `max`, never summed. Reservations with no user link are added separately (they are real jobs that cannot be de-duplicated), as is signed-out work.
- **The tools page reported only billable work.** Added a "Jobs seen in activity" total and a per-tool observed count beside the credited one. A large gap between the two is now the visible signal that reservations are not landing.
- **Period boundaries were counted twice.** The three telemetry aggregations matched `$lte: end` while everything else used `< end`, so an event on the boundary instant landed in both the previous and current window and skewed every change percentage. All are now half-open.
- **"no credits charged"** was printed against Palettable, File Importer and Frames to PDF on rows with 45–63 credited jobs. The flag meant "this row's *top tool* was resolved from telemetry", not "this tool is free". Relabelled "from plugin activity".

---

## A job means a run that finished

Events arrive for everything the plugin does; only a subset is evidence that work completed. `tool_action_completed` is the only event that says so, so it is the only one counted — credited or not.

`feature_used` is deliberately **not** counted and **not** substituted. It marks that a feature was invoked, not that the work it started ever finished, and one run can emit several. Substituting it would put a different number under the word "jobs". It is still tallied, so a tool that reports invocations and starts but never a completion can be *named* as an instrumentation gap: `productTools` returns `summary.toolsWithoutCompletions`, the Tools page shows a "Not reporting completions" card, and `diagnose-user-coverage.js` prints an opened/started/completed/failed matrix per tool. **If a tool appears there, its job count is unmeasurable until the plugin emits `tool_action_completed` on success — that is a plugin fix, not a dashboard one.**

## Catching the real identity

Three separate places were throwing away identities that had actually been sent.

**1. The ingest blanked ids on the strength of a flag.** `normalizeUser` wrote `userId: isAuthenticated ? inferredId : null` and the same for `email`. The flag comes from the plugin's own auth state machine, which lags the token it already holds — so the opening events of a launch arrived as `isAuthenticated: false` *while carrying a real account id*, and that id was deleted on the way in. Presence of an id or an email is now the evidence, and the flag can only add to it, never subtract.

**2. The read path keyed on the account id first.** `$ifNull: ["$user.userId", "$user.email"]` meant that when the plugin's notion of the account is not the backend `users._id` — an auth subject, a provider id — the row was filed under a key nothing resolves, *even when the same event carried a perfectly good email*. The order is now email first, account id second. Email also converges the two eras of stored events, since rows written before the identity fix hold an email in `user.userId`.

Because each row is now filed under exactly one key, the per-user indexes are disjoint, so jobs are **summed** across the id and email indexes rather than one shadowing the other, and the latest tool is whichever of the two happened later.

**3. Sessions.** Covered above — the whole launch belongs to whoever the launch belonged to.

## The comped subscription

`Wallet missing` was accurate about the wallet and wrong about the person. The `subscriptions` collection was loaded, indexed, and returned on the profile — but nothing read it to decide commercial standing. That came only from `userbillings.subscriptionStatus`. A subscription granted directly in the backend writes a subscription record without necessarily initialising a wallet, so a real subscriber rendered byte-for-byte identically to someone who had never opened a checkout: no plan, no credits, stage `activated`.

`activeSubscription()` now reads the collection itself. A status in `active / cancel_scheduled / trialing / past_due / grace` counts; a status in `cancelled / expired / payment_pending / failed / created` does not; anything else falls back to whether `currentPeriodEnd` is still in the future, so a hand-made record carrying a status this dashboard has never seen still resolves. Such a user is now `customer`, carries `subscriptionStatus`, `subscriptionPlan`, `subscriptionSource: "subscription_record"` and `subscriptionEndsAt`, and the credits cell reads "Wallet missing — pro_monthly, granted without a wallet" rather than implying they have nothing. The missing wallet is still reported: it is a real backend gap.

---

## Background services were being counted as finished work

Opening the plugin starts a fleet of background services — sync, credit refresh, license checks, prefetch — and they announce finished work the same way a user's run does: a `tool_action_completed` carrying whatever surface the plugin is on. Counting those made the job column measure **how often someone opened the plugin**, not how much they got done. Every launch added jobs.

The distinguishing fact is not in the event's name. It is that **nobody opened the tool**. A person has to open a tool to run it; a background service does not.

**The rule:** a completion counts as a job only when the same session shows a `tool_opened` for that same tool at or before it.

This is deliberately *not* a denylist of background action names. Those are the plugin's internal vocabulary, they change without notice, and guessing one wrong silently deletes real work. The rule reads only what is already recorded.

Two escape hatches keep it from over-deleting:

- A tool that emits **no** `tool_opened` anywhere in the window cannot be judged by the rule, so all of its completions are counted and the tool is **named** as ungated on Data Health. Over-reporting beats deleting real work.
- Opens are looked up from **twelve hours before** the window, so a session straddling a period boundary does not lose the open that justifies its completions.

The open index is resolved per `(session, tool)` rather than inside the identity grouping, because the two are not aligned: the head of a launch is unidentified and the tail is not, so an open and the completion it justifies can land in different identity groups for the same session.

Both the users page and the tools page apply the **identical** rule — a cross-page test asserts they agree — and the amount filtered is reported rather than silently applied: `GET /api/operations/tools` returns `summary.backgroundCompletions`, Data Health shows it under **Tool jobs**, and `diagnose-user-coverage.js` prints a per-tool counted/background/ungated table plus how long after session start the background ones fire. A pile-up in the 0–5s bucket is the launch burst.

## Making the pages agree

Each page had grown its own definition of the same words. Fixed, and pinned by `test/cross-page-consistency.test.js`, which seeds one realistic world and asserts every page reports it identically:

- **Job counts now use `max(credited, observed)` everywhere.** The users page briefly preferred the credited count outright while the summary took the larger — so a user with 2 billed runs and 5 observed showed 2 on one page and contributed 5 to the other. The larger is the best available lower bound on distinct runs; the sum is never taken, because a credited run emits both records. A row where observed exceeds credited reports source `combined` and reads "3 billed, rest seen in activity".
- **Activation means one thing.** Any tool use, on the users table, the summary metric, the journey funnel and the journey cohort table alike.
- **The signed-out visitor from a launch that later authenticated no longer exists**, so the row count on the users page matches the account count everywhere else.

## Verification

`npm test` — 73 tests, green on four consecutive full runs. Test files now run one at a time (`--test-concurrency=1`): every file drives the same module-level Express app and repoints `process.env.MONGODB_URI` at its own in-memory database, so parallel execution let one file answer another's request from the wrong database. One unexplained double-failure was observed before that change and has not recurred in six runs since.

**Production is not verified.** There are no production credentials in this checkout, so nothing here has been run against real data. `node scripts/diagnose-user-coverage.js` against production is what closes that gap, and its per-tool completion matrix should be read first — every job number depends on `tool_action_completed` actually being emitted.

---

## Still open — not touched by this change

Ranked. The first two predate these commits and are unchanged since the last audit.

1. **`readAuthGate` fails open when unconfigured.** `src/server.js:171` returns `next()` when `DASHBOARD_BASIC_AUTH_USER`/`PASS` are empty, and `.env.example` ships both empty, so a deployment that follows the README's copy-the-example setup serves the entire customer database and the privileged newsletter proxy to anonymous callers.

   **Production is not in that state.** Verified against the live deployment after this merge: `GET /api/operations/summary` answers `401` with `WWW-Authenticate: Basic realm="Waysorted Operations"`, which is reachable only on the branch where both credentials are configured. An earlier draft of this document said the live dashboard was exposed; that was wrong.

   What remains is that the failure is silent. Clearing either variable — a mistyped rename, a lost environment, a fresh preview project — reopens everything with no error, no log line and no startup check. Fail closed instead: refuse to serve with a `503` unless an explicit `ALLOW_UNAUTHENTICATED=true` opt-in is set, put an obviously-invalid placeholder in `.env.example`, and add a test that clears both variables and asserts the operations APIs are not served.
2. **One failed Mongo connect bricks the process.** `src/db.js:61` and `:88` assign `cachedClient` *before* awaiting `connect()`; a rejected connect leaves a poisoned client that every later call reuses. The new `onDbClose` hook did not address this.
3. **Unbounded reads.** `loadCoreUncached` pulls nine collections in full on every cache miss, and `productTools` independently re-reads all of `usagereservations`. The 30 s `coreCache` is per-process, so each serverless container pays it separately. This fails suddenly rather than gradually.
4. **Dead branch.** `$ifNull: ["$deviceId", "$user.anonymousId"]` in both aggregations never reaches the second operand — the ingest always writes a `deviceId`. Harmless, but it reads as a fallback that does not exist.
5. **Plugin-side, and the only real fix for signed-out visitors:** persist the device id in `figma.clientStorage`, and hold the first events of a launch until authentication resolves (or replay them once it does). The dashboard now compensates for both; it should not have to.
