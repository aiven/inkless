# Council review of v3

Review of [v3](../design.md), run on 2026-11-20. The chair reviewed the design independently before the council convened. The council then ran three rounds: blind analysis, anonymized cross-examination, and final positions. Raw round outputs are in the job scratch directory and aren't kept here.

## Council verdict

### Problem

Is the v3 diskless metadata coordinator design correct and ready to hand to implementation against its own data-loss bar and performance bar, and did it close the findings of the two v2 reviews?

### Council composition

The panel had five members: Ada (formal protocol correctness), Taleb (tail risk and the data-loss bar), Feynman (mechanics checked against source), Torvalds (shippability and upstream cost), and Meadows (feedback loops and operator levers). They're the same five as the v2 review, so the results compare across versions. Full mode ran three rounds. The restate gate was folded into round 1 instead of running as its own round.

### Chairman

Chairman: Claude Opus 5.5 (anthropic · opus). Selection rationale: overridden by the user, who asked the coordinator to chair. Single provider, so the Chairman shares a provider with every panel member. The Chairman also wrote an independent review before round 1. Those findings stayed out of the members' prompts, so round 1 was blind to them. The verdict reconciles them in the following sections.

### Provider routing

Default models (single provider). Ada, Feynman, Torvalds, and Meadows ran on sonnet, and Taleb on opus, per agent frontmatter.

### Consensus and agreement

v3 is a large step up from v2. The fence, floor, and release lattice is sound for single faults. The replication mechanics hold in source: the `acks=-1` append, the high-watermark gate under the minimum ISR, and flush placement. Every v2 finding maps to a decided ticket. Members agree that the leases, fences, and release reports carry weight for the data-loss bar, and aren't an optimization. Torvalds withdrew his round 1 proposal to defer them, citing design.md:470.

v3 isn't ready to hand over as written. All five members back **handoff after named fixes**, not another design round. Five fixes block handoff:

1. **Re-derive the overlay cap (all five, plus the chair).** design.md:207 sizes the cap at `4 × B`, but fence batches and release reports are bounded per tracking partition, at up to `N − 1` each. At the default `B=3`, `N=8`, the legitimate peak is about 20 entries against a cap of 12. That contradicts the claim that the cap "trips only on a bug" and the 60-second outage test at design.md:1703. Derive the cap from the in-flight limits, for example `2B + 4N`, and show the arithmetic.
2. **Align the model claims with the model bounds (Ada, Taleb, Torvalds; checked by the chair).** design.md:1814 says "1 or 2 epochs". Every free `FencesFloors` config uses `MaxEpoch = 1` and `MaxFaults = 0`. Only the pinned scenario G runs two epochs, and lost-tail faults run only in `TrackingEntries` and the scenarios, with one fault each. The overlay with its drop-suffix rule (design.md:206, 392, 400) and the tracking-leader change (design.md:588) aren't modeled. Either add those runs or narrow the prose and the accepted-risk rows to what the model checked.
3. **Follower flush on the shared fetcher thread (Feynman, plus the chair).** `UnifiedLog.java:1300` flushes inside the append, and the follower append runs on the replica fetcher thread. `AbstractFetcherThread` processes a response's partitions serially, and `num.replica.fetchers` defaults to 1, so classic partitions share the thread. design.md:237 rejects SQLite on that thread for the same delay that design.md:79 then accepts for five syncs per flush. "Two flushes in series" is a lower bound. Count the follower cost as `k` flushes per fetch response, add fetcher busy time to the benchmarks, and decide on a dedicated fetcher for `__diskless_metadata`.
4. **The leadership handoff for a leader that can't rebuild (Torvalds, plus the chair).** Open items at design.md:1936 and the lines after it say that "none of them changes the design", then name a missing controller request "that the design needs". A leader with no covering snapshot keeps leadership while an in-sync follower may hold a good database. The design has to own that mechanism.
5. **Restate the data-loss bar honestly (Taleb).** Clause 3 exempts the apply-bug and lost-tail rows by parity of cause, but clause 3 judges consequence. A wrong release permanently deletes another partition's object (design.md:1625), and nothing bounds how many. Pull the deletion-rate cap and missing-key log (design.md:1903 and 1904) into v3. Restate clause 3 as bounded by the cap, and move the lost-tail row under clause 2 with an explicit exception.

### Vote tally

- `after named fixes`: 4.125, from Ada [1.5× domain seat, med → 1.125], Taleb [med → 0.75], Feynman [med → 0.75], Torvalds [med → 0.75], and Meadows [med → 0.75].
- `another design round`: 0
- `ready now`: 0
- W_total 5.5 · threshold 3.667 · **cleared: consensus on handoff after named fixes**

### Acceptable compromises

- The performance bar stays a prediction at handoff. Members accept starting implementation before any benchmark runs. Torvalds and Taleb want the laptop proxy run early, but not as a gate on the design.
- Fixed `N` stays. No member asked for partition splitting in v3, even though `N` caps growth and the state-size limit per partition for the life of the cluster.
- The footer recovery and the empty restart stay future work. Taleb wanted a break-glass procedure for a lost replica set but dropped it from his final blocking list.

### Kill criteria

- If the extended model, with two epochs, one fault, the overlay, and a tracking-leader change, finds a delete that crosses partitions by 2026-12-18, the verdict is invalidated, and v3 goes back for a design round on the deletion protocol.
- If the laptop-bench proxy at 3 brokers with `N` of 1, 2, 4, and 8 misses produce P99 within 1.1 times Postgres at every `N` by 2026-12-18, the verdict is invalidated, and the flush and fan-out design reopens before M1 code lands.
- If the re-derived overlay cap still rejects commits in the 60-second participant-outage test at `B=3`, `N=8` in the first shard tests, the cap needs a design change, not a constant.

### Concrete next step

Write a v4 map in `.scratch/` by 2026-11-27, with one ticket per blocking fix. Include its failure-mode row and named test, as the v3 map did.

### Unresolved questions

- Does the follower fsync on the shared fetcher thread actually hurt classic replication at the target load, or does fetch-response batching keep `k` small? Nobody measured arrival patterns.
- Does a deletion-rate cap throttle a legitimate retention burst? Taleb concedes it might. The cap needs a burst allowance tied to the retention-burst benchmark.
- Should gateway routing default to on? Torvalds says off until M4. Meadows says fan-out drives the bar, so routing should stay on and be validated by the gateway pairs. The user decides.
- Should routing get hysteresis? Meadows makes it blocking: a dwell for returning brokers, and a flapping-broker benchmark. Torvalds says it breaks the rule's pure, image-only property. Unresolved.
- How many `CoordinatorRuntime` threads does the coordinator get? Neither design.md nor spec.md says. With fewer threads than led shards, one shard's fsync blocks another's commits (chair finding, not raised by members).

### Recommended next steps

1. Add a milestone plan to spec.md: M1 is the commit and read path with deletion disabled, for benchmarks only and not GA. M2 adds snapshots, M3 adds leases, fences, and releases before GA, and M4 adds gateways (Torvalds).
2. Judge `FileCommitTime` P50 and P99 against Postgres with an absolute ceiling, next to `TotalTimeMs`, which the 250 ms close dominates (Feynman).
3. Add a leading saturation signal for coordinator partitions, such as parts per leader against a benchmarked ceiling, with an alert threshold (Meadows).
4. Run the cache storm at 48 and 96 brokers. At `8B` reads and about 5 ms per scan on 4 threads, queue time reaches the 1000 ms read deadline at `B=96` (Feynman).
5. Add an AZ-loss run that counts tracking switches, range fences, and `PendingFenceRangeCount` drain time (Meadows).
6. State in the deployment docs that brokers from `N` to `2N − 1` per rack are spares that add no diskless capacity, and that `N` times the per-partition row limit caps retained batches for the cluster's life (chair).
7. Specify the coordinator runtime's thread count (chair).
8. Set `segment.bytes` on the topic, and derive the 4 MiB group and "log near 2 GiB" bounds instead of asserting them (Feynman).

### Key insights by member

- **Ada**: The lattice is sound, but "no invariant fails" outruns the bounds. Every exhaustive run uses one epoch and no faults, and the retry logic that depends on the overlay is unverified.
- **Taleb**: v3 is honest about consequences, but clause 3 was amended to fit the rows it couldn't fix. The one irreversible cross-partition action, a WAL delete, has no brake.
- **Feynman**: The mechanics hold in source. Three numbers come from the wrong variable: the follower flush count, the overlay cap, and the read queue at scale.
- **Torvalds**: Hand over v3 as milestones. Eleven new `INTERLEAVED` controller and storage files raise the cost of every upstream sync.
- **Meadows**: The routing loops are bounded on paper but open-loop in operation. Assignment has no load input or hysteresis, and `N` has only lagging signals and no runtime lever.

### Points of disagreement

- Gateway routing default: off and deferred to M4 (Torvalds), or on and validated by benchmarks (Meadows).
- Routing hysteresis: blocking (Meadows), or harmful to the rule's purity (Torvalds).
- Deletion-rate cap in v3: blocking (Taleb), or future work with a named trigger (Torvalds).

### Minority report

No member flagged a dealbreaker. Taleb's position is the sharpest: the extended model run is a falsifier. If it finds a cross-partition delete, he moves to another design round. If it finds none and the rate cap has an owner in M1, he moves to "ready now".

### Epistemic diversity scorecard

- Perspective spread (4 of 5): formal, tail-risk, mechanistic, pragmatic, and systems lenses found largely non-overlapping issues. The overlay cap was the one shared finding.
- Provider spread (1 of 5): single provider.
- Evidence mix: about 45% mechanistic, 20% empirical, 25% strategic, and 10% heuristic.
- Convergence risk: medium. All stances matched at medium confidence. Several blocking findings were independently verified against source or the model (the overlay cap, the model bounds, and the fetcher flush), but every member shares one model family.

### Chair reconciliation

The chair's independent findings matched the council on the overlay cap, the follower fsync on the fetcher thread, and the open-item leadership gap. The council added the model-bounds gap, which the chair verified, the data-loss bar argument, the routing hysteresis, the milestone cut, and the read-queue scaling. Two chair findings no member raised: the unspecified `CoordinatorRuntime` thread count, and the spare-broker band from `N` to `2N − 1` per rack. Both are in Recommended next steps, not in the blocking list, because only the chair raised them.

### Follow-up

After acting on this verdict, revisit: Was this verdict useful? Did the v4 map close the five blocking fixes? What did the extended model run find?

---

### Session metadata

```
schema_version: 1
mode: full
panel_size: 5
rounds_run: 3
enforcement_calls: 2
enforcement_breakdown: {dissent_quota: 0, novelty_gate: 0, agreement_check: 2, anti_recursion: 0, missing_stance: 0}
chairman_failed_fallback: no
tools_used: yes
input_tokens_estimate: ~unknown
output_tokens_estimate: ~unknown
duration_seconds: ~unknown
provider_count: 1
fallbacks_triggered: none
```
