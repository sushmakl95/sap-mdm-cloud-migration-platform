# Migration Lifecycle — 5-Phase Cutover

## Why not just cut over?

The tempting approach: stop the old system on Friday night, run a migration script, start the new system Monday morning. This fails for enterprise SAP migrations because:

1. **SAP MDM is rarely the source of truth for one thing** — it's read by 200+ downstream workflows, each with different reliability expectations
2. **Data bugs often take days to surface** — a pricing regression might only hit during Tuesday's promo run
3. **Rollback is expensive** — once you've cut writes to the new system, the old one is out of date the moment the first write happens

The 5-phase approach trades calendar time for safety. In our experience, a full SAP MDM migration takes **4-8 weeks** using this method — but the cost of a failed big-bang is typically 6-12 months of trust rebuilding.

## The 5 phases

### Phase 1 — Shadow Read (1-2 weeks)

**State:** Old system writes and reads. New system loaded via daily batch migration. Consumers unaffected.

**Goal:** Verify the NEW system's data matches OLD under zero-consumer-load conditions.

**Activities:**
- Nightly `batch-migrate-sm` runs across all tables
- 5-layer reconciliation runs every morning, results to Grafana
- Business stakeholders can query new system "read-only" for ad-hoc validation
- Fix data issues iteratively — these are pipeline bugs, not system bugs

**Exit criteria:**
- 7 consecutive nights of green reconciliation across all tables
- Business team sign-off on 10+ sample records per key table

**Rollback:** Trivial — consumers never touched new system.

### Phase 2 — Dual Write (1-2 weeks)

**State:** Writes go to BOTH systems in parallel. Reads still from old.

**Goal:** Verify synchronous write path works at production write load.

**Activities:**
- Services that write to SAP are updated to also write to the new system (via CDC for non-invasive changes, or via dual-write middleware for major paths)
- Write-time diff monitor: every write is checked against both systems; any mismatch → investigate
- Latency budgets: new system writes must not block old system writes (use fire-and-forget with retry queue)
- CDC pipeline feeds both: DMS catches changes and replicates to new system

**Exit criteria:**
- <0.01% write mismatch rate for 14 consecutive days
- P99 dual-write latency < (old P99 × 1.1)
- No production incidents attributable to dual-write

**Rollback:** Disable new-system write path. Old system remains authoritative.

### Phase 3 — Shadow Read on AWS (1-2 weeks)

**State:** 10% of read traffic is routed to NEW system. Results compared to OLD system results.

**Goal:** Verify analytical + operational queries return identical results at production read load.

**Activities:**
- Application-level traffic split (feature flag framework) — 10% of customers / 10% of requests
- Request-level dual-query: for the sampled percentage, query BOTH systems, return OLD's result, log the diff
- Performance test: new system must meet or beat P99 latency of old

**Exit criteria:**
- <0.01% read diff rate for 14 consecutive days
- New system latency ≤ old system at the 10% level
- No customer complaints tied to mismatched results

**Rollback:** Revert traffic split to 0%. Customers unaware.

### Phase 4 — Primary Read (2-4 weeks)

**State:** 100% of reads come from NEW system. Old system still writes, kept as fallback.

**Goal:** Validate new system at full read load.

**Activities:**
- Gradual ramp: 10% → 25% → 50% → 75% → 100% over 2 weeks
- Each step: 48h bake time before next increment
- Old system kept in sync via dual-write + CDC — RPO ≤ 5 minutes
- Any incident → cut back via traffic split within 5 minutes (RTO)

**Exit criteria:**
- 30 consecutive days at 100% read traffic
- No rollback incidents
- Query cost and performance acceptable to stakeholders

**Rollback:** Traffic split cuts to 0% new-system reads. Old system re-becomes authoritative.

### Phase 5 — Decommission (ongoing)

**State:** New system is authoritative for both read and write. Old system frozen, kept as cold archive.

**Activities:**
- Stop writing to old system — new system becomes write-leader
- Old SAP system switched to read-only mode (the ABAP-level SE06 lock)
- Retain old system for 12-24 months for regulatory / forensic use
- Eventually: snapshot, archive, and decommission

**Exit criteria (to proceed to full decommission):**
- 12 months stable at Phase 5
- Legal/compliance sign-off on archive retention
- Finance cost comparison: new system TCO < old system maintenance

**Rollback:** At this point rollback means "fully re-migrate in reverse" — which takes another 4-8 weeks. This is by design: we only reach Phase 5 after extensive confidence.

## Phase transitions — decision authority

| Transition | Who approves |
|---|---|
| Phase 1 → 2 | Data Engineering tech lead |
| Phase 2 → 3 | DE lead + consuming service owner |
| Phase 3 → 4 | DE lead + service owners + DevOps |
| Phase 4 → 5 | All above + Director of Engineering |

## Automation

Not everything here is automatable. Automatable:
- Phase 1 batch migration + reconciliation (full)
- Phase 4 traffic split ramp (via feature flags + automated canaries)

Not automatable, still manual:
- Phase 2/3 application-level dual-write implementation (depends on each service)
- Phase 5 decommission decision (requires human judgment on risk)

## Shared reference state

Each phase's progress is tracked in a single canonical doc — typically Confluence for SAP environments. Include:

- Current phase
- Date entered phase
- Target exit criteria status (check per-item)
- Open issues blocking next phase
- Last reconciliation run result
- Traffic split percentage (Phases 3-4)
- Next decision point date

## References to our implementation

- Phase 1 & 2: `modules/stepfunctions` + `batch-migrate-sm`
- Phase 2 CDC backbone: `modules/dms` + `src/migration/cdc/`
- Phase 3/4 traffic split: **NOT in this repo** — lives in each service's feature-flag config
- Rollback Step Function: `modules/stepfunctions` + `rollback-sm`
