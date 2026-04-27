# TODOS

Captured from `/plan-ceo-review` sessions. Items here are decisions deferred
from active planning — not implementation tasks.

---

## [P1] FSEvents + ExFAT reliability resolution (Phase 3 blocker)

**Context:** Phase 3 plan (.context/ceo-plan-live-index-phase3.md) Reviewer
Concern #0. FSEvents is unreliable on non-journaled filesystems (ExFAT, FAT32,
NTFS). Prism's README benchmarks on a 2TB USB ExFAT drive — ExFAT is a
primary target.

**Decision needed:**
- **E (self-heal):** single FSEvents code path; on HistoryDone, if replay is
  empty but `files.date_modified` suggests activity, trigger full scan. Leaves
  in-session ExFAT changes undetected until next mount.
- **E+poll:** self-heal + 5min poll fallback only on volumes flagged as
  inconsistent.
- **A (two paths):** filesystem-detect at coordinator init; APFS/HFS+ use
  FSEvents, ExFAT/FAT/NTFS use polling.

**Effort:** S (thinking time + test on a real ExFAT drive to verify behavior).
With CC+gstack: S (one hour of exploratory testing + one decision).

**Priority:** P1. Blocks Phase 3 implementation.

**Depends on / blocked by:** Nothing. Independent decision.

---

## [P2] Phase 4 (ID3 extraction) — CEO review for priority vs Phase 3

**Context:** Phase 3 CEO review outside voice argued Phase 4 is higher user
value than Phase 3 (ID3 unlocks semantic/mood search, which is the 12-month
vision). Builder chose Phase 3 first based on self-identified staleness pain.
Post-Phase-3 shipping, re-evaluate whether Phase 4 or another direction
(FSEvents polish, Phase 5 filters) wins.

**Decision needed:** What's the next phase after Phase 3 ships?

**Effort:** S (one CEO review cycle).
With CC+gstack: S (~30min review).

**Priority:** P2.

**Depends on / blocked by:** Phase 3 shipping (or explicit decision to swap order).

---

## [P2] DESIGN.md — document Phase 3 design vocabulary

**Context:** Phase 3 introduces new visual vocabulary (volume state encoding,
pulse dot, polling badge, error banner styling, offline row treatment). These
decisions were made in `/plan-design-review` and recorded in
`.context/ceo-plan-live-index-phase3.md`. They should be extracted into
`DESIGN.md` so Phase 4+ stays consistent instead of drifting.

**Content to capture:**
- Color tokens: blue/red/orange/secondary mapping to state semantics
- SF Symbol vocabulary: externaldrive.fill/externaldrive (fill vs outline = connection)
- Caption copy voice: status language, utility framing, not marketing mood
- Corner radius: 6pt (macOS standard)
- Accessibility rules: combined labels, isAlert trait on banners
- Animation budget: pulse 2s, fade-out 300ms, meter refresh 5s

**Effort:** S (captures existing decisions, no new thinking).
With CC+gstack: S (~20min copy-paste from plan + format).

**Priority:** P2.

**Depends on / blocked by:** Phase 3 implementation (must see decisions land before documenting).
