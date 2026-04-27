# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Prism** is a high-performance macOS desktop search utility designed as a native alternative to Windows' "Everything" search tool. It uses a tiered database architecture (DuckDB + SQLite FTS5 + in-memory cache) to enable instant filename search (<5ms) for massive audio libraries across multiple external drives.

**Target:** macOS 14.0+ (Sonoma)
**Stack:** Swift (AppKit/SwiftUI Hybrid), DuckDB, SQLite FTS5 (via GRDB.swift)
**Primary Use Case:** Indexing and searching 5M+ audio files across external drives

## Development Commands

- Build: `xcodebuild -scheme prism -configuration Debug`
- Test: `xcodebuild test -scheme prism`
- Run: Open `prism/prism.xcodeproj` in Xcode and press Cmd+R
- Clean DB: `rm -f ~/Library/Application\ Support/Prism/metadata.duckdb* ~/Library/Application\ Support/Prism/index.db*`

### Running specific test suites

```bash
xcodebuild test -scheme prism -only-testing:prismTests/BulkScannerTests
xcodebuild test -scheme prism -only-testing:prismTests/ScannerBenchmark
xcodebuild test -scheme prism -only-testing:prismTests/IntegrationTests
```

## Architecture

### Tiered Storage

```
INGESTION:
  getattrlistbulk (4-8 parallel workers)
       │ AsyncStream (producer-consumer)
       ▼
  files_staging_<volume> (per-scan DuckDB table, Appender fast path)
       │ mergeAndDiff (set-based SQL, id-keyed) — emits ScanDiff with full row payload
       ▼
  DuckDB files (on disk) ── source of truth, all metadata
       │ ScanDiff has everything syncSearchIndex + applyDiff need — no refetch
       ▼
  SQLite/FTS5 (on disk) ── search index only (id, filename, extension)
       │
  In-memory cache ── Dictionary<Int64, SearchResult> built from ScanDiff directly
```

IDs are deterministic: `id = FNV-1a64(volume_uuid || "\0" || path)` via
`PathHash.id(volumeUUID:path:)`. Same file → same id forever, across
rescans and restarts. Same Int64 flows through DuckDB PK → SQLite `files.id` →
FTS5 rowid with no translation.

### Core Components

1. **BulkScanner** (`Scanner/BulkScanner.swift`)
   - Low-level `getattrlistbulk` BSD syscall wrapper
   - Scans a single directory, returns audio files + subdirectories
   - Uses `loadUnaligned(as:)` for buffer parsing (packed attrs are NOT aligned)
   - `attrgroup_t(bitPattern:)` casts required for mixed Int32/UInt32 constants
   - Filters: audio extensions (Set<String>, O(1)), hidden files, system directories

2. **ParallelScanCoordinator** (`Scanner/ParallelScanCoordinator.swift`)
   - Actor-based parallel BFS with bounded concurrency
   - Producer-consumer pattern via `AsyncStream` — scan and DuckDB write overlap
   - 8 workers for internal SSD, 4 for external USB drives
   - Batched DuckDB Appender writes (5,000 rows per flush)

3. **DuckDBStore** (`Database/DuckDBStore.swift`)
   - Persistent on-disk metadata store at `~/Library/Application Support/Prism/metadata.duckdb`
   - **Multi-connection architecture**: 1 writer (`WriterConnection`) + 3-connection
     reader pool (`ReaderPool`). Writes serialize through the writer's NSLock;
     reads round-robin across the reader pool. Cache has its own `cacheLock`,
     separate from DuckDB access. Lets searches complete on MVCC snapshots while
     a scan's `Appender` is writing.
   - Appender API for fast ingestion into a per-volume staging table
   - `beginScan` / `mergeAndDiff` / `applyDiff` drive the incremental flow
   - Single-scan slot: concurrent scans throw `IndexError.scanAlreadyInProgress`
   - Orphan staging tables dropped at init (`cleanupOrphanedStaging`)
   - In-memory cache (`Dictionary<Int64, SearchResult>`) updated incrementally
     via `applyDiff` on every rescan
   - Single-process only — DuckDB locks the file, cannot have two instances
   - `DuckDBStore`, `WriterConnection`, `ReaderPool` are all `nonisolated` — the
     project sets `SWIFT_DEFAULT_ACTOR_ISOLATION = MainActor`, so any new class
     holding a DuckDB `Connection` must opt out or its deinit will race with
     DuckDB cleanup via Swift's `swift_task_deinitOnExecutorImpl` path

4. **DatabaseManager** (`Database/DatabaseManager.swift`)
   - SQLite/GRDB for FTS5 search index at `~/Library/Application Support/Prism/index.db`
   - Slim schema: `files(id, filename, extension)` + FTS5 virtual table
   - Two sync paths:
     - `syncSearchIndex(from:volumeUUID:diff:)` — incremental, O(|diff|). Used
       on the scan hot path. Triggers stay in place; per-row INSERT/DELETE
       propagates to FTS5 via existing `files_ai`/`files_ad`/`files_au`. If
       the diff is large (≥1000 mutations), a `('rebuild')` pass packs
       segments for query performance.
     - `rebuildSearchIndex(from:vacuumAfter:)` — O(N) full rebuild. Used by
       Clear Index / Rebuild Index in Settings. `vacuumAfter` defaults to
       false — A/B benchmarks (`VacuumBenchmark`) showed only a 10% rescan
       win at 27K rows, not worth the VACUUM cost on large indexes.
   - FTS5 trigger SQL lives in one `createFTS5Triggers(_:)` helper
   - Startup trigger integrity check for crash recovery
   - `init(testPath:)` — test-only initializer that bypasses the hardcoded
     `~/Library/Application Support/Prism/` path. Benchmarks MUST use this;
     hitting the shared singleton's real path corrupts user data and
     produces meaningless numbers.

5. **PathHash** (`Database/PathHash.swift`)
   - Stable FNV-1a 64-bit hash of (volume_uuid, path). Deterministic
     across runs/processes/machines.
   - Collision probability ~7e-9 at 5M rows; collisions throw
     `IndexError.hashCollision`.

6. **LiveIndexCoordinator** (`Scanner/LiveIndexCoordinator.swift`) — Phase 3
   - Actor owning FSEventStream per watched volume + NSWorkspace mount/unmount
   - Classifies FSEvents flags; files → direct-diff path, dirs + MustScanSubDirs → staging fallback
   - Coalesces events in a quiescence window (2s idle / 10s max / 10k cap) then applies
   - E+poll fallback: on `HistoryDone` with inconsistent state, flips volume to polling mode with a 5-min `DispatchSourceTimer`
   - Bridges back to UI via `LiveIndexBridge` protocol (SearchViewModel implements it)
   - Disable via `UserDefaults["LiveIndexDisabled"]`; also exposed as Settings toggle

7. **SearchViewModel** (`ViewModels/SearchViewModel.swift`)
   - Search path: FTS5 prefix match → IDs → cache dictionary lookup
   - Scan path: `beginScan` → `ParallelScanCoordinator.scanStreaming` (writes
     into staging) → `mergeAndDiff` → incremental `syncSearchIndex` →
     `applyDiff` on cache
   - `clearVolumeFiles` uses the full-rebuild path
   - 150ms debounce on search input

### Search Flow

```
User types → debounce(150ms) → FTS5 MATCH → [Int64] IDs → cache[id] → [SearchResult] → UI
                                  ~5ms          ~0.3ms
```

### Scan Flow

```
beginScan(volume) ──► creates files_staging_<volume>

BulkScanner × N workers ──► AsyncStream<StreamChunk> ──► DuckDB Appender → staging
                                                              │
                                     mergeAndDiff (set-based SQL, id-keyed)
                                                              │
                                                              ▼
                            ScanDiff {
                              added:   [Entry{id, filename, path, volumeUUID,
                                             ext, sizeBytes, dateModified, dateCreated}],
                              modified: [Entry{...same shape...}],
                              removedIds: [Int64]
                            }
                                                              │
                                                  ┌───────────┴───────────┐
                                                  ▼                       ▼
                                    SQLite DELETE/INSERT OR REPLACE    cache.applyDiff
                                    (triggers update FTS5)             (builds SearchResult
                                                                        from Entry — zero
                                                                        DuckDB round-trips)
```

`ScanDiff.Entry` carries the full row payload so `applyDiff` can construct
`SearchResult` values directly and never has to re-query DuckDB. Before this
change, applyDiff did 28 × `SELECT ... WHERE id IN (...)` chunks on the
writer connection, costing ~6s for a 27K-row rescan (DuckDB point-lookups
are ~200ms each — see "DuckDB point lookups" below).

Empty-diff rescans skip the sync entirely and patch the cache with zero
rows — rescanning an unchanged volume does near-zero post-scan work.

### Live Index Flow (Phase 3)

```
NSWorkspace didMount ──▶ LiveIndexCoordinator.onMount(volume)
                                    │
                                    ▼
              loadWatchState (last_event_id) + createFSEventStream
                                    │
                                    ▼
              Stream fires callback on fsEventsQueue (serial DispatchQueue)
                                    │
                       classify events (flags matrix)
                                    │
       ┌────────────────────────────┼─────────────────────────────┐
       │                            │                             │
 MustScanSubDirs /           Normal file events          HistoryDone
 UserDropped /                      │                             │
 KernelDropped /                    ▼                      E+poll heuristic:
 ItemIsDir+Created      coalescer (2s idle, 10s max,     did replay return
       │                 10k size cap)                    nothing while files
       ▼                            │                     look stale?
 staging path:                      ▼                             │
   beginScan +                buildDiff (grouped by                │ yes → polling_mode
   ParallelScanCoordinator +    parent dir, 1 bulk-stat            │       + 5min timer
   mergeAndDiff +               per dir; rename → remove+add)      │ no  → listening
   pendingBatches drain               │
                                      ▼
                              applyDirectDiff
                              (INSERT ... ON CONFLICT DO UPDATE,
                              chunked 1k rows, writer NSLock
                              released between chunks)
                                      │
                                      ▼
                              dbManager.syncSearchIndex (existing)
                                      │
                                      ▼
                              duckDBStore.applyDiff (existing cache patch)
                                      │
                                      ▼
                              persistEventId (AFTER both commits succeed)
                                      │
                                      ▼
                              bridge.liveIndexDidApplyDiff → SearchViewModel
                                      │
                                      ▼
                              UI refresh via @Published state
```

FSEvents direct-diff re-uses the same `ScanDiff` struct as full scans. The
only new on-disk state is `volume_watch_state(volume_uuid PRIMARY KEY,
last_event_id, last_seen_at, polling_mode, last_reason)`. 

Back-pressure: if pending event count > 100k, abandon direct-diff and
trigger a full-volume rescan. New events arriving during that rescan are
dropped — `mergeAndDiff` at the end reconciles the current filesystem state
anyway.

Mount/unmount via NSWorkspace target-action observers stored as
`NSObjectProtocol` tokens on `SearchViewModel`; teardown via
`removeObserver` on `applicationWillTerminate`. AsyncSequence-based
observation leaks without explicit cancellation, so target-action is
preferred.

### Audio File Filtering

The scanner only indexes audio files with these extensions:
- Common: mp3, wav, flac, aac, m4a, ogg, wma, aiff, aif
- Advanced: ape, opus, alac, dsd, dsf, mp2, mpc, wv, tta, ac3, dts

Any filename starting with `.` or `$` is skipped. In addition,
`BulkScanner.skippedDirectories` holds a basename blocklist:
- Volume metadata: `$RECYCLE.BIN`, `System Volume Information`, `.Trashes`,
  `.Spotlight-V100`, `.fseventsd`, `.TemporaryItems`
- DJ tools: `_Serato_`
- Developer artifacts: `node_modules`, `Pods`, `DerivedData`, `build`,
  `target`, `vendor`, `venv`, `.venv`, `__pycache__`

The list is currently a static `Set<String>` — making it user-configurable
per-volume is on the Phase 5 roadmap.

### Logging

All logging goes through `Log.swift` using `os.Logger`:
- `Log.debug()` — compiled out in Release builds (`#if DEBUG`, uses `logger.notice` for Xcode visibility)
- `Log.error()` — always logged
- `Log.info()` — always logged

## Key Technical Details

- **getattrlistbulk buffer alignment**: Fields are NOT naturally aligned. Always use `loadUnaligned(as:)`. The `timespec` at offset 36 from entry start is misaligned (36 % 8 = 4).
- **ATTR_CMN constants**: Mixed Int32/UInt32 types in Swift. Use `attrgroup_t(ATTR_CMN_RETURNED_ATTRS) | attrgroup_t(bitPattern: ATTR_CMN_NAME)`.
- **DuckDB file locking**: Only one process can open a `.duckdb` file. Kill old app before relaunching.
- **DuckDB Appender**: Does not support ON CONFLICT. The staging-table merge pattern is how we get upsert semantics without losing Appender throughput. See github.com/duckdb/duckdb#11275 — direct `ON CONFLICT DO UPDATE` degrades to ~100s per 10k rows at 100k-row scale.
- **IDs are hash-derived and stable**: `PathHash.id(volumeUUID:path:)` — FNV-1a 64-bit. Never switch the hash function without a full re-index (every row's PK would change). Collisions throw `IndexError.hashCollision`.
- **DuckDB point lookups**: Slow (200ms+ for IN(...) queries). Use in-memory cache instead. This is why `ScanDiff.Entry` carries full row data — `applyDiff` must NOT hit DuckDB after mergeAndDiff, even for "correctness refetch." See the Scan Flow diagram.
- **FTS5 bulk rebuild** (`INSERT INTO files_fts(files_fts) VALUES('rebuild')`) beats per-row triggers for bulk inserts, but fragments FTS5 segments on incremental paths. Incremental sync fires a `'rebuild'` pass only when the diff is ≥1000 mutations.
- **Staging tables are per-volume**: `files_staging_<sanitized_uuid>`. Sanitization replaces `-` with `_` and the name is double-quoted in SQL. Orphan staging tables (from crashed scans) are dropped at `DuckDBStore.init` via `cleanupOrphanedStaging`.
- **Scan slots are per-volume** (Phase 3): `currentScanVolumes: Set<String>` in `DuckDBStore`. `beginScan` on different volumes succeeds concurrently; `beginScan` twice on the SAME volume throws `scanAlreadyInProgress`. The writer connection is still NSLock-serialized, so parallelism is in scan I/O + staging INSERTs; `mergeAndDiff` and `applyDirectDiff` still serialize. Works through sequential interleaving from one executor (the `LiveIndexCoordinator` actor); true multi-threaded parallel use of `DuckDBStore` is not supported — see `MultiVolumeConcurrentScanTests`.
- **`applyDirectDiff` idempotency**: FSEvents replay sends the same events after app restart. Direct-diff must be idempotent. Implementation uses `INSERT ... ON CONFLICT (id) DO UPDATE SET ...` (NOT Appender, which doesn't support ON CONFLICT — the plan's DELETE+Appender attempt triggered the same unique-constraint index error as direct conflicts). `volume_uuid` is excluded from the UPDATE SET because it's indexed and DuckDB rejects UPDATE SET on indexed columns; same id implies same (volume_uuid, path) by PathHash construction anyway.
- **`volume_watch_state` table** (Phase 3): per-volume FSEvents checkpoint + polling-mode flag. Event-id persists AFTER both DuckDB and SQLite commits succeed (revised cadence from `/plan-ceo-review` Reviewer Concern #3 — atomic-with-DuckDB risks advancing the event-id while SQLite sync fails, producing phantom gaps in FTS5).
- **E+poll ExFAT fallback** (Phase 3): FSEvents is unreliable on non-journaled filesystems (ExFAT, FAT, NTFS). On `HistoryDone`, if replay returned zero events AND `MAX(files.date_modified)` predates the mount time, treat as inconsistent and flip `polling_mode = TRUE`. A `DispatchSourceTimer` at 5-minute interval then drives `triggerFullRescan` for that volume. Compare against mount time (NOT `last_seen_at`) to avoid DST-transition false positives — ExFAT stores local time without timezone.

## Performance Targets

Measurements from real USB drive, 27 604 audio files.

- **Search latency**: <10ms warm cache-hit for narrow queries; broad prefixes capped at 1000 results take 50–100ms (BM25 ranking dominates)
- **Scan throughput**: 1.5M files/sec (internal SSD), 4K files/sec (USB), I/O-bound
- **First-scan pipeline**: ~7s for 27K files on USB (scan ~5s + merge ~1.4s + incremental sync ~0.9s + loadCache ~0.3s)
- **Rescan of unchanged volume**: ~1.5s total, ~0.4s post-scan work (empty diff → sync skipped, mergeAndDiff still does its join)
- **Rescan after Clear Index**: ~3.5s total (scan cache-warm ~0.8s + merge ~1.5s + sync ~1.2s + applyDiff ~0.01s). Before the applyDiff refactor this was 9s+ — see Scan Flow.
- **Search during an active scan (p99)**: 1.92ms warm cache-hit, 1.18× idle. Enabled by the writer + 3-reader connection pool.
- UI responsiveness: <16ms (60fps)

## Development Roadmap

| Phase | Focus | Status |
|-------|-------|--------|
| MVP | Basic UI, scanner, database, search | Done |
| Ingestion v2 | getattrlistbulk, parallel scan, DuckDB tiered storage | Done |
| Sync v2 | Incremental FTS5 sync, hash-derived stable IDs, staging-table merge | Done |
| Phase 3 | FSEvents monitoring, auto-indexing on drive mount | Planned |
| Phase 4 | ID3 tag metadata extraction (artist, album, genre, duration), drag-and-drop | Planned |
| Phase 5 | Advanced filters, offline drive handling, **user-configurable ignore-directory list** | Planned |
| Phase 6 | Semantic search — mood/genre/concept queries via ID3 tags + artist lookup + local LLM enrichment | Planned |
