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
  DuckDB (on disk) ── source of truth, all metadata
       │ batch sync
       ▼
  SQLite/FTS5 (on disk) ── search index only (id, filename, extension)
       │
  In-memory cache ── Dictionary<Int64, SearchResult> for display
```

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
   - Appender API for fast ingestion
   - In-memory cache (`Dictionary<Int64, SearchResult>`) loaded after scan
   - Single-process only — DuckDB locks the file, cannot have two instances

4. **DatabaseManager** (`Database/DatabaseManager.swift`)
   - SQLite/GRDB for FTS5 search index at `~/Library/Application Support/Prism/index.db`
   - Slim schema: `files(id, filename, extension)` + FTS5 virtual table
   - Bulk import mode: drop triggers → batch insert → single FTS5 rebuild
   - Startup trigger integrity check for crash recovery

5. **SearchViewModel** (`ViewModels/SearchViewModel.swift`)
   - Search path: FTS5 prefix match → IDs → cache dictionary lookup
   - Scan path: parallel scan → DuckDB stream → FTS5 sync + cache load (parallel)
   - 150ms debounce on search input

### Search Flow

```
User types → debounce(150ms) → FTS5 MATCH → [Int64] IDs → cache[id] → [SearchResult] → UI
                                  ~5ms          ~0.3ms
```

### Scan Flow

```
BulkScanner × N workers → AsyncStream → DuckDB Appender (5K batch)
                                              │
                                    ┌─────────┴─────────┐
                                    ▼                   ▼
                              FTS5 sync          cache load
                              (parallel)         (parallel)
```

### Audio File Filtering

The scanner only indexes audio files with these extensions:
- Common: mp3, wav, flac, aac, m4a, ogg, wma, aiff, aif
- Advanced: ape, opus, alac, dsd, dsf, mp2, mpc, wv, tta, ac3, dts

Skipped directories: hidden (`.`), system (`$RECYCLE.BIN`, `System Volume Information`, `_Serato_`)

### Logging

All logging goes through `Log.swift` using `os.Logger`:
- `Log.debug()` — compiled out in Release builds (`#if DEBUG`, uses `logger.notice` for Xcode visibility)
- `Log.error()` — always logged
- `Log.info()` — always logged

## Key Technical Details

- **getattrlistbulk buffer alignment**: Fields are NOT naturally aligned. Always use `loadUnaligned(as:)`. The `timespec` at offset 36 from entry start is misaligned (36 % 8 = 4).
- **ATTR_CMN constants**: Mixed Int32/UInt32 types in Swift. Use `attrgroup_t(ATTR_CMN_RETURNED_ATTRS) | attrgroup_t(bitPattern: ATTR_CMN_NAME)`.
- **DuckDB file locking**: Only one process can open a `.duckdb` file. Kill old app before relaunching.
- **DuckDB Appender**: Does not support DEFAULT column values. Use explicit IDs with a counter.
- **DuckDB point lookups**: Slow (200ms+ for IN(...) queries). Use in-memory cache instead.
- **FTS5 rebuild**: `INSERT INTO files_fts(files_fts) VALUES('rebuild')` — faster than per-row triggers for bulk operations.

## Performance Targets

- Search latency: <5ms (FTS5 + cache)
- Scan throughput: 1.5M files/sec (internal SSD), 4K files/sec (USB)
- Pipeline: 81% I/O bound on external drives, code overhead ~1.4s for 27K files
- UI responsiveness: <16ms (60fps)

## Development Roadmap

| Phase | Focus | Status |
|-------|-------|--------|
| MVP | Basic UI, scanner, database, search | Done |
| Ingestion v2 | getattrlistbulk, parallel scan, DuckDB tiered storage | Done |
| Phase 3 | FSEvents monitoring, auto-indexing on drive mount | Planned |
| Phase 4 | ID3 tag metadata extraction (artist, album, genre, duration), drag-and-drop | Planned |
| Phase 5 | Advanced filters, offline drive handling | Planned |
| Phase 6 | Semantic search — mood/genre/concept queries via ID3 tags + artist lookup + local LLM enrichment | Planned |
