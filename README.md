# Prism

> Lightning-fast file search for macOS — a native alternative to Windows' "Everything" search tool

Prism is a high-performance desktop search utility for macOS that maintains a tiered database architecture to enable instant filename search across massive audio libraries (5M+ files) on external drives.

![Prism Preview](docs/img/prism_preview.png)

## Features

- **Instant Search**: Sub-20ms search latency using SQLite FTS5 + in-memory cache
- **External Drive Support**: Index and search files on external drives, even when offline
- **Audio-Focused**: Specialized indexing for audio file formats (MP3, WAV, FLAC, and more)
- **Volume UUID Tracking**: Handles drive renaming without re-indexing
- **Parallel Scanning**: `getattrlistbulk` with multi-worker BFS for maximum throughput
- **Tiered Storage**: DuckDB (metadata) + SQLite FTS5 (search index) + in-memory cache

## Performance

### Full pipeline (27,604 audio files, 2TB USB ExFAT drive)

| Stage | Time | % of pipeline |
|-------|------|---------------|
| USB I/O + scan | 6.00s | 81% |
| DuckDB Appender writes | 0.80s | 11% |
| FTS5 sync + cache load (parallel) | 0.57s | 8% |
| **Total ingestion** | **7.39s** | **4x faster than original (~30s)** |
| **Search "amor"** (834 results) | **2.7ms** | **37x faster than original (~100ms)** |

> 81% of pipeline time is USB I/O. The code (DuckDB + FTS5 + cache) completes in 1.37s.
> On internal SSD, the same 27K files would ingest in ~1.5s.

### Scanner throughput (7,800 files, internal SSD)

| Approach | Throughput | vs FileManager |
|----------|-----------|----------------|
| `FileManager` (original) | 112K files/sec | 1.0x |
| `getattrlistbulk` serial | 623K files/sec | 5.6x |
| `getattrlistbulk` parallel (8 workers) | 1,530K files/sec | **14x** |

### Search latency (27,604 files)

| Query | FTS5 | Cache | Total |
|-------|------|-------|-------|
| `am` (1,000 results) | 4.9ms | 0.6ms | 5.6ms |
| `amor` (834 results) | 2.4ms | 0.3ms | 2.7ms |

### Optimization breakdown

| Optimization | Impact |
|---|---|
| `getattrlistbulk` (replaces `FileManager`) | ~5x fewer syscalls per directory |
| Parallel BFS (4-8 workers) | 2.5x on SSD, ~1.5x on USB |
| Producer-consumer `AsyncStream` pipeline | Scan and DuckDB write overlap |
| Batched DuckDB Appender (5K rows/flush) | 0.80s for 27K rows |
| FTS5 triggers dropped during bulk import | Single rebuild vs per-row updates |
| FTS5 sync + cache load in parallel | 0.57s vs 0.62s sequential |
| In-memory cache for search results | 0.3ms vs 200ms DuckDB point lookup |

## Supported Audio Formats

**Common**: mp3, wav, flac, aac, m4a, ogg, wma, aiff, aif

**Advanced**: ape, opus, alac, dsd, dsf, mp2, mpc, wv, tta, ac3, dts

## Requirements

- macOS 14.0 (Sonoma) or later
- Xcode 15.0+ (for building from source)

## Installation

### From Source

```bash
git clone https://github.com/jotarios/prism.git
cd prism/prism
open prism.xcodeproj
```

Build and run using Xcode (Cmd+R) or command line:

```bash
xcodebuild -scheme prism -configuration Release
```

## Usage

1. **Launch Prism** and connect your external drives
2. **Scan a volume** by clicking the scan button next to a volume in the sidebar
3. **Search instantly** as files are indexed — just start typing in the search bar
4. **Open files** by double-clicking results or use QuickLook (Spacebar)

## Architecture

```
INGESTION:
  getattrlistbulk ───┐
  (parallel BFS)     │  stream per-directory
                     ▼
  ┌───────────────────────────────┐
  │  DuckDB (persistent, on disk) │
  │  Source of truth              │
  │  Appender API (fast writes)   │
  └──────────┬────────────────────┘
             │ sync
             ▼
  ┌───────────────────────────────┐
  │  SQLite/FTS5 (on disk)        │
  │  Search index only            │
  │  id + filename + extension    │
  └───────────────────────────────┘

SEARCH:
  User types ──▶ FTS5 prefix match ──▶ IDs ──▶ in-memory cache ──▶ results
```

### Core Components

- **BulkScanner**: Low-level `getattrlistbulk` BSD syscall wrapper for batch file attribute retrieval
- **ParallelScanCoordinator**: Actor-based parallel BFS with bounded concurrency (8 workers internal, 4 external)
- **DuckDB Store**: Persistent on-disk metadata store using DuckDB's Appender API for fast ingestion
- **SQLite/FTS5**: Full-text search index with prefix matching, synced from DuckDB after scan
- **In-memory Cache**: Dictionary-based metadata lookup for sub-millisecond search result display

### Key Design Decisions

- **`getattrlistbulk` over `FileManager`**: 14x faster scanning by batching ~500 file attributes per kernel call
- **`loadUnaligned(as:)` for buffer parsing**: Packed attribute buffers are not aligned to natural boundaries
- **DuckDB for metadata, SQLite for search**: Each database does what it's best at
- **FTS5 triggers dropped during bulk import**: Single `'rebuild'` at the end instead of per-row trigger updates
- **Startup trigger integrity check**: Detects and recovers from crashes during bulk import

## Development

### Building

```bash
xcodebuild -scheme prism -configuration Debug
```

### Testing

```bash
xcodebuild test -scheme prism
```

### Project Structure

```
prism/
├── Database/
│   ├── DatabaseManager.swift    # SQLite/GRDB FTS5 search index
│   └── DuckDBStore.swift        # DuckDB persistent metadata store
├── Scanner/
│   ├── BulkScanner.swift        # getattrlistbulk wrapper
│   ├── ParallelScanCoordinator.swift  # Parallel BFS actor
│   └── VolumeManager.swift      # Volume discovery & UUID
├── Models/
│   ├── FileRecord.swift         # SearchResult, FileRecordInsert, SyncRecord
│   └── VolumeInfo.swift         # Volume metadata
├── ViewModels/
│   └── SearchViewModel.swift    # Central state, dual-DB search + scan pipeline
└── Views/
    ├── MainWindow.swift         # NavigationSplitView root
    ├── SidebarView.swift        # Volume list + scan buttons
    ├── SearchBarView.swift      # Search input with debounce
    ├── ResultsTableView.swift   # Sortable results table
    ├── SettingsView.swift       # Per-volume management
    └── QuickLookPreview.swift   # Spacebar file preview
```

### Running Benchmarks

```bash
# Scanner: FileManager vs getattrlistbulk vs parallel
xcodebuild test -scheme prism -only-testing:prismTests/ScannerBenchmark

# DuckDB: Appender throughput + point lookup latency
xcodebuild test -scheme prism -only-testing:prismTests/DuckDBStoreTests

# Full pipeline: scan → DuckDB → FTS5 sync → search
xcodebuild test -scheme prism -only-testing:prismTests/IntegrationTests

# Scale comparison: FTS5+Cache vs DuckDB-only vs brute force
xcodebuild test -scheme prism -only-testing:prismTests/ScaleTests
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes using [Conventional Commits](https://www.conventionalcommits.org/) format
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Roadmap

- [x] **MVP**: Basic UI, file scanner, database, instant search
- [x] **Ingestion v2**: `getattrlistbulk`, parallel scanning, DuckDB tiered storage
- [ ] **Phase 3**: FSEvents monitoring, auto-indexing on drive mount
- [ ] **Phase 4**: ID3 tag metadata extraction (artist, album, genre, duration), drag-and-drop
- [ ] **Phase 5**: Advanced filters, offline drive handling
- [ ] **Phase 6**: Semantic search — query by mood, genre, or concept ("sad songs", "workout music"). ID3 genre/mood tags + filename-based artist lookup + optional local LLM enrichment for untagged files. Hybrid approach: three tiers of coverage depending on available metadata

## License

MIT License - see [LICENSE](LICENSE) file for details

## Acknowledgments

- Inspired by [Everything](https://www.voidtools.com/) for Windows
- Uses [GRDB.swift](https://github.com/groue/GRDB.swift) for SQLite access
- Uses [DuckDB](https://github.com/duckdb/duckdb-swift) for columnar metadata storage
- Built with Swift and SwiftUI
