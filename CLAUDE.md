# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Prism** is a high-performance macOS desktop search utility designed as a native alternative to Windows' "Everything" search tool. It maintains a lightweight SQLite database to enable instant filename search (sub-100ms) for massive libraries (5M+ files) across multiple external drives, bypassing Spotlight's limitations.

**Target:** macOS 14.0+ (Sonoma)
**Stack:** Swift (AppKit/SwiftUI Hybrid), SQLite with FTS5
**Primary Use Case:** Indexing and searching 5M+ audio files across external drives

## Development Commands

This is a greenfield project with no existing source code yet. Once the Xcode project is created, standard Swift/Xcode commands will apply:

- Build: `xcodebuild -scheme Prism -configuration Debug`
- Test: `xcodebuild test -scheme Prism`
- Run: Open `Prism.xcodeproj` in Xcode and press Cmd+R

## Architecture Overview

### Core Components (Planned)

1. **File System Scanner** (`getattrlistbulk`-based)
   - Uses low-level BSD calls instead of `FileManager` for maximum speed
   - Implements `FSEvents` for live file monitoring
   - Identifies volumes by UUID (not name) to handle drive renaming
   - Supports offline mode for disconnected drives

2. **SQLite Database** (`~/Library/Application Support/Prism/index.db`)
   - **FTS5 Table (`search_index`)**: Fast text search on filename/extension
   - **Metadata Table (`file_meta`)**: Size, date_modified, duration_sec (audio files)
   - **Configuration**: WAL mode for concurrent read/write, batched inserts (10k records)

3. **Search Engine**
   - SQLite FTS5-powered with prefix search, extension filters (`ext:mp3`), boolean queries
   - Target: <100ms query latency for 5M+ records

4. **UI Layer** (AppKit/SwiftUI Hybrid)
   - **Sidebar**: `NSVisualEffectView` with drive status and smart filters
   - **Results Table**: `NSTableView` with virtualization for 5M rows
   - **Interactions**: Drag-and-drop to Finder/DAWs, QuickLook (Spacebar), context menus

### Audio File Filtering

The scanner only indexes audio files with the following extensions:
- Common: mp3, wav, flac, aac, m4a, ogg, wma, aiff/aif
- Advanced: ape, opus, alac, dsd, dsf, mp2, mpc, wv, tta, ac3, dts

Non-audio files are skipped during scanning.

### Two-Phase Indexing

- **Phase 1 (Scan)**: Index name, path, size, date immediately
- **Phase 2 (Enrichment)**: Background queue reads ID3 tags for audio duration

### macOS HIG Compliance

All UI must follow Apple's Human Interface Guidelines:
- Use system font stacks, SF Symbols 5, translucent sidebars
- Standard shortcuts: Cmd+F (search), Cmd+, (preferences), Spacebar (QuickLook)
- 60fps UI interactions, zebra striping for tables

## Development Roadmap

| Phase | Focus | Deliverables |
|-------|-------|--------------|
| MVP | Scanner | Basic UI, `getattrlistbulk` scanner, database, <100ms search |
| Phase 2 | Watcher | `FSEvents`, external drive mount/unmount handling |
| Phase 3 | Metadata | ID3 tag background worker, drag-and-drop |
| Phase 4 | Polish | Dark mode, QuickLook integration |

## Key Technical Constraints

- **Volume Identification**: Must use Volume UUID, not name
- **Performance Targets**:
  - Search latency: <100ms
  - UI responsiveness: <16ms (60fps)
- **Database Writes**: Batch in 10k transactions to minimize I/O
- **Virtualization**: Table must render only visible rows (~40) from millions
