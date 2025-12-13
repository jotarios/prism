# Prism

> Lightning-fast file search for macOS — a native alternative to Windows' "Everything" search tool

Prism is a high-performance desktop search utility for macOS that maintains a lightweight SQLite database to enable instant filename search across massive audio libraries (5M+ files) on external drives.

## Features

- **Instant Search**: Sub-100ms search latency for millions of files using SQLite FTS5
- **External Drive Support**: Index and search files on external drives, even when offline
- **Audio-Focused**: Specialized indexing for audio file formats (MP3, WAV, FLAC, and more)
- **Volume UUID Tracking**: Handles drive renaming without re-indexing
- **Responsive UI**: Maintains 60fps performance while indexing large libraries
- **Real-time Progress**: Live feedback during volume scanning

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

### Core Components

- **File Scanner**: Breadth-first traversal with batched database writes
- **SQLite Database**: FTS5 full-text search with WAL mode for concurrency
- **Search Engine**: Optimized prefix matching for large datasets
- **UI Layer**: AppKit/SwiftUI hybrid with virtualized table views

### Performance Characteristics

- Search latency: <100ms for 5M+ files
- Batch size: 500 files per transaction
- Progress updates: Every 50 files
- UI responsiveness: Non-blocking async operations

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
├── Database/          # SQLite database manager
├── Models/            # Data models (FileRecord, VolumeInfo)
├── Scanner/           # File system scanning logic
├── ViewModels/        # Search and state management
└── Views/             # SwiftUI/AppKit UI components
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Roadmap

- [x] **MVP**: Basic UI, file scanner, database, instant search
- [ ] **Phase 2**: Performance improvement, FSEvents monitoring, auto-indexing on drive mount
- [ ] **Phase 3**: ID3 tag metadata extraction, drag-and-drop support
- [ ] **Phase 4**: Dark mode, QuickLook integration, advanced filters

## License

MIT License - see [LICENSE](LICENSE) file for details

## Acknowledgments

- Inspired by [Everything](https://www.voidtools.com/) for Windows
- Uses [GRDB.swift](https://github.com/groue/GRDB.swift) for SQLite access
- Built with Swift and SwiftUI

---

**Note**: This project is in active development. The current version implements core search functionality with planned enhancements for file monitoring and metadata extraction.
