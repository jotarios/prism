# Product Requirements Document: Prism

| Project Name | Prism (Working Title) |
| :--- | :--- |
| **Version** | 1.0 |
| **Status** | Draft |
| **Target Platform** | macOS 14.0+ (Sonoma) |
| **Architecture** | Swift (AppKit), SQLite, FTS5 |
| **Primary Use Case** | Indexing & Searching 5M+ Audio Files across External Drives |

---

## 1. Executive Summary
**Prism** is a high-performance desktop search utility for macOS, designed to function as a native alternative to "Everything" (Windows).

Unlike Spotlight, which relies on system-wide indexing (often slow or incomplete on external volumes), Prism maintains its own lightweight, high-speed database. It prioritizes **instant filename search** (sub-100ms latency) for massive libraries (5+ million files) stored on multiple external hard drives.

## 2. Design Principles (macOS HIG)
The application must adhere strictly to Apple’s [Human Interface Guidelines](https://developer.apple.com/design/human-interface-guidelines/designing-for-macos) to ensure it feels like a built-in system tool.

* **Immersive & Native:** Use standard window chrome, translucent sidebars, and system font stacks.
* **Speed as a Feature:** Search queries must return first results within 100ms. UI must remain responsive (60fps) during all operations.
* **Predictable:** Standard keyboard shortcuts (`Cmd+F`, `Cmd+,`, `Spacebar` for QuickLook).

---

## 3. Functional Requirements

### 3.1. File System Indexing
* **Scope:** The app must scan user-selected volumes (internal and external).
* **Methodology:** Use low-level BSD calls (`getattrlistbulk`) rather than `FileManager` enumeration for maximum speed.
* **Live Monitoring:** Implement `FSEvents` to track file creations, deletions, and renames. Handle FSEvents coalescing and latency (1-5s typical).
* **Volume Handling:**
    * Detect when external drives are mounted/unmounted via DiskArbitration framework.
    * Identify drives by **Volume UUID**, not Volume Name (to handle renaming).
    * *Offline Mode:* Search results from disconnected drives remain visible with visual indicator (badge or dimmed text). Offline results appear in separate section or at bottom of results list.
* **Permissions:** Requires Full Disk Access (TCC) for indexing system volumes. Uses `com.apple.security.files.user-selected.read-only` entitlement for sandboxed access.
* **Error Handling:**
    * Gracefully handle permission errors, read-only volumes, and mid-scan disconnections.
    * Provide user feedback when volumes cannot be indexed.

### 3.2. Search Capabilities
* **Engine:** SQLite FTS5 (Full-Text Search 5) with prefix indexing enabled.
* **Query Types:**
    * **Prefix Search:** `love` matches "Lover", "Lovely".
    * **Extension Filter:** `ext:mp3` (implemented via post-FTS filtering, not FTS5 query).
    * **Boolean:** `madonna AND live`.
* **Searchable Fields:**
    * Filename (primary)
    * Extension
* **Sortable/Filterable Metadata:**
    * Date Modified
    * File Size
    * Volume UUID (for online/offline filtering)
    * Duration (audio files only, populated asynchronously)
* **Result Pagination:** Return maximum 10,000 results per query. Display first 100 immediately, lazy-load remainder on scroll.
* **Performance Target:** First 100 results rendered within 100ms (cold cache), sorting deferred until user requests.

### 3.3. Audio-Specific Logic
* **Deferred Metadata:**
    * *Phase 1 (Scan):* Index name, path, size, date modified immediately during volume scan.
    * *Phase 2 (Enrichment):* Background queue reads audio duration using AVFoundation (not raw ID3 parsing). Queue is throttled to prevent I/O contention.
* **Audio Duration Extraction:**
    * Use AVFoundation's `AVAsset` to read duration (handles MP3, AAC, FLAC, WAV, etc.).
    * Process files at low priority (QoS: `.utility`).
    * Skip files over 500MB or unreadable formats without blocking queue.

---

## 4. User Interface Specifications

### 4.1. Main Window Layout
* **Sidebar:**
    * **Sources Section:** List indexed volumes with status indicators (online/offline).
    * Use **SF Symbols** (e.g., `externaldrive.fill`, `internaldrive`).
    * **MVP Scope:** Volume list only. Defer smart filters to post-MVP.
* **Toolbar:**
    * Unified title/toolbar style.
    * Centered, expansive Search Field with live search (debounced 150ms).
* **Results Table:**
    * **Component:** `NSTableView` (AppKit) with manual cell reuse for performance.
    * **Columns:** Name, Date Modified, Size, Path, Duration (shows "—" until loaded).
    * **Typography:** Use `monospacedDigitSystemFont` for Size/Date to align numbers.
    * **Zebra Striping:** Enabled for readability.
    * **Virtualization:** Render only visible rows using `NSTableView` reuse mechanism.

### 4.2. Interactions
* **Context Menu:** Right-click on file → "Show in Finder", "Open With", "Copy Path".
* **Drag & Drop:** Users can drag files from results table to Finder or applications (DAWs, editors).
* **QuickLook:** Press Spacebar to preview file using `QLPreviewPanel`.
* **Keyboard Navigation:** Arrow keys navigate results, Return opens file, Cmd+Return reveals in Finder.
* **Offline Files:** Clicking an offline file prompts user to reconnect volume.

### 4.3. Settings/Preferences Window
* **Volume Selection:**
    * List all mounted volumes with checkboxes to enable/disable indexing.
    * Show indexing progress (X of Y files indexed) for each volume.
    * "Rebuild Index" button for individual volumes.
* **MVP Scope:** Volume selection only. Defer folder exclusions, file type filters to post-MVP.

---

## 5. Technical Architecture

### 5.1. Database Design
**File Location:** `~/Library/Application Support/Prism/index.db`

**Tables:**
1. **Primary Files Table:** Stores stable file ID, path, volume UUID, size, dates, online status.
2. **FTS5 Virtual Table:** Indexes filename and extension for full-text search. Links to primary table via file ID.
3. **Audio Metadata Table:** Stores duration (seconds) for audio files. Links to primary table via file ID.

**Indexes Required:**
* Volume UUID (for filtering online/offline drives)
* Date Modified (for sorting)
* File Size (for sorting)
* Extension (for filtering)

**Performance Configuration:**
* WAL mode enabled (concurrent read/write during indexing)
* Synchronous mode: NORMAL (balance safety and speed)
* Batched inserts: 10,000 records per transaction
* FTS5 prefix indexing enabled for autocomplete-style search

**Database Recovery:**
* Detect corruption on startup using `PRAGMA integrity_check`.
* Offer user option to rebuild index if corruption detected.

### 5.2. Scanner Implementation
* **Primary API:** `getattrlistbulk` for batch file attribute retrieval.
* **Attributes Retrieved:** Name, size, modification date, creation date, file type.
* **Scan Strategy:** Breadth-first traversal to show progress evenly across directory tree.
* **Progress Reporting:** Update UI every 1000 files scanned.

### 5.3. FSEvents Integration
* **Latency Handling:** Coalesce events into 5-second windows to avoid thrashing database.
* **Event Types:** Monitor create, delete, rename, modify events.
* **Race Conditions:** Use file modification timestamps to detect stale events.
* **Bulk Operations:** Detect mass deletions/additions (threshold: 100+ files in one event) and trigger partial rescan instead of individual updates.

## 6. Roadmap

| Phase         | Objective         | Key Deliverables                                                                                                                |
|---------------|-------------------|---------------------------------------------------------------------------------------------------------------------------------|
| Phase 1 (MVP) | Core Search       | Main window UI (sidebar, search field, results table). `getattrlistbulk` scanner. Database schema. FTS5 search (<100ms first results). Settings window for volume selection. |
| Phase 2       | Live Monitoring   | FSEvents integration with event coalescing. DiskArbitration for mount/unmount detection. Offline drive handling.               |
| Phase 3       | Audio Enrichment  | Background worker for audio duration extraction (AVFoundation). Throttled processing queue. Duration column population.        |
| Phase 4       | User Experience   | Drag-and-drop support. QuickLook integration. Keyboard shortcuts. Context menu refinements.                                     |
| Post-MVP      | Advanced Features | Smart filters. Folder exclusions. File type filters. Saved searches.                                                           |

## 7. Out of Scope (MVP)
* Content search (searching inside files)
* Smart filters ("Audio Only", "Large Files", "Last 24 Hours")
* Folder exclusion preferences
* File type filtering preferences
* Saved search queries
* Tag/label support
* Network volume indexing


