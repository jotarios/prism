# Product Requirements Document: Prism

| Project Name | Prism (Working Title) |
| :--- | :--- |
| **Version** | 1.0 |
| **Status** | Draft |
| **Target Platform** | macOS 14.0+ (Sonoma) |
| **Architecture** | Swift (AppKit/SwiftUI Hybrid), SQLite, FTS5 |
| **Primary Use Case** | Indexing & Searching 5M+ Audio Files across External Drives |

---

## 1. Executive Summary
**Prism** is a high-performance desktop search utility for macOS, designed to function as a native alternative to "Everything" (Windows).

Unlike Spotlight, which relies on system-wide indexing (often slow or incomplete on external volumes), Prism maintains its own lightweight, high-speed database. It prioritizes **instant filename search** (sub-100ms latency) for massive libraries (5+ million files) stored on multiple external hard drives.

## 2. Design Principles (macOS HIG)
The application must adhere strictly to Apple’s [Human Interface Guidelines](https://developer.apple.com/design/human-interface-guidelines/designing-for-macos) to ensure it feels like a built-in system tool.

* **Immersive & Native:** Use standard window chrome, translucent sidebars, and system font stacks.
* **Speed as a Feature:** UI interactions (sorting, filtering) must happen within 16ms (60fps). Search queries must return within 100ms.
* **Predictable:** Standard keyboard shortcuts (`Cmd+F`, `Cmd+,`, `Spacebar` for QuickLook).

---

## 3. Functional Requirements

### 3.1. File System Indexing
* **Scope:** The app must scan selected internal folders and external volumes.
* **Methodology:** Use low-level BSD calls (`getattrlistbulk`) rather than `FileManager` enumeration for maximum speed.
* **Live Monitoring:** Implement `FSEvents` to track file creations, deletions, and renames in real-time.
* **Volume Handling:**
    * Detect when external drives are mounted/unmounted.
    * Identify drives by **Volume UUID**, not Volume Name (to handle renaming).
    * *Offline Mode:* Search results from disconnected drives remain visible but are visually dimmed.

### 3.2. Search Capabilities
* **Engine:** SQLite FTS5 (Full-Text Search 5).
* **Query Types:**
    * **Prefix Search:** `love` matches "Lover", "Lovely".
    * **Extension Filter:** `ext:mp3` or `type:audio`.
    * **Boolean:** `madonna AND live`.
* **Metadata Targets:**
    1.  Filename
    2.  File Path
    3.  Extension
    4.  Date Modified
    5.  File Size

### 3.3. Audio-Specific Logic
* **Deferred Metadata:**
    * *Phase 1 (Scan):* Index Name, Path, Size, Date (Instant).
    * *Phase 2 (Enrichment):* A background queue slowly opens files to read ID3 tags (Duration/Length). This prevents the initial scan from taking hours.

---

## 4. User Interface Specifications

### 4.1. Main Window Layout
* **Sidebar (`NSVisualEffectView` .sidebar):**
    * **Sources Section:** List connected drives with status indicators (Green=Live, Red=Offline).
    * **Smart Filters:** "Audio Only", "Large Files (>500MB)", "Last 24 Hours".
    * Use **SF Symbols 5** (e.g., `externaldrive.fill`, `music.mic`).
* **Toolbar:**
    * Unified title/toolbar style.
    * Centered, expansive Search Field.
* **Results Table:**
    * **Component:** `NSTableView` (AppKit) or heavily optimized SwiftUI `Table`.
    * **Columns:** Name, Date Modified, Size, Path, Duration (Empty until fetched).
    * **Typography:** Use `monospacedDigitSystemFont` for Size/Date to align numbers.
    * **Zebra Striping:** Enabled for readability.

### 4.2. Interactions
* **Virtualization:** The table must support 5 million rows but only render the visible rows (~40).
* **Context Menu:** Right-click on file -> "Show in Finder", "Open With", "Copy Path".
* **Drag & Drop:** Users must be able to drag a file from Prism directly into a DAW (Logic Pro, Ableton) or Finder.

---

## 5. Technical Architecture

### 5.1. Database Schema (SQLite)
**File Location:** `~/Library/Application Support/Prism/index.db`

```sql
-- 1. The Fast Index (FTS5)
-- Stores data needed for instant search and list rendering.
CREATE VIRTUAL TABLE search_index USING fts5(
    filename,           -- Indexed for text search
    extension,          -- Indexed
    path UNINDEXED,     -- Stored but not text-searched by default
    volume_uuid UNINDEXED
);

-- 2. The Metadata Store
-- Stores sortable attributes and "heavy" data (Duration).
CREATE TABLE file_meta (
    id INTEGER PRIMARY KEY,
    size_bytes INTEGER,
    date_modified INTEGER, -- Unix Timestamp
    duration_sec INTEGER DEFAULT 0, -- 0 until processed
    FOREIGN KEY(id) REFERENCES search_index(rowid)
);


### 5.2. Performance Settings

WAL Mode: PRAGMA journal_mode=WAL; (Write-Ahead Logging allows reading while scanning).

Synchronous: PRAGMA synchronous=NORMAL;.

Batching: Insert records in transactions of 10,000 items to reduce disk I/O overhead.

## 6. Roadmap

| Phase         | Objective    | Key Deliverables                                                                                       |
|---------------|--------------|--------------------------------------------------------------------------------------------------------|
| Phase 1 (MVP) | The Scanner  | Basic UI. getattrlistbulk scanner implementation. Database creation. Search returns results in <100ms. |
| Phase 2       | The Watcher  | FSEvents integration. Handling External Drive mount/unmount events smoothly.                           |
| Phase 3       | The Metadata | Background worker for ID3 tag reading (Duration). Drag-and-drop support.                               |
| Phase 4       | Polish       | Dark Mode refinements. QuickLook integration (Spacebar preview).                                       |


