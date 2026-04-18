//
//  FileRecord.swift
//  prism
//

import Foundation

/// Represents a file indexed in the database
struct FileRecord: Identifiable {
    let id: Int64
    let filename: String
    let path: String
    let volumeUUID: String
    let ext: String
    let sizeBytes: Int64
    let dateModified: Date
    let dateCreated: Date
    let isOnline: Bool

    /// Audio-specific metadata (nil for non-audio files)
    var durationSeconds: Double?
}

/// Minimal record for insertion during scanning
struct FileRecordInsert: Sendable {
    let filename: String
    let path: String
    let volumeUUID: String
    let ext: String
    let sizeBytes: Int64
    let dateModified: Date
    let dateCreated: Date
    let isOnline: Bool
}

/// Diff produced by a scan: rows added, rows whose content changed, and rows
/// that vanished. Carries the full row data needed by both consumers:
///   - `DatabaseManager.syncSearchIndex(...)` — uses id/filename/ext for FTS5
///   - `DuckDBStore.applyDiff(...)` — uses the rest to rebuild SearchResult
///     for the in-memory cache without re-querying DuckDB
/// Carrying the full payload costs a few extra fields per diff entry but
/// eliminates an `IN (27K-ids)` round-trip to DuckDB on every rescan.
struct ScanDiff: Sendable {
    struct Entry: Sendable {
        let id: Int64
        let filename: String
        let path: String
        let volumeUUID: String
        let ext: String
        let sizeBytes: Int64
        let dateModified: Int64   // unix seconds — Date materialized at point of use
        let dateCreated: Int64
    }

    let added: [Entry]
    let modified: [Entry]
    let removedIds: [Int64]

    var isEmpty: Bool { added.isEmpty && modified.isEmpty && removedIds.isEmpty }

    static let empty = ScanDiff(added: [], modified: [], removedIds: [])
}

/// Search result record optimized for display
struct SearchResult: Identifiable, Equatable {
    let id: Int64
    let filename: String
    let path: String
    let volumeUUID: String
    let ext: String
    let sizeBytes: Int64
    let dateModified: Date
    let isOnline: Bool
    let durationSeconds: Double?

    var formattedSize: String {
        ByteCountFormatter.string(fromByteCount: sizeBytes, countStyle: .file)
    }

    var formattedDuration: String {
        guard let duration = durationSeconds else { return "—" }
        let minutes = Int(duration) / 60
        let seconds = Int(duration) % 60
        return String(format: "%d:%02d", minutes, seconds)
    }
}
