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
/// that vanished. Lets `DatabaseManager.syncSearchIndex(from:volumeUUID:diff:)`
/// propagate only the delta to SQLite/FTS5 and the in-memory cache.
struct ScanDiff: Sendable {
    /// Subset of fields sync needs, carried alongside the id so the sync step
    /// doesn't re-round-trip DuckDB.
    struct Entry: Sendable {
        let id: Int64
        let filename: String
        let ext: String
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
