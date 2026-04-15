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
