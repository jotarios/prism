//
//  FileRecord.swift
//  prism
//

import Foundation

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
    var durationSeconds: Double?
}

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

/// Entries carry the full row payload (not just ids) so applyDiff can
/// rebuild SearchResult without a second IN(...) query against DuckDB.
/// That round-trip costs ~6s on a 27K-row rescan; the extra fields cost
/// nothing measurable.
struct ScanDiff: Sendable {
    struct Entry: Sendable {
        let id: Int64
        let filename: String
        let path: String
        let volumeUUID: String
        let ext: String
        let sizeBytes: Int64
        let dateModified: Int64   // unix seconds
        let dateCreated: Int64

        static func from(scannedFile file: ScannedFile, volumeUUID: String) -> Entry {
            let path = file.parentPath + "/" + file.filename
            return Entry(
                id: PathHash.id(volumeUUID: volumeUUID, path: path),
                filename: file.filename,
                path: path,
                volumeUUID: volumeUUID,
                ext: file.ext,
                sizeBytes: Int64(file.sizeBytes),
                dateModified: Int64(file.modTimeSec),
                dateCreated: Int64(file.createTimeSec)
            )
        }
    }

    let added: [Entry]
    let modified: [Entry]
    let removedIds: [Int64]

    var isEmpty: Bool { added.isEmpty && modified.isEmpty && removedIds.isEmpty }

    static let empty = ScanDiff(added: [], modified: [], removedIds: [])
}

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
