//
//  LiveIndexDiffBuilder.swift
//  prism
//

import Foundation

struct PendingEvent: Sendable {
    let path: String
    let flags: UInt32
    let eventId: UInt64
}

/// Pure-function ScanDiff construction. Stateless; one per batch.
struct LiveIndexDiffBuilder {
    /// Looks up an existing cache entry. Returning `nil` means "not in cache,
    /// treat as added." Returning a value lets the builder compare stat
    /// fields against what's already known and drop true no-ops (file read,
    /// atime touched, but mtime/size identical to cached row).
    typealias CacheProbe = (_ id: Int64) -> CachedStat?

    /// Subset of cached row used for change detection.
    struct CachedStat {
        let sizeBytes: Int64
        let dateModified: Int64
    }

    let volumeUUID: String
    let cacheProbe: CacheProbe

    // Groups events by parent dir to amortize stat via one
    // BulkScanner.scanDirectory per dir. ItemRenamed fires on both old and
    // new paths with no reliable pairing in the stream, so every renamed
    // path gets removed by PathHash; the new name (if the file still
    // exists) re-adds on the directory walk. Acceptable transient
    // duplicate window.
    func build(from events: [PendingEvent]) -> ScanDiff {
        var byDir: [String: [PendingEvent]] = [:]
        // Sets, not arrays: FSEvents can fire the same flag multiple times
        // for one path (e.g. Removed + Renamed-to-Trash, or two
        // back-to-back Modifies). Counting each as a separate "change"
        // double-ticks the meter and bloats applyDirectDiff chunks.
        var removeIdSet: Set<Int64> = []
        var renameOldPaths: Set<String> = []

        let audioExts = BulkScanner.audioExtensions

        for event in events {
            let path = event.path
            let flags = event.flags
            let ext = (path as NSString).pathExtension.lowercased()
            let isAudio = audioExts.contains(ext)

            let isRemoved = flags & UInt32(kFSEventStreamEventFlagItemRemoved) != 0
            let isRenamed = flags & UInt32(kFSEventStreamEventFlagItemRenamed) != 0

            if isRemoved {
                removeIdSet.insert(PathHash.id(volumeUUID: volumeUUID, path: path))
                continue
            }

            if isRenamed {
                renameOldPaths.insert(path)
            }

            guard isAudio else { continue }

            let dir = (path as NSString).deletingLastPathComponent
            byDir[dir, default: []].append(event)
        }

        for oldPath in renameOldPaths {
            removeIdSet.insert(PathHash.id(volumeUUID: volumeUUID, path: oldPath))
        }

        var addedById: [Int64: ScanDiff.Entry] = [:]
        var modifiedById: [Int64: ScanDiff.Entry] = [:]

        for (dir, evs) in byDir {
            let snapshot = BulkScanner.scanDirectory(atPath: dir)
            let wantedNames = Set(evs.map { ($0.path as NSString).lastPathComponent })
            for scanned in snapshot.audioFiles where wantedNames.contains(scanned.filename) {
                let entry = ScanDiff.Entry.from(scannedFile: scanned, volumeUUID: volumeUUID)
                if let cached = cacheProbe(entry.id) {
                    // No-op modify (atime/xattr touch with identical content).
                    if cached.sizeBytes == entry.sizeBytes &&
                       cached.dateModified == entry.dateModified {
                        continue
                    }
                    modifiedById[entry.id] = entry
                } else {
                    addedById[entry.id] = entry
                }
            }
        }

        // A path that's both added/modified AND in removeIdSet is
        // contradictory (file existed in snapshot but was removed in
        // the same batch). Trust the removal — file is gone now.
        for id in removeIdSet {
            addedById.removeValue(forKey: id)
            modifiedById.removeValue(forKey: id)
        }

        return ScanDiff(
            added: Array(addedById.values),
            modified: Array(modifiedById.values),
            removedIds: Array(removeIdSet)
        )
    }
}
