//
//  DuckDBCache.swift
//  prism
//

import Foundation
import DuckDB

// In-memory hot path for search. One per store. Internal NSLock; all methods
// are thread-safe. The only I/O is the initial load reader closure.
nonisolated final class DuckDBCache: @unchecked Sendable {
    typealias FullLoadReader = () throws -> [SearchResult]

    private let lock = NSLock()
    private var entries: [Int64: SearchResult] = [:]
    private var isLoaded = false

    private func withLock<T>(_ body: () throws -> T) rethrows -> T {
        lock.lock()
        defer { lock.unlock() }
        return try body()
    }

    func load(using reader: FullLoadReader) throws {
        let start = CFAbsoluteTimeGetCurrent()
        let results = try reader()
        withLock {
            entries.removeAll()
            for r in results { entries[r.id] = r }
            isLoaded = true
            Log.debug("Cache loaded: \(entries.count) entries in \(String(format: "%.2f", CFAbsoluteTimeGetCurrent() - start))s")
        }
    }

    func invalidate() {
        withLock {
            entries.removeAll()
            isLoaded = false
        }
    }

    var loaded: Bool {
        withLock { isLoaded }
    }

    // Added rows default to is_online=TRUE. Modified rows preserve their
    // existing is_online — offline is only flipped via setVolumeOnline.
    func applyDiff(_ diff: ScanDiff) {
        let shouldApply = withLock { isLoaded }
        guard shouldApply else { return }

        var built: [SearchResult] = []
        built.reserveCapacity(diff.added.count + diff.modified.count)
        for entry in diff.added {
            built.append(Self.searchResult(from: entry, isOnline: true))
        }
        for entry in diff.modified {
            let preserved = withLock { entries[entry.id]?.isOnline } ?? true
            built.append(Self.searchResult(from: entry, isOnline: preserved))
        }

        withLock {
            for id in diff.removedIds { entries.removeValue(forKey: id) }
            for r in built { entries[r.id] = r }
        }
    }

    func setVolumeOnline(_ volumeUUID: String, isOnline: Bool) {
        withLock {
            for (id, result) in entries where result.volumeUUID == volumeUUID && result.isOnline != isOnline {
                entries[id] = SearchResult(
                    id: result.id,
                    filename: result.filename,
                    path: result.path,
                    volumeUUID: result.volumeUUID,
                    ext: result.ext,
                    sizeBytes: result.sizeBytes,
                    dateModified: result.dateModified,
                    isOnline: isOnline,
                    durationSeconds: result.durationSeconds
                )
            }
        }
    }

    func dropVolume(_ volumeUUID: String) {
        withLock {
            entries = entries.filter { $0.value.volumeUUID != volumeUUID }
        }
    }

    func result(for id: Int64) -> SearchResult? {
        withLock { entries[id] }
    }

    /// Returns nil when the cache isn't loaded — caller falls back to DB.
    func results(for ids: [Int64]) -> [SearchResult]? {
        withLock {
            isLoaded ? ids.compactMap { entries[$0] } : nil
        }
    }

    /// Returns nil when the cache isn't loaded — caller falls back to DB.
    func allSortedByDateDesc(limit: Int) -> [SearchResult]? {
        withLock {
            isLoaded ? Array(entries.values.sorted { $0.dateModified > $1.dateModified }.prefix(limit)) : nil
        }
    }

    func allValues() -> [SearchResult] {
        withLock { Array(entries.values) }
    }

    private static func searchResult(from entry: ScanDiff.Entry, isOnline: Bool) -> SearchResult {
        SearchResult(
            id: entry.id,
            filename: entry.filename,
            path: entry.path,
            volumeUUID: entry.volumeUUID,
            ext: entry.ext,
            sizeBytes: entry.sizeBytes,
            dateModified: Date(timeIntervalSince1970: Double(entry.dateModified)),
            isOnline: isOnline,
            durationSeconds: nil
        )
    }
}
