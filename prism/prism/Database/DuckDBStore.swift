//
//  DuckDBStore.swift
//  prism
//

import Foundation
import DuckDB

enum IndexError: Error {
    case scanAlreadyInProgress(volumeUUID: String)
    case noActiveScan
    case hashCollision(path: String, volumeUUID: String)
}

// Thread-safe wrapper around a single DuckDB.Connection. All access to the
// connection and to the in-memory cache is serialized via `lock`, because
// DuckDB's Swift Connection is not safe for concurrent use from multiple
// threads. Callers can hit DuckDBStore from any queue/actor.
final class DuckDBStore: @unchecked Sendable {
    private let database: Database
    private let connection: Connection
    let dbPath: String

    private let lock = NSLock()
    private var cache: [Int64: SearchResult] = [:]
    private var cacheLoaded = false

    // Scan slot. `beginScan` sets this to the active volume; `mergeAndDiff`
    // clears it. A non-nil value blocks concurrent scans.
    private var currentScanVolume: String?

    private func sync<T>(_ body: () throws -> T) rethrows -> T {
        lock.lock()
        defer { lock.unlock() }
        return try body()
    }

    init(path: String? = nil) throws {
        let appSupport = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
        let prismDir = appSupport.appendingPathComponent("Prism", isDirectory: true)
        try? FileManager.default.createDirectory(at: prismDir, withIntermediateDirectories: true)

        let resolvedPath = path ?? prismDir.appendingPathComponent("metadata.duckdb").path
        dbPath = resolvedPath

        database = try Database(store: .file(at: URL(fileURLWithPath: resolvedPath)))
        connection = try database.connect()
        try createSchema()
        try cleanupOrphanedStaging()
    }

    private func createSchema() throws {
        try sync {
        try connection.execute("""
            CREATE TABLE IF NOT EXISTS files (
                id BIGINT PRIMARY KEY,
                filename VARCHAR NOT NULL,
                path VARCHAR NOT NULL,
                volume_uuid VARCHAR NOT NULL,
                extension VARCHAR NOT NULL,
                size_bytes BIGINT NOT NULL,
                date_modified BIGINT NOT NULL,
                date_created BIGINT NOT NULL,
                is_online BOOLEAN NOT NULL DEFAULT TRUE
            )
        """)
        try connection.execute("CREATE INDEX IF NOT EXISTS idx_files_volume ON files(volume_uuid)")
        // Uniqueness of (volume_uuid, path) is enforced indirectly via the
        // `id` PRIMARY KEY, since id = PathHash.id(volume_uuid, path). A
        // dedicated UNIQUE index on (volume_uuid, path) costs per-row index
        // maintenance on every INSERT and isn't needed for correctness —
        // merge joins use id, not (volume, path).
        }
    }

    /// Drop any staging tables left behind by a crashed scan. Called once at
    /// init. Staging tables follow the pattern `files_staging_<sanitized_uuid>`.
    /// Internal rather than private so tests can drive it without juggling
    /// DuckDB's process-level file lock.
    internal func cleanupOrphanedStaging() throws {
        try sync {
            let result = try connection.query("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'main' AND table_name LIKE 'files_staging_%'
            """)
            let col = result[0].cast(to: String.self)
            for idx in 0..<result.rowCount {
                guard let name = col[idx] else { continue }
                // Table name came from information_schema; safe to interpolate
                // back after quoting. Still double-quote to be defensive.
                try connection.execute("DROP TABLE IF EXISTS \"\(name)\"")
                Log.debug("Dropped orphaned staging table: \(name)")
            }
        }
    }

    /// Replace UUID characters that are awkward in identifiers. Output is
    /// always safe to quote as a DuckDB identifier.
    private static func sanitizeVolumeForTable(_ uuid: String) -> String {
        uuid.replacingOccurrences(of: "-", with: "_")
    }

    private static func stagingTableName(for volumeUUID: String) -> String {
        "files_staging_\(sanitizeVolumeForTable(volumeUUID))"
    }

    // MARK: - Scan Lifecycle

    /// Begin a scan of `volumeUUID`. Acquires the scan slot and (re)creates
    /// an empty per-volume staging table. Concurrent scans throw
    /// `scanAlreadyInProgress`.
    func beginScan(volumeUUID: String) throws {
        try sync {
            if let existing = currentScanVolume {
                throw IndexError.scanAlreadyInProgress(volumeUUID: existing)
            }

            let staging = DuckDBStore.stagingTableName(for: volumeUUID)
            try connection.execute("DROP TABLE IF EXISTS \"\(staging)\"")
            try connection.execute("""
                CREATE TABLE "\(staging)" (
                    id BIGINT NOT NULL,
                    filename VARCHAR NOT NULL,
                    path VARCHAR NOT NULL,
                    volume_uuid VARCHAR NOT NULL,
                    extension VARCHAR NOT NULL,
                    size_bytes BIGINT NOT NULL,
                    date_modified BIGINT NOT NULL,
                    date_created BIGINT NOT NULL
                )
            """)

            currentScanVolume = volumeUUID
            Log.debug("beginScan volume=\(volumeUUID)")
        }
    }

    // MARK: - Ingestion

    /// Ingest a batch of scanned files. When a scan is active (beginScan was
    /// called and mergeAndDiff hasn't yet run), rows land in the per-volume
    /// staging table. Otherwise — tests, one-shots — this routes through a
    /// direct Appender into `files` with hash-derived ids. That path is fast
    /// on an empty or newly-deleted-for-volume table; if the caller ingests
    /// the same (volume, path) twice, the second call violates the UNIQUE
    /// constraint (use beginScan/mergeAndDiff for idempotent rescans).
    func ingestBatch(_ files: [ScannedFile], volumeUUID: String) throws {
        try sync {
            if let scanning = currentScanVolume, scanning == volumeUUID {
                try ingestToStagingLocked(files: files, volumeUUID: volumeUUID)
            } else {
                try ingestDirectLocked(files: files, volumeUUID: volumeUUID)
            }
        }
    }

    private func ingestToStagingLocked(files: [ScannedFile], volumeUUID: String) throws {
        let staging = DuckDBStore.stagingTableName(for: volumeUUID)
        let appender = try Appender(connection: connection, table: staging)

        for file in files {
            let path = file.parentPath + "/" + file.filename
            let id = PathHash.id(volumeUUID: volumeUUID, path: path)
            try appender.append(id)
            try appender.append(file.filename)
            try appender.append(path)
            try appender.append(volumeUUID)
            try appender.append(file.ext)
            try appender.append(Int64(file.sizeBytes))
            try appender.append(Int64(file.modTimeSec))
            try appender.append(Int64(file.createTimeSec))
            try appender.endRow()
        }

        try appender.flush()
    }

    /// Direct Appender into `files`. Fast path for non-scan callers (tests,
    /// one-shots). Caller is responsible for not re-inserting the same
    /// (volume_uuid, path) — would trip the PK via hash equality. The
    /// scan-time path uses staging + merge to handle rescans safely.
    private func ingestDirectLocked(files: [ScannedFile], volumeUUID: String) throws {
        let appender = try Appender(connection: connection, table: "files")
        for file in files {
            let path = file.parentPath + "/" + file.filename
            let id = PathHash.id(volumeUUID: volumeUUID, path: path)
            try appender.append(id)
            try appender.append(file.filename)
            try appender.append(path)
            try appender.append(volumeUUID)
            try appender.append(file.ext)
            try appender.append(Int64(file.sizeBytes))
            try appender.append(Int64(file.modTimeSec))
            try appender.append(Int64(file.createTimeSec))
            try appender.append(true)       // is_online
            try appender.endRow()
        }
        try appender.flush()
    }

    // MARK: - Merge

    /// Compute the scan's diff against `files`, apply it to DuckDB, drop the
    /// staging table, release the scan slot, and return the diff so the
    /// SQLite sync step can propagate the same set of changes.
    ///
    /// SQL shape (three logical queries, one transaction):
    ///   added    = staging rows whose (volume, path) isn't in files
    ///   modified = staging rows whose (volume, path) is in files but
    ///              (size_bytes, date_modified) differ
    ///   removed  = files rows (for this volume) whose (path) isn't in staging
    func mergeAndDiff(volumeUUID: String) throws -> ScanDiff {
        try sync {
            guard let scanning = currentScanVolume, scanning == volumeUUID else {
                throw IndexError.noActiveScan
            }

            let staging = DuckDBStore.stagingTableName(for: volumeUUID)

            var added: [ScanDiff.Entry] = []
            var modified: [ScanDiff.Entry] = []
            var removedIds: [Int64] = []

            try connection.execute("BEGIN TRANSACTION")
            do {
                let quotedVolume = "'\(volumeUUID.replacingOccurrences(of: "'", with: "''"))'"

                // Joins use `id` (= PathHash.id(volume, path)) rather than
                // (volume_uuid, path). The PK gives us a hash lookup instead
                // of a two-column scan, and the semantics are identical by
                // construction: same id ⇔ same (volume, path).

                // --- ADDED: staging rows whose id isn't in files.
                let addedResult = try connection.query("""
                    SELECT s.id, s.filename, s.extension
                    FROM "\(staging)" s
                    LEFT JOIN files f ON f.id = s.id
                    WHERE f.id IS NULL
                """)
                added = extractEntries(from: addedResult)

                // --- MODIFIED: matching id, differing (size, mtime).
                let modifiedResult = try connection.query("""
                    SELECT s.id, s.filename, s.extension
                    FROM "\(staging)" s
                    JOIN files f ON f.id = s.id
                    WHERE f.size_bytes <> s.size_bytes OR f.date_modified <> s.date_modified
                """)
                modified = extractEntries(from: modifiedResult)

                // --- REMOVED: files rows on this volume whose id isn't in staging.
                let removedResult = try connection.query("""
                    SELECT f.id FROM files f
                    LEFT JOIN "\(staging)" s ON s.id = f.id
                    WHERE f.volume_uuid = \(quotedVolume)
                      AND s.id IS NULL
                """)
                removedIds = extractIds(from: removedResult)

                // --- Apply to DuckDB. Order matters: INSERT before DELETE.
                if !added.isEmpty {
                    try connection.execute("""
                        INSERT INTO files (id, filename, path, volume_uuid, extension,
                                           size_bytes, date_modified, date_created, is_online)
                        SELECT s.id, s.filename, s.path, s.volume_uuid, s.extension,
                               s.size_bytes, s.date_modified, s.date_created, TRUE
                        FROM "\(staging)" s
                        LEFT JOIN files f ON f.id = s.id
                        WHERE f.id IS NULL
                    """)
                }
                if !modified.isEmpty {
                    // UPDATE excludes `is_online` so a Phase-5 offline flag
                    // isn't clobbered by rescans.
                    try connection.execute("""
                        UPDATE files
                        SET filename = s.filename,
                            extension = s.extension,
                            size_bytes = s.size_bytes,
                            date_modified = s.date_modified,
                            date_created = s.date_created
                        FROM "\(staging)" s
                        WHERE files.id = s.id
                          AND (files.size_bytes <> s.size_bytes OR files.date_modified <> s.date_modified)
                    """)
                }
                if !removedIds.isEmpty {
                    // DuckDB has no parameter-count limit like SQLite's 32K,
                    // but we chunk for readability and DELETE query size.
                    for chunk in removedIds.chunked(into: 1000) {
                        let list = chunk.map(String.init).joined(separator: ",")
                        try connection.execute("""
                            DELETE FROM files WHERE id IN (\(list))
                        """)
                    }
                }

                try connection.execute("DROP TABLE IF EXISTS \"\(staging)\"")
                try connection.execute("COMMIT")
            } catch {
                try? connection.execute("ROLLBACK")
                // Leave staging table for the next beginScan to clean up.
                currentScanVolume = nil
                throw error
            }

            currentScanVolume = nil

            let diff = ScanDiff(added: added, modified: modified, removedIds: removedIds)
            Log.debug("mergeAndDiff volume=\(volumeUUID) added=\(diff.added.count) modified=\(diff.modified.count) removed=\(diff.removedIds.count)")
            return diff
        }
    }

    private func extractEntries(from result: ResultSet) -> [ScanDiff.Entry] {
        let idCol = result[0].cast(to: Int64.self)
        let nameCol = result[1].cast(to: String.self)
        let extCol = result[2].cast(to: String.self)
        var out: [ScanDiff.Entry] = []
        out.reserveCapacity(Int(result.rowCount))
        for i in 0..<result.rowCount {
            guard let id = idCol[i], let name = nameCol[i], let ext = extCol[i] else { continue }
            out.append(ScanDiff.Entry(id: id, filename: name, ext: ext))
        }
        return out
    }

    private func extractIds(from result: ResultSet) -> [Int64] {
        let idCol = result[0].cast(to: Int64.self)
        var out: [Int64] = []
        out.reserveCapacity(Int(result.rowCount))
        for i in 0..<result.rowCount {
            if let id = idCol[i] { out.append(id) }
        }
        return out
    }

    // MARK: - Queries

    func getFileCount() throws -> Int {
        try sync {
            let result = try connection.query("SELECT COUNT(*) FROM files")
            let col = result[0].cast(to: Int64.self)
            return Int(col[0] ?? 0)
        }
    }

    func getFileCountByVolume(_ volumeUUID: String) throws -> Int {
        try sync {
            let stmt = try PreparedStatement(connection: connection, query: "SELECT COUNT(*) FROM files WHERE volume_uuid = $1")
            try stmt.bind(volumeUUID, at: 1)
            let result = try stmt.execute()
            let col = result[0].cast(to: Int64.self)
            return Int(col[0] ?? 0)
        }
    }

    func loadCache() throws {
        try sync {
            let start = CFAbsoluteTimeGetCurrent()
            let result = try connection.query("""
                SELECT id, filename, path, volume_uuid, extension,
                       size_bytes, date_modified, date_created, is_online
                FROM files
            """)
            cache.removeAll()
            let results = extractSearchResults(from: result)
            for r in results {
                cache[r.id] = r
            }
            cacheLoaded = true
            Log.debug("Cache loaded: \(cache.count) entries in \(String(format: "%.2f", CFAbsoluteTimeGetCurrent() - start))s")
        }
    }

    func invalidateCache() {
        sync {
            cache.removeAll()
            cacheLoaded = false
        }
    }

    /// Apply an already-computed scan diff to the in-memory cache. Called
    /// after `syncSearchIndex(from:volumeUUID:diff:)` so the cache stays
    /// consistent without an O(N) `loadCache()` reload.
    func applyDiff(_ diff: ScanDiff) throws {
        try sync {
            // If the cache was never loaded, first-time `loadCache` is still
            // the cheaper and simpler path. Callers can decide.
            guard cacheLoaded else { return }

            for id in diff.removedIds {
                cache.removeValue(forKey: id)
            }

            let affectedIds = diff.added.map(\.id) + diff.modified.map(\.id)
            guard !affectedIds.isEmpty else { return }

            // Fetch full row data for added/modified ids so the cache holds
            // display-ready SearchResult values, not just the sync-minimal
            // subset carried in ScanDiff.Entry.
            for chunk in affectedIds.chunked(into: 1000) {
                let list = chunk.map(String.init).joined(separator: ",")
                let result = try connection.query("""
                    SELECT id, filename, path, volume_uuid, extension,
                           size_bytes, date_modified, date_created, is_online
                    FROM files WHERE id IN (\(list))
                """)
                for r in extractSearchResults(from: result) {
                    cache[r.id] = r
                }
            }
        }
    }

    func getAllCachedValues() -> [SearchResult] {
        sync { Array(cache.values) }
    }

    func getFilesByIDs(_ ids: [Int64]) throws -> [SearchResult] {
        guard !ids.isEmpty else { return [] }
        return try sync {
            if cacheLoaded {
                return ids.compactMap { cache[$0] }
            }

            try connection.execute("CREATE OR REPLACE TEMP TABLE _lookup (id BIGINT, ord BIGINT)")
            let appender = try Appender(connection: connection, table: "_lookup")
            for (idx, id) in ids.enumerated() {
                try appender.append(id)
                try appender.append(Int64(idx))
                try appender.endRow()
            }
            try appender.flush()

            let result = try connection.query("""
                SELECT f.id, f.filename, f.path, f.volume_uuid, f.extension,
                       f.size_bytes, f.date_modified, f.date_created, f.is_online
                FROM files f JOIN _lookup l ON f.id = l.id
                ORDER BY l.ord
            """)
            let results = extractSearchResults(from: result)
            try connection.execute("DROP TABLE IF EXISTS _lookup")
            return results
        }
    }

    func getAllFiles(limit: Int = 1000) throws -> [SearchResult] {
        try sync {
            if cacheLoaded {
                return Array(cache.values.sorted { $0.dateModified > $1.dateModified }.prefix(limit))
            }
            let stmt = try PreparedStatement(connection: connection, query: """
                SELECT id, filename, path, volume_uuid, extension,
                       size_bytes, date_modified, date_created, is_online
                FROM files ORDER BY date_modified DESC LIMIT $1
            """)
            try stmt.bind(Int64(limit), at: 1)
            let result = try stmt.execute()
            return extractSearchResults(from: result)
        }
    }

    private func extractSearchResults(from result: ResultSet) -> [SearchResult] {
        let idCol = result[0].cast(to: Int64.self)
        let filenameCol = result[1].cast(to: String.self)
        let pathCol = result[2].cast(to: String.self)
        let volumeCol = result[3].cast(to: String.self)
        let extCol = result[4].cast(to: String.self)
        let sizeCol = result[5].cast(to: Int64.self)
        let modCol = result[6].cast(to: Int64.self)
        let onlineCol = result[8].cast(to: Bool.self)

        var results: [SearchResult] = []
        let rowCount = result.rowCount
        for idx in 0..<rowCount {
            guard let id = idCol[idx],
                  let filename = filenameCol[idx],
                  let path = pathCol[idx],
                  let volumeUUID = volumeCol[idx],
                  let ext = extCol[idx],
                  let sizeBytes = sizeCol[idx],
                  let dateMod = modCol[idx] else { continue }

            results.append(SearchResult(
                id: id,
                filename: filename,
                path: path,
                volumeUUID: volumeUUID,
                ext: ext,
                sizeBytes: sizeBytes,
                dateModified: Date(timeIntervalSince1970: Double(dateMod)),
                isOnline: onlineCol[idx] ?? true,
                durationSeconds: nil
            ))
        }
        return results
    }

    // MARK: - Sync to SQLite

    func iterateAllForSync(batchSize: Int = 10_000, handler: ([SyncRecord]) throws -> Void) throws {
        // Snapshot all rows under the lock, then invoke the handler outside
        // the lock so the SQLite writer doesn't block DuckDB reads.
        var allRecords: [SyncRecord] = []
        try sync {
            let result = try connection.query("SELECT id, filename, extension FROM files")
            let idCol = result[0].cast(to: Int64.self)
            let filenameCol = result[1].cast(to: String.self)
            let extCol = result[2].cast(to: String.self)
            let rowCount = result.rowCount
            allRecords.reserveCapacity(Int(rowCount))
            for idx in 0..<rowCount {
                guard let id = idCol[idx],
                      let filename = filenameCol[idx],
                      let ext = extCol[idx] else { continue }
                allRecords.append(SyncRecord(id: id, filename: filename, ext: ext))
            }
        }

        var batch: [SyncRecord] = []
        batch.reserveCapacity(batchSize)
        for record in allRecords {
            batch.append(record)
            if batch.count >= batchSize {
                try handler(batch)
                batch.removeAll(keepingCapacity: true)
            }
        }
        if !batch.isEmpty {
            try handler(batch)
        }
    }

    // MARK: - Internal Access (for benchmarks)

    func cachedResult(for id: Int64) -> SearchResult? {
        sync { cache[id] }
    }

    func connection_query(_ sql: String) throws -> ResultSet {
        try sync { try connection.query(sql) }
    }

    func connection_execute(_ sql: String) throws {
        try sync { try connection.execute(sql) }
    }

    func createAppender(table: String) throws -> Appender {
        try sync { try Appender(connection: connection, table: table) }
    }

    func extractResults(from result: ResultSet) -> [SearchResult] {
        extractSearchResults(from: result)
    }

    // MARK: - DuckDB-native Search

    func searchByFilename(query: String, limit: Int = 1000) throws -> [SearchResult] {
        try sync {
            let stmt = try PreparedStatement(connection: connection, query: """
                SELECT id, filename, path, volume_uuid, extension,
                       size_bytes, date_modified, date_created, is_online
                FROM files WHERE filename ILIKE $1 LIMIT $2
            """)
            try stmt.bind("%" + query + "%", at: 1)
            try stmt.bind(Int64(limit), at: 2)
            let result = try stmt.execute()
            return extractSearchResults(from: result)
        }
    }

    // MARK: - Volume Operations

    func deleteFilesByVolume(_ volumeUUID: String) throws {
        try sync {
            #if DEBUG
            let resultBefore = try connection.query("SELECT COUNT(*) FROM files")
            let countBefore = Int(resultBefore[0].cast(to: Int64.self)[0] ?? 0)
            #endif
            let stmt = try PreparedStatement(connection: connection, query: "DELETE FROM files WHERE volume_uuid = $1")
            try stmt.bind(volumeUUID, at: 1)
            _ = try stmt.execute()
            // Drop any cached entries belonging to this volume; otherwise
            // searches hit stale rows via the hot-path cache.
            cache = cache.filter { $0.value.volumeUUID != volumeUUID }
            #if DEBUG
            let resultAfter = try connection.query("SELECT COUNT(*) FROM files")
            let countAfter = Int(resultAfter[0].cast(to: Int64.self)[0] ?? 0)
            Log.debug("deleteFilesByVolume '\(volumeUUID)': \(countBefore) → \(countAfter) (\(countBefore - countAfter) deleted)")
            #endif
        }
    }

    func clearAll() throws {
        try sync {
            try connection.execute("DELETE FROM files")
            cache.removeAll()
            cacheLoaded = false
        }
    }
}

struct SyncRecord: Sendable {
    let id: Int64
    let filename: String
    let ext: String
}

private extension Array {
    func chunked(into size: Int) -> [[Element]] {
        stride(from: 0, to: count, by: size).map {
            Array(self[$0..<Swift.min($0 + size, count)])
        }
    }
}
