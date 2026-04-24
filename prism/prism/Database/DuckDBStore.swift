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
    case invalidVolumeUUID(String)
}

// Thread-safe DuckDB store. Writes flow through a single writer connection;
// reads flow through a pool of reader connections. The in-memory cache has
// its own lock, separate from the DuckDB layer. `currentScanVolume` is
// accessed only inside `writer.sync { }`, so its mutations are serialized.
nonisolated final class DuckDBStore: @unchecked Sendable {
    static let defaultReaderCount = 3

    private let database: Database
    let writer: WriterConnection
    let readers: ReaderPool
    let dbPath: String

    private let cacheLock = NSLock()
    private var cache: [Int64: SearchResult] = [:]
    private var cacheLoaded = false

    // Scan slot. `beginScan` sets this to the active volume; `mergeAndDiff`
    // clears it. A non-nil value blocks concurrent scans *on this volume*.
    // A concurrent ingestBatch for a different volumeUUID will take the
    // `ingestDirectOnWriter` path and write directly to `files` — this is
    // only used by tests and one-shots; production scan paths all come
    // through beginScan first. All reads/writes of this field happen inside
    // `writer.sync { }`, so no extra lock is needed.
    private var currentScanVolume: String?

    private func withCacheLock<T>(_ body: () throws -> T) rethrows -> T {
        cacheLock.lock()
        defer { cacheLock.unlock() }
        return try body()
    }

    init(path: String? = nil, readerCount: Int = DuckDBStore.defaultReaderCount) throws {
        let appSupport = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
        let prismDir = appSupport.appendingPathComponent("Prism", isDirectory: true)
        try? FileManager.default.createDirectory(at: prismDir, withIntermediateDirectories: true)

        let resolvedPath = path ?? prismDir.appendingPathComponent("metadata.duckdb").path
        dbPath = resolvedPath

        database = try Database(store: .file(at: URL(fileURLWithPath: resolvedPath)))
        writer = try WriterConnection(database: database)
        readers = try ReaderPool(database: database, count: readerCount)
        try createSchema()
        try cleanupOrphanedStaging()
    }

    private func createSchema() throws {
        try writer.sync { conn in
            try conn.execute("""
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
            try conn.execute("CREATE INDEX IF NOT EXISTS idx_files_volume ON files(volume_uuid)")
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
        try writer.sync { conn in
            let result = try conn.query("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'main' AND table_name LIKE 'files_staging_%'
            """)
            let col = result[0].cast(to: String.self)
            for idx in 0..<result.rowCount {
                guard let name = col[idx] else { continue }
                // Table name came from information_schema; safe to interpolate
                // back after quoting. Still double-quote to be defensive.
                try conn.execute("DROP TABLE IF EXISTS \"\(name)\"")
                Log.debug("Dropped orphaned staging table: \(name)")
            }
        }
    }

    /// Replace UUID characters that are awkward in identifiers, then enforce
    /// an allowlist. macOS `volumeUUIDStringKey` always returns canonical
    /// CFUUID format (hex digits + hyphens), so any character outside
    /// `[0-9A-Za-z_]` means the UUID came from somewhere we don't trust —
    /// throw instead of interpolating into SQL.
    private static func sanitizeVolumeForTable(_ uuid: String) throws -> String {
        let sanitized = uuid.replacingOccurrences(of: "-", with: "_")
        guard !sanitized.isEmpty,
              sanitized.allSatisfy({ $0.isASCII && ($0.isLetter || $0.isNumber || $0 == "_") }) else {
            throw IndexError.invalidVolumeUUID(uuid)
        }
        return sanitized
    }

    private static func stagingTableName(for volumeUUID: String) throws -> String {
        "files_staging_\(try sanitizeVolumeForTable(volumeUUID))"
    }

    // MARK: - Scan Lifecycle

    /// Begin a scan of `volumeUUID`. Acquires the scan slot and (re)creates
    /// an empty per-volume staging table. Concurrent scans throw
    /// `scanAlreadyInProgress`.
    func beginScan(volumeUUID: String) throws {
        try writer.sync { conn in
            if let existing = currentScanVolume {
                throw IndexError.scanAlreadyInProgress(volumeUUID: existing)
            }

            let staging = try DuckDBStore.stagingTableName(for: volumeUUID)
            try conn.execute("DROP TABLE IF EXISTS \"\(staging)\"")
            try conn.execute("""
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
        try writer.sync { conn in
            if let scanning = currentScanVolume, scanning == volumeUUID {
                try ingestToStagingOnWriter(conn: conn, files: files, volumeUUID: volumeUUID)
            } else {
                try ingestDirectOnWriter(conn: conn, files: files, volumeUUID: volumeUUID)
            }
        }
    }

    private func ingestToStagingOnWriter(conn: Connection, files: [ScannedFile], volumeUUID: String) throws {
        let staging = try DuckDBStore.stagingTableName(for: volumeUUID)
        let appender = try Appender(connection: conn, table: staging)

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
    private func ingestDirectOnWriter(conn: Connection, files: [ScannedFile], volumeUUID: String) throws {
        let appender = try Appender(connection: conn, table: "files")
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
        try writer.sync { conn in
            guard let scanning = currentScanVolume, scanning == volumeUUID else {
                throw IndexError.noActiveScan
            }

            let staging = try DuckDBStore.stagingTableName(for: volumeUUID)

            var added: [ScanDiff.Entry] = []
            var modified: [ScanDiff.Entry] = []
            var removedIds: [Int64] = []

            try conn.execute("BEGIN TRANSACTION")
            do {
                let quotedVolume = "'\(volumeUUID.replacingOccurrences(of: "'", with: "''"))'"

                // Joins use `id` (= PathHash.id(volume, path)) rather than
                // (volume_uuid, path). The PK gives us a hash lookup instead
                // of a two-column scan, and the semantics are identical by
                // construction: same id ⇔ same (volume, path).

                // --- ADDED: staging rows whose id isn't in files. Select
                // the full row so callers can populate the cache without a
                // second IN (...) round-trip. Staging tables hold the same
                // columns as `files` minus is_online (which is always TRUE
                // for newly-added rows).
                let addedResult = try conn.query("""
                    SELECT s.id, s.filename, s.path, s.volume_uuid, s.extension,
                           s.size_bytes, s.date_modified, s.date_created
                    FROM "\(staging)" s
                    LEFT JOIN files f ON f.id = s.id
                    WHERE f.id IS NULL
                """)
                added = extractEntries(from: addedResult)

                // --- MODIFIED: matching id, differing (size, mtime). Return
                // the staging values (the new state), same columns as added.
                let modifiedResult = try conn.query("""
                    SELECT s.id, s.filename, s.path, s.volume_uuid, s.extension,
                           s.size_bytes, s.date_modified, s.date_created
                    FROM "\(staging)" s
                    JOIN files f ON f.id = s.id
                    WHERE f.size_bytes <> s.size_bytes OR f.date_modified <> s.date_modified
                """)
                modified = extractEntries(from: modifiedResult)

                // --- REMOVED: files rows on this volume whose id isn't in staging.
                let removedResult = try conn.query("""
                    SELECT f.id FROM files f
                    LEFT JOIN "\(staging)" s ON s.id = f.id
                    WHERE f.volume_uuid = \(quotedVolume)
                      AND s.id IS NULL
                """)
                removedIds = extractIds(from: removedResult)

                // --- Apply to DuckDB. Order matters: INSERT before DELETE.
                if !added.isEmpty {
                    try conn.execute("""
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
                    try conn.execute("""
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
                        try conn.execute("""
                            DELETE FROM files WHERE id IN (\(list))
                        """)
                    }
                }

                try conn.execute("DROP TABLE IF EXISTS \"\(staging)\"")
                try conn.execute("COMMIT")
            } catch {
                try? conn.execute("ROLLBACK")
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
        let pathCol = result[2].cast(to: String.self)
        let volCol = result[3].cast(to: String.self)
        let extCol = result[4].cast(to: String.self)
        let sizeCol = result[5].cast(to: Int64.self)
        let modCol = result[6].cast(to: Int64.self)
        let crtCol = result[7].cast(to: Int64.self)
        var out: [ScanDiff.Entry] = []
        out.reserveCapacity(Int(result.rowCount))
        for i in 0..<result.rowCount {
            guard let id = idCol[i], let name = nameCol[i], let path = pathCol[i],
                  let vol = volCol[i], let ext = extCol[i], let size = sizeCol[i],
                  let mod = modCol[i], let crt = crtCol[i] else { continue }
            out.append(ScanDiff.Entry(
                id: id, filename: name, path: path, volumeUUID: vol, ext: ext,
                sizeBytes: size, dateModified: mod, dateCreated: crt
            ))
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
        try readers.sync { conn in
            let result = try conn.query("SELECT COUNT(*) FROM files")
            let col = result[0].cast(to: Int64.self)
            return Int(col[0] ?? 0)
        }
    }

    func getFileCountByVolume(_ volumeUUID: String) throws -> Int {
        try readers.sync { conn in
            let stmt = try PreparedStatement(connection: conn, query: "SELECT COUNT(*) FROM files WHERE volume_uuid = $1")
            try stmt.bind(volumeUUID, at: 1)
            let result = try stmt.execute()
            let col = result[0].cast(to: Int64.self)
            return Int(col[0] ?? 0)
        }
    }

    func loadCache() throws {
        let start = CFAbsoluteTimeGetCurrent()
        let results: [SearchResult] = try readers.sync { conn in
            let result = try conn.query("""
                SELECT id, filename, path, volume_uuid, extension,
                       size_bytes, date_modified, date_created, is_online
                FROM files
            """)
            return extractSearchResults(from: result)
        }
        withCacheLock {
            cache.removeAll()
            for r in results {
                cache[r.id] = r
            }
            cacheLoaded = true
            Log.debug("Cache loaded: \(cache.count) entries in \(String(format: "%.2f", CFAbsoluteTimeGetCurrent() - start))s")
        }
    }

    func invalidateCache() {
        withCacheLock {
            cache.removeAll()
            cacheLoaded = false
        }
    }

    /// Apply an already-computed scan diff to the in-memory cache. Builds
    /// SearchResult values directly from the diff's payload — no DuckDB
    /// round-trip. `mergeAndDiff` populates ScanDiff.Entry with full row
    /// data precisely so this path stays in-memory.
    ///
    /// Correctness: the entry values are a snapshot from staging at merge
    /// time, so they reflect exactly what was committed to `files`. A
    /// subsequent scan overwriting the same ids will produce its own diff
    /// and call applyDiff again, so there's no "stale write" risk.
    ///
    /// `is_online` is not in the diff because newly-added rows are always
    /// online (is_online=TRUE in the INSERT) and modified rows keep their
    /// previous is_online unchanged (offline handling is a future feature).
    func applyDiff(_ diff: ScanDiff) throws {
        let isLoaded = withCacheLock { cacheLoaded }
        guard isLoaded else { return }

        var built: [SearchResult] = []
        built.reserveCapacity(diff.added.count + diff.modified.count)
        for entry in diff.added {
            built.append(searchResult(from: entry, isOnline: true))
        }
        for entry in diff.modified {
            // Preserve the previous is_online value from the existing cache
            // entry if present. If the entry isn't cached (race with
            // invalidateCache), default to TRUE — the next loadCache will
            // reconcile.
            let preserved = withCacheLock { cache[entry.id]?.isOnline } ?? true
            built.append(searchResult(from: entry, isOnline: preserved))
        }

        // Apply removals and upserts in a single critical section so a
        // concurrent search can't observe a state that's missing the old
        // rows but doesn't yet have the new ones.
        withCacheLock {
            for id in diff.removedIds { cache.removeValue(forKey: id) }
            for r in built { cache[r.id] = r }
        }
    }

    private func searchResult(from entry: ScanDiff.Entry, isOnline: Bool) -> SearchResult {
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

    func getAllCachedValues() -> [SearchResult] {
        withCacheLock { Array(cache.values) }
    }

    func getFilesByIDs(_ ids: [Int64]) throws -> [SearchResult] {
        guard !ids.isEmpty else { return [] }

        let hot: [SearchResult]? = withCacheLock {
            cacheLoaded ? ids.compactMap { cache[$0] } : nil
        }
        if let hot { return hot }

        return try readers.sync { conn in
            try conn.execute("CREATE OR REPLACE TEMP TABLE _lookup (id BIGINT, ord BIGINT)")
            // Ensure the TEMP table is dropped even if appender/query throws.
            // Otherwise it sticks around on this reader's connection (TEMP
            // tables are connection-local) and leaks until the connection is
            // closed.
            defer { try? conn.execute("DROP TABLE IF EXISTS _lookup") }

            let appender = try Appender(connection: conn, table: "_lookup")
            for (idx, id) in ids.enumerated() {
                try appender.append(id)
                try appender.append(Int64(idx))
                try appender.endRow()
            }
            try appender.flush()

            let result = try conn.query("""
                SELECT f.id, f.filename, f.path, f.volume_uuid, f.extension,
                       f.size_bytes, f.date_modified, f.date_created, f.is_online
                FROM files f JOIN _lookup l ON f.id = l.id
                ORDER BY l.ord
            """)
            return extractSearchResults(from: result)
        }
    }

    func getAllFiles(limit: Int = 1000) throws -> [SearchResult] {
        let hot: [SearchResult]? = withCacheLock {
            cacheLoaded ? Array(cache.values.sorted { $0.dateModified > $1.dateModified }.prefix(limit)) : nil
        }
        if let hot { return hot }

        return try readers.sync { conn in
            let stmt = try PreparedStatement(connection: conn, query: """
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
        // Snapshot all rows on a reader connection (MVCC; writer not blocked).
        // Handler runs outside the reader so SQLite writes don't block DuckDB.
        var allRecords: [SyncRecord] = []
        try readers.sync { conn in
            let result = try conn.query("SELECT id, filename, extension FROM files")
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
        withCacheLock { cache[id] }
    }

    // Writer-path escape hatches for benchmarks only. Each call fully
    // serializes through `writer.sync`. Do NOT use for production code paths
    // — the typed API above (beginScan, ingestBatch, mergeAndDiff, …) is
    // what's been validated for correctness.
    func writer_query(_ sql: String) throws -> ResultSet {
        try writer.sync { conn in try conn.query(sql) }
    }

    func writer_execute(_ sql: String) throws {
        try writer.sync { conn in try conn.execute(sql) }
    }

    /// Run `body` with an Appender bound to the writer connection. The
    /// Appender and its flush happen entirely inside the writer lock, so
    /// nothing else can write to this connection in parallel. The Appender
    /// must not escape the closure.
    func withWriterAppender<T>(table: String, _ body: (Appender) throws -> T) throws -> T {
        try writer.sync { conn in
            let appender = try Appender(connection: conn, table: table)
            return try body(appender)
        }
    }

    /// Reader-path escape hatch for benchmarks that need raw SELECT access
    /// on a reader connection (e.g. to time cache-miss paths directly).
    func reader_query(_ sql: String) throws -> ResultSet {
        try readers.sync { conn in try conn.query(sql) }
    }

    func extractResults(from result: ResultSet) -> [SearchResult] {
        extractSearchResults(from: result)
    }

    // MARK: - DuckDB-native Search

    func searchByFilename(query: String, limit: Int = 1000) throws -> [SearchResult] {
        try readers.sync { conn in
            let stmt = try PreparedStatement(connection: conn, query: """
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
        // Drop cache entries *before* the DuckDB DELETE commits. A concurrent
        // search hitting the cache between the two can only miss (→ falls
        // through to reader query, which sees the row until DELETE commits,
        // and then won't see it after). Cached hit returning a row that no
        // longer exists is the stale-data failure mode we avoid.
        withCacheLock {
            cache = cache.filter { $0.value.volumeUUID != volumeUUID }
        }
        try writer.sync { conn in
            #if DEBUG
            let resultBefore = try conn.query("SELECT COUNT(*) FROM files")
            let countBefore = Int(resultBefore[0].cast(to: Int64.self)[0] ?? 0)
            #endif
            let stmt = try PreparedStatement(connection: conn, query: "DELETE FROM files WHERE volume_uuid = $1")
            try stmt.bind(volumeUUID, at: 1)
            _ = try stmt.execute()
            #if DEBUG
            let resultAfter = try conn.query("SELECT COUNT(*) FROM files")
            let countAfter = Int(resultAfter[0].cast(to: Int64.self)[0] ?? 0)
            Log.debug("deleteFilesByVolume '\(volumeUUID)': \(countBefore) → \(countAfter) (\(countBefore - countAfter) deleted)")
            #endif
        }
    }

    func clearAll() throws {
        withCacheLock {
            cache.removeAll()
            cacheLoaded = false
        }
        try writer.sync { conn in
            try conn.execute("DELETE FROM files")
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
