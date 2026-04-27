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

// Writes flow through a single writer connection; reads flow through a
// pool of reader connections. In-memory SearchResult cache lives in
// `cache` (DuckDBCache); volume_watch_state CRUD in `watchState`
// (VolumeWatchStateStore). This type owns the `files` table and the
// scan lifecycle.
//
// `currentScanVolumes` and `pendingBatches` are accessed only inside
// `writer.sync { }`, so they're serialized by the writer NSLock.
nonisolated final class DuckDBStore: @unchecked Sendable {
    static let defaultReaderCount = 3

    private let database: Database
    let writer: WriterConnection
    let readers: ReaderPool
    let dbPath: String
    let cache: DuckDBCache
    let watchState: VolumeWatchStateStore

    // Per-volume scan slots. beginScan adds; mergeAndDiff removes. Two
    // different volumes can scan concurrently at the staging-INSERT level
    // (each has its own staging table), but `mergeAndDiff` and
    // `applyDirectDiff` still serialize on the writer NSLock.
    private var currentScanVolumes: Set<String> = []

    // FSEvents batches for volumes mid-scan. Applied after mergeAndDiff
    // releases the slot, so direct-diff doesn't race with the merge SQL.
    private var pendingBatches: [String: [ScanDiff]] = [:]

    init(path: String? = nil, readerCount: Int = DuckDBStore.defaultReaderCount) throws {
        let appSupport = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
        let prismDir = appSupport.appendingPathComponent("Prism", isDirectory: true)
        try? FileManager.default.createDirectory(at: prismDir, withIntermediateDirectories: true)

        let resolvedPath = path ?? prismDir.appendingPathComponent("metadata.duckdb").path
        dbPath = resolvedPath

        database = try Database(store: .file(at: URL(fileURLWithPath: resolvedPath)))
        writer = try WriterConnection(database: database)
        readers = try ReaderPool(database: database, count: readerCount)
        cache = DuckDBCache()
        watchState = VolumeWatchStateStore(writer: writer, readers: readers)
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
            // id = PathHash(volume_uuid, path) is the PK, so (volume_uuid,
            // path) uniqueness is already implied. An explicit unique index
            // would cost per-row maintenance; merge joins use id anyway.

            try conn.execute("""
                CREATE TABLE IF NOT EXISTS volume_watch_state (
                    volume_uuid VARCHAR PRIMARY KEY,
                    last_event_id BIGINT NOT NULL,
                    last_seen_at BIGINT NOT NULL,
                    polling_mode BOOLEAN NOT NULL DEFAULT FALSE,
                    last_reason VARCHAR
                )
            """)

            // last_scanned_at is the user-facing "Last scanned X ago"
            // timestamp shown in Settings. Set once at the end of a
            // successful mergeAndDiff. Not folded into volume_watch_state
            // because that updates on every FSEvents checkpoint and
            // wouldn't reflect "last full scan" semantics.
            try conn.execute("""
                CREATE TABLE IF NOT EXISTS volume_scan_state (
                    volume_uuid VARCHAR PRIMARY KEY,
                    last_scanned_at BIGINT NOT NULL
                )
            """)
        }
    }

    /// Drop staging tables left behind by a crashed scan. Runs at init.
    /// `internal` so tests can drive it.
    internal func cleanupOrphanedStaging() throws {
        try writer.sync { conn in
            let result = try conn.query("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'main' AND table_name LIKE 'files_staging_%'
            """)
            let col = result[0].cast(to: String.self)
            for idx in 0..<result.rowCount {
                guard let name = col[idx] else { continue }
                try conn.execute("DROP TABLE IF EXISTS \"\(name)\"")
                Log.debug("Dropped orphaned staging table: \(name)")
            }
        }
    }

    private static func sanitizeVolumeForTable(_ uuid: String) -> String {
        uuid.replacingOccurrences(of: "-", with: "_")
    }

    private static func stagingTableName(for volumeUUID: String) -> String {
        "files_staging_\(sanitizeVolumeForTable(volumeUUID))"
    }

    // MARK: - Scan Lifecycle

    /// Same-volume double-begin throws; different-volume begin succeeds.
    func beginScan(volumeUUID: String) throws {
        try writer.sync { conn in
            if currentScanVolumes.contains(volumeUUID) {
                throw IndexError.scanAlreadyInProgress(volumeUUID: volumeUUID)
            }

            let staging = DuckDBStore.stagingTableName(for: volumeUUID)
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

            currentScanVolumes.insert(volumeUUID)
            Log.debug("beginScan volume=\(volumeUUID) (active volumes: \(currentScanVolumes.count))")
        }
    }

    func isScanning(volumeUUID: String) -> Bool {
        writer.sync { _ in currentScanVolumes.contains(volumeUUID) }
    }

    // MARK: - Ingestion

    /// During a scan, rows go to per-volume staging. Otherwise (tests,
    /// one-shots) they Appender-insert into `files` directly — caller must
    /// not re-insert the same (volume, path) since Appender doesn't support
    /// ON CONFLICT; use beginScan/mergeAndDiff for idempotent rescans.
    func ingestBatch(_ files: [ScannedFile], volumeUUID: String) throws {
        try writer.sync { conn in
            if currentScanVolumes.contains(volumeUUID) {
                try ingestToStagingOnWriter(conn: conn, files: files, volumeUUID: volumeUUID)
            } else {
                try ingestDirectOnWriter(conn: conn, files: files, volumeUUID: volumeUUID)
            }
        }
    }

    private func ingestToStagingOnWriter(conn: Connection, files: [ScannedFile], volumeUUID: String) throws {
        let staging = DuckDBStore.stagingTableName(for: volumeUUID)
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

    /// Diff against `files`, apply, drop the staging table, release the
    /// scan slot. SQL shape (three queries, one transaction):
    ///   added    = staging rows whose id isn't in files
    ///   modified = matching id but differing (size_bytes, date_modified)
    ///   removed  = files rows for this volume whose id isn't in staging
    func mergeAndDiff(volumeUUID: String) throws -> ScanDiff {
        try writer.sync { conn in
            guard currentScanVolumes.contains(volumeUUID) else {
                throw IndexError.noActiveScan
            }

            let staging = DuckDBStore.stagingTableName(for: volumeUUID)

            var added: [ScanDiff.Entry] = []
            var modified: [ScanDiff.Entry] = []
            var removedIds: [Int64] = []

            try conn.execute("BEGIN TRANSACTION")
            do {
                let quotedVolume = "'\(volumeUUID.replacingOccurrences(of: "'", with: "''"))'"

                // Selecting the full staging row keeps applyDiff in-memory —
                // no IN(...) round-trip to DuckDB to rehydrate the cache.
                let addedResult = try conn.query("""
                    SELECT s.id, s.filename, s.path, s.volume_uuid, s.extension,
                           s.size_bytes, s.date_modified, s.date_created
                    FROM "\(staging)" s
                    LEFT JOIN files f ON f.id = s.id
                    WHERE f.id IS NULL
                """)
                added = extractEntries(from: addedResult)

                let modifiedResult = try conn.query("""
                    SELECT s.id, s.filename, s.path, s.volume_uuid, s.extension,
                           s.size_bytes, s.date_modified, s.date_created
                    FROM "\(staging)" s
                    JOIN files f ON f.id = s.id
                    WHERE f.size_bytes <> s.size_bytes OR f.date_modified <> s.date_modified
                """)
                modified = extractEntries(from: modifiedResult)

                let removedResult = try conn.query("""
                    SELECT f.id FROM files f
                    LEFT JOIN "\(staging)" s ON s.id = f.id
                    WHERE f.volume_uuid = \(quotedVolume)
                      AND s.id IS NULL
                """)
                removedIds = extractIds(from: removedResult)

                // INSERT before DELETE matters — deleting then inserting
                // the same id in one transaction trips DuckDB's unique index.
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
                    // UPDATE excludes is_online so offline flag survives rescan.
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
                    for chunk in removedIds.chunked(into: 1000) {
                        let list = chunk.map(String.init).joined(separator: ",")
                        try conn.execute("DELETE FROM files WHERE id IN (\(list))")
                    }
                }

                // Stamp last_scanned_at inside the same transaction as
                // the file mutations — if mergeAndDiff commits, the
                // timestamp updates atomically with the data.
                let now = Int64(Date().timeIntervalSince1970)
                let ts = try PreparedStatement(connection: conn, query: """
                    INSERT OR REPLACE INTO volume_scan_state (volume_uuid, last_scanned_at)
                    VALUES ($1, $2)
                """)
                try ts.bind(volumeUUID, at: 1)
                try ts.bind(now, at: 2)
                _ = try ts.execute()

                try conn.execute("DROP TABLE IF EXISTS \"\(staging)\"")
                try conn.execute("COMMIT")
            } catch {
                try? conn.execute("ROLLBACK")
                // Leave staging table behind; next init's cleanupOrphanedStaging sweeps it.
                currentScanVolumes.remove(volumeUUID)
                throw error
            }

            currentScanVolumes.remove(volumeUUID)

            let diff = ScanDiff(added: added, modified: modified, removedIds: removedIds)
            Log.debug("mergeAndDiff volume=\(volumeUUID) added=\(diff.added.count) modified=\(diff.modified.count) removed=\(diff.removedIds.count)")
            return diff
        }
    }

    /// Last full scan timestamp for a volume (unix seconds), or nil if never scanned.
    func lastScannedAt(volumeUUID: String) throws -> Foundation.Date? {
        try readers.sync { conn in
            let stmt = try PreparedStatement(connection: conn, query: """
                SELECT last_scanned_at FROM volume_scan_state WHERE volume_uuid = $1
            """)
            try stmt.bind(volumeUUID, at: 1)
            let result = try stmt.execute()
            guard result.rowCount > 0 else { return nil }
            let col = result[0].cast(to: Int64.self)
            guard let raw = col[0] else { return nil }
            return Foundation.Date(timeIntervalSince1970: TimeInterval(raw))
        }
    }

    /// Drained by LiveIndexCoordinator after mergeAndDiff completes.
    /// Returns the queued batches and clears the queue atomically.
    func drainPendingBatches(volumeUUID: String) -> [ScanDiff] {
        writer.sync { _ in
            let batches = pendingBatches[volumeUUID] ?? []
            pendingBatches.removeValue(forKey: volumeUUID)
            return batches
        }
    }

    /// Enqueue an FSEvents-derived diff to apply after the current scan
    /// completes. Called by LiveIndexCoordinator when `isScanning(volumeUUID)`
    func enqueuePendingBatch(volumeUUID: String, diff: ScanDiff) {
        writer.sync { _ in
            pendingBatches[volumeUUID, default: []].append(diff)
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
        try cache.load { [readers] in
            try readers.sync { conn in
                let result = try conn.query("""
                    SELECT id, filename, path, volume_uuid, extension,
                           size_bytes, date_modified, date_created, is_online
                    FROM files
                """)
                return Self.extractSearchResults(from: result)
            }
        }
    }

    func invalidateCache() { cache.invalidate() }

    func applyDiff(_ diff: ScanDiff) throws {
        cache.applyDiff(diff)
    }

    func getAllCachedValues() -> [SearchResult] {
        cache.allValues()
    }

    func getFilesByIDs(_ ids: [Int64]) throws -> [SearchResult] {
        guard !ids.isEmpty else { return [] }

        if let hot = cache.results(for: ids) { return hot }

        return try readers.sync { conn in
            try conn.execute("CREATE OR REPLACE TEMP TABLE _lookup (id BIGINT, ord BIGINT)")
            // TEMP tables are connection-local; drop on all paths or they
            // leak until the connection closes.
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
            return Self.extractSearchResults(from: result)
        }
    }

    func getAllFiles(limit: Int = 1000) throws -> [SearchResult] {
        if let hot = cache.allSortedByDateDesc(limit: limit) { return hot }

        return try readers.sync { conn in
            let stmt = try PreparedStatement(connection: conn, query: """
                SELECT id, filename, path, volume_uuid, extension,
                       size_bytes, date_modified, date_created, is_online
                FROM files ORDER BY date_modified DESC LIMIT $1
            """)
            try stmt.bind(Int64(limit), at: 1)
            let result = try stmt.execute()
            return Self.extractSearchResults(from: result)
        }
    }

    static func extractSearchResults(from result: ResultSet) -> [SearchResult] {
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

    /// Handler runs OUTSIDE the reader closure so SQLite writes in the
    /// handler don't hold a DuckDB reader connection.
    func iterateAllForSync(batchSize: Int = 10_000, handler: ([SyncRecord]) throws -> Void) throws {
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

    // MARK: - Benchmark escape hatches
    //
    // These bypass the typed scan/query API (beginScan/ingestBatch/mergeAndDiff).
    // Benchmarks only — do not use from production code paths.

    func cachedResult(for id: Int64) -> SearchResult? {
        cache.result(for: id)
    }

    func writer_query(_ sql: String) throws -> ResultSet {
        try writer.sync { conn in try conn.query(sql) }
    }

    func writer_execute(_ sql: String) throws {
        try writer.sync { conn in try conn.execute(sql) }
    }

    /// Appender must not escape the closure.
    func withWriterAppender<T>(table: String, _ body: (Appender) throws -> T) throws -> T {
        try writer.sync { conn in
            let appender = try Appender(connection: conn, table: table)
            return try body(appender)
        }
    }

    func reader_query(_ sql: String) throws -> ResultSet {
        try readers.sync { conn in try conn.query(sql) }
    }

    func extractResults(from result: ResultSet) -> [SearchResult] {
        Self.extractSearchResults(from: result)
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
            return Self.extractSearchResults(from: result)
        }
    }

    // MARK: - Direct Diff (Phase 3 — FSEvents hot path)

    /// Apply a diff without going through the staging-table merge. Writer
    /// NSLock is released between 1k-row chunks so concurrent writers
    /// (full scans, Clear Index) don't starve on a large FSEvents burst.
    ///
    /// is_online is not updated: always TRUE on INSERT; UPDATE omits it so
    /// the offline flag survives rescans.
    func applyDirectDiff(_ diff: ScanDiff, volumeUUID: String) throws {
        guard !diff.isEmpty else { return }

        let chunkSize = 1000

        // Must be idempotent — FSEvents replay sends the same events after
        // app restart via HistoryDone. Appender doesn't support ON CONFLICT;
        // DELETE+Appender in one transaction trips DuckDB's unique-constraint
        // index (the index isn't invalidated until commit). Per-row prepared
        // INSERT is slower than Appender but correct.
        let addedChunks = diff.added.chunked(into: chunkSize)
        for chunk in addedChunks {
            try writer.sync { conn in
                try conn.execute("BEGIN TRANSACTION")
                do {
                    let stmt = try PreparedStatement(connection: conn, query: """
                        INSERT INTO files
                            (id, filename, path, volume_uuid, extension,
                             size_bytes, date_modified, date_created, is_online)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, TRUE)
                        ON CONFLICT (id) DO UPDATE SET
                            filename = EXCLUDED.filename,
                            path = EXCLUDED.path,
                            extension = EXCLUDED.extension,
                            size_bytes = EXCLUDED.size_bytes,
                            date_modified = EXCLUDED.date_modified,
                            date_created = EXCLUDED.date_created
                            -- volume_uuid omitted: DuckDB forbids UPDATE SET
                            -- on indexed columns (idx_files_volume).
                    """)
                    for entry in chunk {
                        try stmt.bind(entry.id, at: 1)
                        try stmt.bind(entry.filename, at: 2)
                        try stmt.bind(entry.path, at: 3)
                        try stmt.bind(entry.volumeUUID, at: 4)
                        try stmt.bind(entry.ext, at: 5)
                        try stmt.bind(entry.sizeBytes, at: 6)
                        try stmt.bind(entry.dateModified, at: 7)
                        try stmt.bind(entry.dateCreated, at: 8)
                        _ = try stmt.execute()
                    }
                    try conn.execute("COMMIT")
                } catch {
                    try? conn.execute("ROLLBACK")
                    throw error
                }
            }
        }

        let modifiedChunks = diff.modified.chunked(into: chunkSize)
        for chunk in modifiedChunks {
            try writer.sync { conn in
                try conn.execute("BEGIN TRANSACTION")
                do {
                    // ~200μs/update × 1000 ≈ 200ms per chunk. Within budget.
                    let stmt = try PreparedStatement(connection: conn, query: """
                        UPDATE files
                        SET filename = $1,
                            extension = $2,
                            size_bytes = $3,
                            date_modified = $4,
                            date_created = $5
                        WHERE id = $6
                    """)
                    for entry in chunk {
                        try stmt.bind(entry.filename, at: 1)
                        try stmt.bind(entry.ext, at: 2)
                        try stmt.bind(entry.sizeBytes, at: 3)
                        try stmt.bind(entry.dateModified, at: 4)
                        try stmt.bind(entry.dateCreated, at: 5)
                        try stmt.bind(entry.id, at: 6)
                        _ = try stmt.execute()
                    }
                    try conn.execute("COMMIT")
                } catch {
                    try? conn.execute("ROLLBACK")
                    throw error
                }
            }
        }

        let removedChunks = diff.removedIds.chunked(into: chunkSize)
        for chunk in removedChunks {
            try writer.sync { conn in
                try conn.execute("BEGIN TRANSACTION")
                do {
                    let list = chunk.map(String.init).joined(separator: ",")
                    try conn.execute("DELETE FROM files WHERE id IN (\(list))")
                    try conn.execute("COMMIT")
                } catch {
                    try? conn.execute("ROLLBACK")
                    throw error
                }
            }
        }

        Log.debug("applyDirectDiff volume=\(volumeUUID) +\(diff.added.count) ~\(diff.modified.count) -\(diff.removedIds.count)")
    }

    /// ~50-150ms on a 1M-row volume; one-shot per mount/unmount.
    func setVolumeOnline(_ volumeUUID: String, isOnline: Bool) throws {
        try writer.sync { conn in
            let stmt = try PreparedStatement(connection: conn, query: """
                UPDATE files SET is_online = $1 WHERE volume_uuid = $2
            """)
            try stmt.bind(isOnline, at: 1)
            try stmt.bind(volumeUUID, at: 2)
            _ = try stmt.execute()
        }
        cache.setVolumeOnline(volumeUUID, isOnline: isOnline)
    }

    // MARK: - Volume watch-state facades

    func persistEventId(volumeUUID: String, lastEventId: UInt64, reason: String? = nil) throws {
        try watchState.persistEventId(volumeUUID: volumeUUID, lastEventId: lastEventId, reason: reason)
    }

    func loadWatchState(volumeUUID: String) throws -> (lastEventId: UInt64, pollingMode: Bool)? {
        try watchState.load(volumeUUID: volumeUUID)
    }

    func setPollingMode(volumeUUID: String, enabled: Bool) throws {
        try watchState.setPollingMode(volumeUUID: volumeUUID, enabled: enabled)
    }

    func maxDateModified(volumeUUID: String) throws -> Int64? {
        try watchState.maxDateModified(volumeUUID: volumeUUID)
    }

    // MARK: - Volume Operations

    func deleteFilesByVolume(_ volumeUUID: String) throws {
        // Drop cache BEFORE the DuckDB DELETE. Concurrent search between
        // the two can only miss (→ falls through to reader, which still
        // sees the row until the DELETE commits). Stale cache hit for a
        // deleted row is the failure mode we avoid.
        cache.dropVolume(volumeUUID)
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
        cache.invalidate()
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
