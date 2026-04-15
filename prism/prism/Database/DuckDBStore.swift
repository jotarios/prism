//
//  DuckDBStore.swift
//  prism
//

import Foundation
import DuckDB

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
    private var nextID: Int64 = 1

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

        let result = try connection.query("SELECT COALESCE(MAX(id), 0) FROM files")
        let maxID = result[0].cast(to: Int64.self)[0] ?? 0
        nextID = maxID + 1
        }
    }

    // MARK: - Ingestion

    func ingestBatch(_ files: [ScannedFile], volumeUUID: String) throws {
        try sync {
        let appender = try Appender(connection: connection, table: "files")

        for file in files {
            do {
                try appender.append(nextID)
                nextID += 1
                try appender.append(file.filename)
                try appender.append(file.parentPath + "/" + file.filename)
                try appender.append(volumeUUID)
                try appender.append(file.ext)
                try appender.append(Int64(file.sizeBytes))
                try appender.append(Int64(file.modTimeSec))
                try appender.append(Int64(file.createTimeSec))
                try appender.append(true)
                try appender.endRow()
            } catch {
                Log.error("DuckDB ingest error for \(file.filename): \(error)")
                throw error
            }
        }

        try appender.flush()
        }
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
