//
//  DuckDBStore.swift
//  prism
//

import Foundation
import DuckDB

// Not thread-safe. All mutation (ingestBatch, deleteFilesByVolume, clearAll)
// must be called from a single serial context. The ParallelScanCoordinator's
// consumer loop guarantees this during scan.
final class DuckDBStore {
    private let database: Database
    private let connection: Connection
    let dbPath: String

    private var cache: [Int64: SearchResult] = [:]
    private var cacheLoaded = false
    private var nextID: Int64 = 1

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

    // MARK: - Ingestion

    func ingestBatch(_ files: [ScannedFile], volumeUUID: String) throws {
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

    // MARK: - Queries

    func getFileCount() throws -> Int {
        let result = try connection.query("SELECT COUNT(*) FROM files")
        let col = result[0].cast(to: Int64.self)
        return Int(col[0] ?? 0)
    }

    func getFileCountByVolume(_ volumeUUID: String) throws -> Int {
        let stmt = try PreparedStatement(connection: connection, query: "SELECT COUNT(*) FROM files WHERE volume_uuid = $1")
        try stmt.bind(volumeUUID, at: 1)
        let result = try stmt.execute()
        let col = result[0].cast(to: Int64.self)
        return Int(col[0] ?? 0)
    }

    func loadCache() throws {
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

    func invalidateCache() {
        cache.removeAll()
        cacheLoaded = false
    }

    func getAllCachedValues() -> Dictionary<Int64, SearchResult>.Values {
        cache.values
    }

    func getAllFileIDs(limit: Int = 10_000) throws -> [Int64] {
        let stmt = try PreparedStatement(connection: connection, query: "SELECT id FROM files ORDER BY date_modified DESC LIMIT $1")
        try stmt.bind(Int64(limit), at: 1)
        let result = try stmt.execute()
        let col = result[0].cast(to: Int64.self)
        var ids: [Int64] = []
        for idx in 0..<result.rowCount {
            if let id = col[idx] { ids.append(id) }
        }
        return ids
    }

    func getFilesByIDs(_ ids: [Int64]) throws -> [SearchResult] {
        guard !ids.isEmpty else { return [] }
        if cacheLoaded {
            return ids.compactMap { cache[$0] }
        }

        try connection.execute("CREATE OR REPLACE TEMP TABLE _lookup (id BIGINT)")
        let appender = try Appender(connection: connection, table: "_lookup")
        for id in ids {
            try appender.append(id)
            try appender.endRow()
        }
        try appender.flush()

        let result = try connection.query("""
            SELECT f.id, f.filename, f.path, f.volume_uuid, f.extension,
                   f.size_bytes, f.date_modified, f.date_created, f.is_online
            FROM files f JOIN _lookup l ON f.id = l.id
        """)
        let results = extractSearchResults(from: result)
        try connection.execute("DROP TABLE IF EXISTS _lookup")
        return results
    }

    func getAllFiles(limit: Int = 1000) throws -> [SearchResult] {
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
        let result = try connection.query("SELECT id, filename, extension FROM files")
        let idCol = result[0].cast(to: Int64.self)
        let filenameCol = result[1].cast(to: String.self)
        let extCol = result[2].cast(to: String.self)

        var batch: [SyncRecord] = []
        batch.reserveCapacity(batchSize)
        let rowCount = result.rowCount

        for idx in 0..<rowCount {
            guard let id = idCol[idx],
                  let filename = filenameCol[idx],
                  let ext = extCol[idx] else { continue }

            batch.append(SyncRecord(id: id, filename: filename, ext: ext))

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
        cache[id]
    }

    func connection_query(_ sql: String) throws -> ResultSet {
        try connection.query(sql)
    }

    func connection_execute(_ sql: String) throws {
        try connection.execute(sql)
    }

    func createAppender(table: String) throws -> Appender {
        try Appender(connection: connection, table: table)
    }

    func extractResults(from result: ResultSet) -> [SearchResult] {
        extractSearchResults(from: result)
    }

    // MARK: - DuckDB-native Search

    func searchByFilename(query: String, limit: Int = 1000) throws -> [SearchResult] {
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

    // MARK: - Volume Operations

    func deleteFilesByVolume(_ volumeUUID: String) throws {
        #if DEBUG
        let countBefore = try getFileCount()
        #endif
        let stmt = try PreparedStatement(connection: connection, query: "DELETE FROM files WHERE volume_uuid = $1")
        try stmt.bind(volumeUUID, at: 1)
        _ = try stmt.execute()
        #if DEBUG
        let countAfter = try getFileCount()
        Log.debug("deleteFilesByVolume '\(volumeUUID)': \(countBefore) → \(countAfter) (\(countBefore - countAfter) deleted)")
        #endif
    }

    func clearAll() throws {
        try connection.execute("DELETE FROM files")
    }
}

struct SyncRecord: Sendable {
    let id: Int64
    let filename: String
    let ext: String
}
