//
//  DatabaseManager.swift
//  prism
//

import Foundation
import GRDB

enum DatabaseError: Error {
    case openFailed(String)
    case executionFailed(String)
    case corruptDatabase
    case prepareFailed(String)
    case syncMismatch(expected: Int, actual: Int)
}

private extension Array {
    func chunked(into size: Int) -> [[Element]] {
        stride(from: 0, to: count, by: size).map {
            Array(self[$0..<Swift.min($0 + size, count)])
        }
    }
}

final class DatabaseManager {
    static let shared = DatabaseManager()

    private var dbPool: DatabasePool!
    private let dbPath: String

    private init() {
        let appSupport = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
        let prismDir = appSupport.appendingPathComponent("Prism", isDirectory: true)
        try? FileManager.default.createDirectory(at: prismDir, withIntermediateDirectories: true)
        dbPath = prismDir.appendingPathComponent("index.db").path
    }

    // MARK: - Database Lifecycle

    func open() throws {
        var config = Configuration()

        config.prepareDatabase { db in
            try db.execute(sql: "PRAGMA journal_mode=WAL")
            try db.execute(sql: "PRAGMA synchronous=NORMAL")
            try db.execute(sql: "PRAGMA cache_size=-50000")
            try db.execute(sql: "PRAGMA temp_store=MEMORY")
            try db.execute(sql: "PRAGMA foreign_keys=ON")
            try db.execute(sql: "PRAGMA busy_timeout=100")
        }

        config.defaultTransactionKind = .deferred
        dbPool = try DatabasePool(path: dbPath, configuration: config)

        try checkIntegrity()
        try handleMigrations()
        try ensureTriggersExist()
    }

    func close() {
        dbPool = nil
    }

    // MARK: - Migrations

    private func handleMigrations() throws {
        var migrator = DatabaseMigrator()

        migrator.registerMigration("v2-search-index") { db in
            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS files (
                    id INTEGER PRIMARY KEY,
                    filename TEXT NOT NULL,
                    extension TEXT NOT NULL
                )
            """)

            try db.execute(sql: """
                CREATE VIRTUAL TABLE IF NOT EXISTS files_fts USING fts5(
                    filename,
                    extension,
                    content='files',
                    content_rowid='id',
                    prefix='2 3 4'
                )
            """)

            try DatabaseManager.createFTS5Triggers(db)

            try db.execute(sql: "CREATE INDEX IF NOT EXISTS idx_extension ON files(extension)")
        }

        try migrator.migrate(dbPool)
    }

    // MARK: - Integrity

    private func checkIntegrity() throws {
        try dbPool.read { db in
            let result = try String.fetchOne(db, sql: "PRAGMA integrity_check")
            if result != "ok" {
                throw DatabaseError.corruptDatabase
            }
        }
    }

    // MARK: - FTS5 Trigger Definitions

    /// Creates the three FTS5 sync triggers. Idempotent (`IF NOT EXISTS`).
    /// Callers that need to drop-then-recreate should `DROP TRIGGER IF EXISTS`
    /// first; this helper never drops.
    /// Static so it can be called from migration closures without capturing self.
    fileprivate static func createFTS5Triggers(_ db: Database) throws {
        try db.execute(sql: """
            CREATE TRIGGER IF NOT EXISTS files_ai AFTER INSERT ON files BEGIN
                INSERT INTO files_fts(rowid, filename, extension)
                VALUES (new.id, new.filename, new.extension);
            END
        """)
        try db.execute(sql: """
            CREATE TRIGGER IF NOT EXISTS files_ad AFTER DELETE ON files BEGIN
                INSERT INTO files_fts(files_fts, rowid, filename, extension)
                VALUES('delete', old.id, old.filename, old.extension);
            END
        """)
        try db.execute(sql: """
            CREATE TRIGGER IF NOT EXISTS files_au AFTER UPDATE ON files BEGIN
                INSERT INTO files_fts(files_fts, rowid, filename, extension)
                VALUES('delete', old.id, old.filename, old.extension);
                INSERT INTO files_fts(rowid, filename, extension)
                VALUES (new.id, new.filename, new.extension);
            END
        """)
    }

    private func ensureTriggersExist() throws {
        let hasTrigger = try dbPool.read { db in
            try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name='files_ai'") ?? 0
        }
        if hasTrigger == 0 {
            try endBulkImport()
        }
    }

    // MARK: - Bulk Import Mode

    func beginBulkImport() throws {
        try dbPool.write { db in
            try db.execute(sql: "DROP TRIGGER IF EXISTS files_ai")
            try db.execute(sql: "DROP TRIGGER IF EXISTS files_ad")
            try db.execute(sql: "DROP TRIGGER IF EXISTS files_au")
        }
    }

    func endBulkImport() throws {
        try dbPool.write { db in
            try DatabaseManager.createFTS5Triggers(db)
            try db.execute(sql: "INSERT INTO files_fts(files_fts) VALUES('rebuild')")
        }
    }

    // MARK: - Sync from DuckDB

    /// Full rebuild of the SQLite search index from DuckDB. Used by Clear
    /// Index / Rebuild Index and by first-time sync. Scan-time callers should
    /// prefer `syncSearchIndex(from:volumeUUID:diff:)` which is O(delta).
    func rebuildSearchIndex(from store: DuckDBStore) throws {
        // Snapshot all records from DuckDB first (outside the SQLite transaction
        // so we don't hold a write lock while reading from the other DB).
        var allRecords: [SyncRecord] = []
        try store.iterateAllForSync { batch in
            allRecords.append(contentsOf: batch)
        }

        // Perform the entire replacement — drop triggers, wipe files, bulk
        // insert, restore triggers, rebuild FTS5 — inside a single write
        // transaction. Any failure rolls back to the previous good state.
        try dbPool.write { db in
            try db.execute(sql: "DROP TRIGGER IF EXISTS files_ai")
            try db.execute(sql: "DROP TRIGGER IF EXISTS files_ad")
            try db.execute(sql: "DROP TRIGGER IF EXISTS files_au")

            try db.execute(sql: "DELETE FROM files")

            let stmt = try db.cachedStatement(sql: "INSERT OR REPLACE INTO files (id, filename, extension) VALUES (?, ?, ?)")
            for record in allRecords {
                try stmt.execute(arguments: [record.id, record.filename, record.ext])
            }

            try DatabaseManager.createFTS5Triggers(db)
            try db.execute(sql: "INSERT INTO files_fts(files_fts) VALUES('rebuild')")
        }

        let sqliteCount = try dbPool.read { db in
            try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM files") ?? 0
        }
        let duckdbCount = try store.getFileCount()
        if sqliteCount != duckdbCount {
            Log.error("Sync mismatch: SQLite=\(sqliteCount) DuckDB=\(duckdbCount)")
        }
    }

    /// Incremental sync: apply a pre-computed `ScanDiff` to SQLite + FTS5 in
    /// a single write transaction. Triggers stay in place; they propagate
    /// each per-row change to FTS5 cheaply. Cost is O(|added ∪ modified ∪
    /// removed|) rather than O(N).
    ///
    /// Callers: `SearchViewModel.scanVolume` after `DuckDBStore.mergeAndDiff`.
    func syncSearchIndex(from store: DuckDBStore, volumeUUID: String, diff: ScanDiff) throws {
        guard !diff.isEmpty else {
            Log.debug("syncSearchIndex: empty diff for volume=\(volumeUUID), skipping")
            return
        }

        try dbPool.write { db in
            // DELETEs. Chunked at 500 to stay comfortably below SQLite's
            // SQLITE_MAX_VARIABLE_NUMBER (default 32_766 in 3.32+, but older
            // builds cap at 999; we keep headroom).
            if !diff.removedIds.isEmpty {
                for chunk in diff.removedIds.chunked(into: 500) {
                    let placeholders = Array(repeating: "?", count: chunk.count).joined(separator: ",")
                    try db.execute(
                        sql: "DELETE FROM files WHERE id IN (\(placeholders))",
                        arguments: StatementArguments(chunk)
                    )
                }
            }

            // INSERT OR REPLACE for added ∪ modified. The files_ai and
            // files_au triggers handle FTS5 propagation row-by-row.
            let mutationCount = diff.added.count + diff.modified.count + diff.removedIds.count
            if !diff.added.isEmpty || !diff.modified.isEmpty {
                let stmt = try db.cachedStatement(
                    sql: "INSERT OR REPLACE INTO files (id, filename, extension) VALUES (?, ?, ?)"
                )
                for entry in diff.added {
                    try stmt.execute(arguments: [entry.id, entry.filename, entry.ext])
                }
                for entry in diff.modified {
                    try stmt.execute(arguments: [entry.id, entry.filename, entry.ext])
                }
            }

            // After bulk mutations, FTS5 is left with many small segments
            // that drag down MATCH + ORDER BY rank for common prefixes.
            // `rebuild` reconstructs the whole index from `files` in a
            // single optimally-packed segment — same structure main's
            // bulk-rebuild sync path produces. Only worth the cost when we
            // just mutated a lot of rows.
            if mutationCount >= 1000 {
                try db.execute(sql: "INSERT INTO files_fts(files_fts) VALUES('rebuild')")
            }
        }

        // Verification: SQLite rows for this volume should equal DuckDB rows.
        let sqliteCount = try dbPool.read { db in
            try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM files") ?? 0
        }
        let duckdbCount = try store.getFileCount()
        if sqliteCount != duckdbCount {
            Log.error("Incremental sync mismatch: SQLite=\(sqliteCount) DuckDB=\(duckdbCount) (volume=\(volumeUUID))")
        }
    }

    // MARK: - Search

    func searchFileIDs(query: String, limit: Int = 1000) async throws -> [Int64] {
        guard !query.isEmpty else { return [] }

        return try await dbPool.read { db in
            let sanitized = query
                .trimmingCharacters(in: .whitespacesAndNewlines)
                .replacingOccurrences(of: "\"", with: "\"\"")

            let terms = sanitized.components(separatedBy: .whitespaces)
                .filter { !$0.isEmpty }
                .map { "\"\($0)\"*" }
                .joined(separator: " ")

            return try Int64.fetchAll(db, sql: """
                SELECT f.id
                FROM files_fts
                JOIN files f ON files_fts.rowid = f.id
                WHERE files_fts MATCH ?
                ORDER BY rank
                LIMIT ?
            """, arguments: [terms, limit])
        }
    }


    // MARK: - Query Operations

    func getFileCount() async throws -> Int {
        try await dbPool.read { db in
            try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM files") ?? 0
        }
    }

    func getPragmaSettings() throws -> [String: String] {
        try dbPool.read { db in
            var settings: [String: String] = [:]
            settings["journal_mode"] = try String.fetchOne(db, sql: "PRAGMA journal_mode") ?? ""
            settings["synchronous"] = try String.fetchOne(db, sql: "PRAGMA synchronous") ?? ""
            settings["cache_size"] = try String.fetchOne(db, sql: "PRAGMA cache_size") ?? ""
            settings["temp_store"] = try String.fetchOne(db, sql: "PRAGMA temp_store") ?? ""
            settings["foreign_keys"] = try String.fetchOne(db, sql: "PRAGMA foreign_keys") ?? ""
            return settings
        }
    }

    // MARK: - Database Rebuild

    func rebuildDatabase() throws {
        try dbPool.write { db in
            try db.execute(sql: "DROP TRIGGER IF EXISTS files_au")
            try db.execute(sql: "DROP TRIGGER IF EXISTS files_ad")
            try db.execute(sql: "DROP TRIGGER IF EXISTS files_ai")
            try db.execute(sql: "DROP TABLE IF EXISTS files_fts")
            try db.execute(sql: "DROP TABLE IF EXISTS files")
            try db.execute(sql: "DROP TABLE IF EXISTS grdb_migrations")
        }
        try handleMigrations()
    }
}
