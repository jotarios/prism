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
    case incompatibleVersion(current: Int, required: Int)
}

final class DatabaseManager {
    static let shared = DatabaseManager()

    // Current schema version - increment when schema changes
    private static let CURRENT_SCHEMA_VERSION = 1

    private var dbPool: DatabasePool!
    private let dbPath: String

    private init() {
        // Database location: ~/Library/Application Support/Prism/index.db
        let appSupport = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
        let prismDir = appSupport.appendingPathComponent("Prism", isDirectory: true)

        try? FileManager.default.createDirectory(at: prismDir, withIntermediateDirectories: true)

        dbPath = prismDir.appendingPathComponent("index.db").path
    }

    // MARK: - Database Lifecycle

    func open() throws {
        // Configure database pool for optimal concurrency
        var config = Configuration()

        // MAX PERFORMANCE: Prepare EACH connection (runs for every connection in the pool)
        // This is critical - prepareDatabase runs ONCE PER CONNECTION, not once globally
        config.prepareDatabase { db in
            // KEY CONFIG #1: WAL Mode
            // Allows reading while writing - critical for UI responsiveness
            // Note: This only needs to be set once, but it's safe to call repeatedly
            try db.execute(sql: "PRAGMA journal_mode=WAL")

            // KEY CONFIG #2: Synchronous = NORMAL
            // 'FULL' waits for physical disk - too slow
            // 'NORMAL' is safe for WAL and much faster
            try db.execute(sql: "PRAGMA synchronous=NORMAL")

            // KEY CONFIG #3: Cache Size
            // Use ~50MB of RAM for caching pages to speed up searching
            // Negative number = kilobytes
            try db.execute(sql: "PRAGMA cache_size=-50000")

            // KEY CONFIG #4: Temp Store
            // Store temporary indices in RAM, not on disk
            try db.execute(sql: "PRAGMA temp_store=MEMORY")

            // KEY CONFIG #5: Foreign Keys
            // Must be enabled on every connection
            try db.execute(sql: "PRAGMA foreign_keys=ON")

            // KEY CONFIG #6: Busy Timeout
            // Wait up to 100ms if database is locked, then fail fast
            // This prevents UI freezing by not blocking indefinitely
            try db.execute(sql: "PRAGMA busy_timeout=100")
        }

        // KEY CONFIG #7: Set read-only transaction deferred mode
        // This allows multiple readers to coexist with a writer
        config.defaultTransactionKind = .deferred

        // USE A POOL, NOT A QUEUE
        // DatabasePool manages concurrent Reader/Writer threads automatically
        dbPool = try DatabasePool(path: dbPath, configuration: config)

        // Check database integrity
        try checkIntegrity()

        // Handle migrations
        try handleMigrations()
    }

    func close() {
        // GRDB handles connection cleanup automatically
        dbPool = nil
    }

    // MARK: - Migrations

    private func handleMigrations() throws {
        var migrator = DatabaseMigrator()

        // Migration v1: Initial schema
        migrator.registerMigration("v1") { db in
            // 1. Primary files table with stable IDs
            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT NOT NULL,
                    path TEXT NOT NULL UNIQUE,
                    volume_uuid TEXT NOT NULL,
                    extension TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    date_modified INTEGER NOT NULL,
                    date_created INTEGER NOT NULL,
                    is_online INTEGER NOT NULL DEFAULT 1
                );
                """)

            // 2. FTS5 virtual table for full-text search
            try db.execute(sql: """
                CREATE VIRTUAL TABLE IF NOT EXISTS files_fts USING fts5(
                    filename,
                    extension,
                    content='files',
                    content_rowid='id',
                    prefix='2 3 4'
                );
                """)

            // 3. Triggers to keep FTS5 in sync with files table
            try db.execute(sql: """
                CREATE TRIGGER IF NOT EXISTS files_ai AFTER INSERT ON files BEGIN
                    INSERT INTO files_fts(rowid, filename, extension)
                    VALUES (new.id, new.filename, new.extension);
                END;
                """)

            try db.execute(sql: """
                CREATE TRIGGER IF NOT EXISTS files_ad AFTER DELETE ON files BEGIN
                    INSERT INTO files_fts(files_fts, rowid, filename, extension)
                    VALUES('delete', old.id, old.filename, old.extension);
                END;
                """)

            try db.execute(sql: """
                CREATE TRIGGER IF NOT EXISTS files_au AFTER UPDATE ON files BEGIN
                    INSERT INTO files_fts(files_fts, rowid, filename, extension)
                    VALUES('delete', old.id, old.filename, old.extension);
                    INSERT INTO files_fts(rowid, filename, extension)
                    VALUES (new.id, new.filename, new.extension);
                END;
                """)

            // 4. Audio metadata table
            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS audio_metadata (
                    file_id INTEGER PRIMARY KEY,
                    duration_seconds REAL NOT NULL,
                    FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
                );
                """)

            // 5. Create indexes
            try db.execute(sql: "CREATE INDEX IF NOT EXISTS idx_volume_uuid ON files(volume_uuid);")
            try db.execute(sql: "CREATE INDEX IF NOT EXISTS idx_date_modified ON files(date_modified);")
            try db.execute(sql: "CREATE INDEX IF NOT EXISTS idx_size_bytes ON files(size_bytes);")
            try db.execute(sql: "CREATE INDEX IF NOT EXISTS idx_extension ON files(extension);")
            try db.execute(sql: "CREATE INDEX IF NOT EXISTS idx_is_online ON files(is_online);")
        }

        // Future migrations go here:
        // migrator.registerMigration("v2") { db in
        //     try db.execute(sql: "ALTER TABLE files ADD COLUMN new_field TEXT;")
        // }

        // Run migrations
        try migrator.migrate(dbPool)
    }

    // MARK: - Integrity Check

    private func checkIntegrity() throws {
        try dbPool.read { db in
            let result = try String.fetchOne(db, sql: "PRAGMA integrity_check")
            if result != "ok" {
                throw DatabaseError.corruptDatabase
            }
        }
    }

    // MARK: - Insert Operations

    /// Insert files in batches for optimal performance
    /// WRITE STRATEGY: Single transaction per batch (called with 5,000-10,000 records)
    /// The database is only locked for the split second it takes to commit the batch
    func insertFiles(_ records: [FileRecordInsert]) throws {
        guard !records.isEmpty else { return }

        // ONE TRANSACTION for the entire batch
        // This is the key to preventing freezing:
        // - Lock is held for a brief moment during the transaction
        // - Readers can access the database between batch commits
        try dbPool.write { db in
            // Use a cached prepared statement for maximum performance
            let statement = try db.cachedStatement(sql: """
                INSERT OR REPLACE INTO files (filename, path, volume_uuid, extension, size_bytes, date_modified, date_created, is_online)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """)

            // Insert all records in this single transaction
            for record in records {
                try statement.execute(arguments: [
                    record.filename,
                    record.path,
                    record.volumeUUID,
                    record.ext,
                    record.sizeBytes,
                    Int64(record.dateModified.timeIntervalSince1970),
                    Int64(record.dateCreated.timeIntervalSince1970),
                    record.isOnline ? 1 : 0
                ])
            }
        }
        // Transaction commits here in one atomic operation
        // Write lock is released, allowing readers to grab a fresh snapshot
    }

    // MARK: - Database Rebuild

    /// Delete all data and reset schema (for rebuild operations)
    func rebuildDatabase() throws {
        try dbPool.write { db in
            // Drop all tables and triggers
            try db.execute(sql: "DROP TABLE IF EXISTS audio_metadata;")
            try db.execute(sql: "DROP TRIGGER IF EXISTS files_au;")
            try db.execute(sql: "DROP TRIGGER IF EXISTS files_ad;")
            try db.execute(sql: "DROP TRIGGER IF EXISTS files_ai;")
            try db.execute(sql: "DROP TABLE IF EXISTS files_fts;")
            try db.execute(sql: "DROP TABLE IF EXISTS files;")

            // Drop migration tracking table so migrations can run again
            try db.execute(sql: "DROP TABLE IF EXISTS grdb_migrations;")
        }

        // Re-run migrations (will recreate all tables)
        try handleMigrations()
    }

    /// Delete files for a specific volume
    func deleteFilesByVolume(_ volumeUUID: String) throws {
        try dbPool.write { db in
            try db.execute(sql: "DELETE FROM files WHERE volume_uuid = ?", arguments: [volumeUUID])
        }
    }

    // MARK: - Query Operations

    func getFileCount() async throws -> Int {
        try await dbPool.read { db in
            try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM files") ?? 0
        }
    }

    /// Get current PRAGMA settings for verification
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

    func getFileCountByVolume(_ volumeUUID: String) throws -> Int {
        try dbPool.read { db in
            try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM files WHERE volume_uuid = ?", arguments: [volumeUUID]) ?? 0
        }
    }

    /// Get all indexed files (limited for display)
    func getAllFiles(limit: Int = 1000) async throws -> [SearchResult] {
        try await dbPool.read { db in
            let rows = try Row.fetchAll(db, sql: """
                SELECT id, filename, path, volume_uuid, extension, size_bytes, date_modified, is_online
                FROM files
                ORDER BY date_modified DESC
                LIMIT ?
                """, arguments: [limit])

            return rows.map { row in
                SearchResult(
                    id: row["id"],
                    filename: row["filename"],
                    path: row["path"],
                    volumeUUID: row["volume_uuid"],
                    ext: row["extension"],
                    sizeBytes: row["size_bytes"],
                    dateModified: Date(timeIntervalSince1970: Double(row["date_modified"] as Int64)),
                    isOnline: (row["is_online"] as Int) == 1,
                    durationSeconds: nil
                )
            }
        }
    }

    /// Search files using FTS5 full-text search
    /// Uses prefix matching for instant-as-you-type search
    func searchFiles(query: String, limit: Int = 1000) async throws -> [SearchResult] {
        guard !query.isEmpty else {
            return try await getAllFiles(limit: limit)
        }

        return try await dbPool.read { db in
            // Sanitize query and prepare for FTS5 prefix search
            let sanitized = query
                .trimmingCharacters(in: .whitespacesAndNewlines)
                .replacingOccurrences(of: "\"", with: "\"\"")  // Escape quotes

            // Build FTS5 query with prefix matching (*)
            // Split on spaces to support multi-word queries
            let terms = sanitized.components(separatedBy: .whitespaces)
                .filter { !$0.isEmpty }
                .map { "\"\($0)\"*" }  // Each term gets prefix matching
                .joined(separator: " ")

            let rows = try Row.fetchAll(db, sql: """
                SELECT f.id, f.filename, f.path, f.volume_uuid, f.extension,
                       f.size_bytes, f.date_modified, f.is_online
                FROM files_fts
                JOIN files f ON files_fts.rowid = f.id
                WHERE files_fts MATCH ?
                ORDER BY rank
                LIMIT ?
                """, arguments: [terms, limit])

            return rows.map { row in
                SearchResult(
                    id: row["id"],
                    filename: row["filename"],
                    path: row["path"],
                    volumeUUID: row["volume_uuid"],
                    ext: row["extension"],
                    sizeBytes: row["size_bytes"],
                    dateModified: Date(timeIntervalSince1970: Double(row["date_modified"] as Int64)),
                    isOnline: (row["is_online"] as Int) == 1,
                    durationSeconds: nil
                )
            }
        }
    }
}
