//
//  DatabaseTests.swift
//  prismTests
//

import XCTest
@testable import prism

final class DatabaseTests: XCTestCase {

    var dbManager: DatabaseManager!

    override func setUp() {
        super.setUp()
        // Use the shared database manager
        dbManager = DatabaseManager.shared

        print("setUp: Opening database...")
        // Open database if not already open (will throw if already open, that's fine)
        do {
            try dbManager.open()
            print("setUp: Database opened successfully")
        } catch {
            print("setUp: Database already open or error: \(error)")
        }

        print("setUp: Rebuilding database...")
        // Clear all data before each test
        do {
            try dbManager.rebuildDatabase()
            print("setUp: Database rebuilt")
        } catch {
            print("setUp: Failed to rebuild: \(error)")
        }
    }

    override func tearDown() {
        // Don't close the database - we're using a shared singleton
        // Just clean up the data
        try? dbManager.rebuildDatabase()
        super.tearDown()
    }

    func testDatabaseCreation() async throws {
        // Database should be created with version 1
        print("Testing database creation...")
        let count = try await dbManager.getFileCount()
        print("File count: \(count)")
        XCTAssertEqual(count, 0, "New database should have no files, but got \(count)")
    }

    func testBatchInsert() async throws {
        // Create test records
        var records: [FileRecordInsert] = []
        for i in 1...1000 {
            records.append(FileRecordInsert(
                filename: "test_file_\(i).mp3",
                path: "/Volumes/Test/Music/test_file_\(i).mp3",
                volumeUUID: "TEST-UUID-123",
                ext: "mp3",
                sizeBytes: Int64(i * 1024),
                dateModified: Date(),
                dateCreated: Date(),
                isOnline: true
            ))
        }

        // Insert in batch
        try dbManager.insertFiles(records)

        // Verify count
        let count = try await dbManager.getFileCount()
        XCTAssertEqual(count, 1000, "Should have inserted 1000 files")

        // Verify count by volume
        let volumeCount = try dbManager.getFileCountByVolume("TEST-UUID-123")
        XCTAssertEqual(volumeCount, 1000, "Should have 1000 files for test volume")
    }

    func testLargeBatchInsert() async throws {
        // Test with more than batch size (10,000)
        var records: [FileRecordInsert] = []
        for i in 1...25000 {
            records.append(FileRecordInsert(
                filename: "file_\(i).mp3",
                path: "/Volumes/External/file_\(i).mp3",
                volumeUUID: "EXTERNAL-UUID",
                ext: "mp3",
                sizeBytes: Int64(i * 512),
                dateModified: Date(),
                dateCreated: Date(),
                isOnline: true
            ))
        }

        let startTime = Date()
        try dbManager.insertFiles(records)
        let elapsed = Date().timeIntervalSince(startTime)

        print("Inserted 25,000 records in \(elapsed) seconds")

        let count = try await dbManager.getFileCount()
        XCTAssertEqual(count, 25000, "Should have inserted 25,000 files")

        // Verify batching worked (should be fast)
        XCTAssertLessThan(elapsed, 5.0, "Should insert 25k records in under 5 seconds")
    }

    func testRebuildDatabase() async throws {
        // Insert some data
        let records = [
            FileRecordInsert(
                filename: "test.mp3",
                path: "/test.mp3",
                volumeUUID: "UUID",
                ext: "mp3",
                sizeBytes: 1024,
                dateModified: Date(),
                dateCreated: Date(),
                isOnline: true
            )
        ]
        try dbManager.insertFiles(records)

        var count = try await dbManager.getFileCount()
        XCTAssertEqual(count, 1)

        // Rebuild
        try dbManager.rebuildDatabase()

        // Should be empty
        count = try await dbManager.getFileCount()
        XCTAssertEqual(count, 0, "Database should be empty after rebuild")
    }

    func testDatabasePragmaSettings() throws {
        // Test that all PRAGMA settings are applied correctly
        let pragmaSettings = try dbManager.getPragmaSettings()

        // Verify WAL mode
        XCTAssertEqual(pragmaSettings["journal_mode"], "wal", "Journal mode should be WAL")

        // Verify synchronous mode
        XCTAssertEqual(pragmaSettings["synchronous"], "1", "Synchronous should be NORMAL (1)")

        // Verify cache size (should be negative for KB)
        if let cacheSize = pragmaSettings["cache_size"], let cacheSizeInt = Int(cacheSize) {
            XCTAssertLessThan(cacheSizeInt, 0, "Cache size should be negative (in KB)")
            XCTAssertLessThanOrEqual(cacheSizeInt, -50000, "Cache size should be at least 50MB")
        } else {
            XCTFail("Cache size not set")
        }

        // Verify temp store
        XCTAssertEqual(pragmaSettings["temp_store"], "2", "Temp store should be MEMORY (2)")

        // Verify foreign keys
        XCTAssertEqual(pragmaSettings["foreign_keys"], "1", "Foreign keys should be ON (1)")

        print("PRAGMA settings verified:")
        for (key, value) in pragmaSettings.sorted(by: { $0.key < $1.key }) {
            print("  \(key) = \(value)")
        }
    }
}
