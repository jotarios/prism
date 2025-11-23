//
//  DatabaseTests.swift
//  prismTests
//

import XCTest
@testable import prism

final class DatabaseTests: XCTestCase {

    var dbManager: DatabaseManager!

    override func setUp() async throws {
        // Use a temporary database for testing
        dbManager = DatabaseManager.shared
        try dbManager.open()
        try dbManager.rebuildDatabase()
    }

    override func tearDown() async throws {
        dbManager.close()
    }

    func testDatabaseCreation() throws {
        // Database should be created with version 1
        let count = try dbManager.getFileCount()
        XCTAssertEqual(count, 0, "New database should have no files")
    }

    func testBatchInsert() throws {
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
        let count = try dbManager.getFileCount()
        XCTAssertEqual(count, 1000, "Should have inserted 1000 files")

        // Verify count by volume
        let volumeCount = try dbManager.getFileCountByVolume("TEST-UUID-123")
        XCTAssertEqual(volumeCount, 1000, "Should have 1000 files for test volume")
    }

    func testLargeBatchInsert() throws {
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

        let count = try dbManager.getFileCount()
        XCTAssertEqual(count, 25000, "Should have inserted 25,000 files")

        // Verify batching worked (should be fast)
        XCTAssertLessThan(elapsed, 5.0, "Should insert 25k records in under 5 seconds")
    }

    func testRebuildDatabase() throws {
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

        var count = try dbManager.getFileCount()
        XCTAssertEqual(count, 1)

        // Rebuild
        try dbManager.rebuildDatabase()

        // Should be empty
        count = try dbManager.getFileCount()
        XCTAssertEqual(count, 0, "Database should be empty after rebuild")
    }
}
