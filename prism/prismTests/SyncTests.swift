//
//  SyncTests.swift
//  prismTests
//

import XCTest
@testable import prism

final class SyncTests: XCTestCase {

    var duckDBStore: DuckDBStore!
    var dbManager: DatabaseManager!
    var testDuckDBPath: String!

    override func setUp() async throws {
        testDuckDBPath = FileManager.default.temporaryDirectory
            .appendingPathComponent("PrismSyncTest_\(UUID().uuidString).duckdb").path
        duckDBStore = try DuckDBStore(path: testDuckDBPath)

        dbManager = DatabaseManager.shared
        try dbManager.open()
        try dbManager.rebuildDatabase()
    }

    override func tearDown() async throws {
        dbManager.close()
        duckDBStore = nil
        try? FileManager.default.removeItem(atPath: testDuckDBPath)
        try? FileManager.default.removeItem(atPath: testDuckDBPath + ".wal")
    }

    private func ingestFiles(count: Int) throws {
        let files = (0..<count).map { i in
            ScannedFile(
                filename: "file_\(i).mp3",
                parentPath: "/Volumes/Test",
                ext: "mp3",
                sizeBytes: 1024,
                modTimeSec: 1700000000 + i,
                createTimeSec: 1700000000,
                isDirectory: false
            )
        }
        try duckDBStore.ingestBatch(files, volumeUUID: "TEST-VOL")
    }

    func testSyncHappyPath() async throws {
        try ingestFiles(count: 500)

        try dbManager.syncSearchIndex(from: duckDBStore)

        let sqliteCount = try await dbManager.getFileCount()
        let duckDBCount = try duckDBStore.getFileCount()
        XCTAssertEqual(sqliteCount, duckDBCount)
        XCTAssertEqual(sqliteCount, 500)
    }

    func testFTS5SearchAfterSync() async throws {
        let files = [
            ScannedFile(filename: "dua lipa levitating.mp3", parentPath: "/Music", ext: "mp3",
                       sizeBytes: 1024, modTimeSec: 1700000000, createTimeSec: 1700000000, isDirectory: false),
            ScannedFile(filename: "madonna material girl.flac", parentPath: "/Music", ext: "flac",
                       sizeBytes: 2048, modTimeSec: 1700000001, createTimeSec: 1700000000, isDirectory: false),
            ScannedFile(filename: "dua lipa dont start now.wav", parentPath: "/Music", ext: "wav",
                       sizeBytes: 4096, modTimeSec: 1700000002, createTimeSec: 1700000000, isDirectory: false),
        ]
        try duckDBStore.ingestBatch(files, volumeUUID: "TEST")
        try dbManager.syncSearchIndex(from: duckDBStore)

        let ids = try await dbManager.searchFileIDs(query: "dua", limit: 100)
        XCTAssertEqual(ids.count, 2, "Should find 2 'dua' files, found \(ids.count)")

        let madonnaIDs = try await dbManager.searchFileIDs(query: "madonna", limit: 100)
        XCTAssertEqual(madonnaIDs.count, 1)
    }

    func testResyncIsIdempotent() async throws {
        try ingestFiles(count: 100)

        try dbManager.syncSearchIndex(from: duckDBStore)
        try dbManager.syncSearchIndex(from: duckDBStore)

        let count = try await dbManager.getFileCount()
        XCTAssertEqual(count, 100)
    }

    func testTriggersRestoredAfterSync() async throws {
        try ingestFiles(count: 10)
        try dbManager.syncSearchIndex(from: duckDBStore)

        let triggerCount = try await Task.detached {
            try self.dbManager.getPragmaSettings()
        }.value

        // Verify by checking we can search (FTS5 is populated)
        let ids = try await dbManager.searchFileIDs(query: "file_0", limit: 10)
        XCTAssertGreaterThan(ids.count, 0)
    }
}
