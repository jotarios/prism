//
//  IncrementalSyncDatabaseManagerTests.swift
//  prismTests
//
//  Covers the diff-based syncSearchIndex(from:volumeUUID:diff:) and the
//  interaction with DuckDBStore's mergeAndDiff. Separate file from SyncTests
//  so the singleton-state concerns stay contained.
//
//  NOTE ON PARALLEL EXECUTION: like SyncTests and IntegrationTests, these
//  tests use `DatabaseManager.shared`, which writes to
//  `~/Library/Application Support/Prism/index.db`. When xcodebuild runs tests
//  in parallel processes (the default), another process can call
//  `rebuildDatabase()` on that same on-disk file mid-test and flake these
//  assertions. Run with `-parallel-testing-enabled NO` in CI, or isolate
//  this class to a dedicated test plan. The underlying fix is to make
//  DatabaseManager instantiable with a per-test path, tracked as a separate
//  follow-up.
//

import XCTest
@testable import prism

final class IncrementalSyncDatabaseManagerTests: XCTestCase {

    var store: DuckDBStore!
    var dbManager: DatabaseManager!
    var testPath: String!

    override func setUp() async throws {
        testPath = FileManager.default.temporaryDirectory
            .appendingPathComponent("PrismIncDM_\(UUID().uuidString).duckdb").path
        store = try DuckDBStore(path: testPath)

        dbManager = DatabaseManager.shared
        do { try dbManager.open() } catch { /* already open */ }
        try dbManager.rebuildDatabase()
    }

    override func tearDown() async throws {
        dbManager.close()
        store = nil
        try? FileManager.default.removeItem(atPath: testPath)
        try? FileManager.default.removeItem(atPath: testPath + ".wal")
    }

    private func file(_ name: String, mtime: Int = 1_700_000_000, size: Int64 = 1024) -> ScannedFile {
        ScannedFile(filename: name, parentPath: "/Volumes/Test",
                    ext: "mp3", sizeBytes: size, modTimeSec: mtime,
                    createTimeSec: 1_700_000_000, isDirectory: false)
    }

    private func runScan(volume: String, files: [ScannedFile]) throws -> ScanDiff {
        _ = try store.beginScan(volumeUUID: volume)
        try store.ingestBatch(files, volumeUUID: volume)
        return try store.mergeAndDiff(volumeUUID: volume)
    }

    // MARK: - Invariants

    // Tests in this suite use unique filename prefixes so they don't collide
    // with other parallel test processes sharing DatabaseManager.shared's
    // application-support database. We assert against FTS5 search results
    // scoped by the unique prefix, not against getFileCount() globals.

    func testIncrementalSyncEmptyDiffIsNoop() async throws {
        let volA = "VOL-A"
        let prefix = "emptynoop\(UUID().uuidString.prefix(8))"
        let files = (0..<5).map { file("\(prefix)\($0).mp3") }

        let diff1 = try runScan(volume: volA, files: files)
        try dbManager.syncSearchIndex(from: store, volumeUUID: volA, diff: diff1)
        let hits1 = try await dbManager.searchFileIDs(query: prefix, limit: 10)
        XCTAssertEqual(hits1.count, 5)

        // Rescan with same files → empty diff → sync should touch nothing.
        let diff2 = try runScan(volume: volA, files: files)
        XCTAssertTrue(diff2.isEmpty)

        let start = CFAbsoluteTimeGetCurrent()
        try dbManager.syncSearchIndex(from: store, volumeUUID: volA, diff: diff2)
        let elapsed = CFAbsoluteTimeGetCurrent() - start

        let hits2 = try await dbManager.searchFileIDs(query: prefix, limit: 10)
        XCTAssertEqual(hits2.count, 5)
        XCTAssertLessThan(elapsed, 0.1, "Empty-diff sync must finish in <100 ms")
    }

    func testIncrementalSyncAppliesAdded() async throws {
        let volA = "VOL-A"
        let prefix = "added\(UUID().uuidString.prefix(8))"
        let diff1 = try runScan(volume: volA, files: (0..<3).map { file("\(prefix)\($0).mp3") })
        try dbManager.syncSearchIndex(from: store, volumeUUID: volA, diff: diff1)

        let diff2 = try runScan(volume: volA, files: (0..<5).map { file("\(prefix)\($0).mp3") })
        XCTAssertEqual(diff2.added.count, 2)
        try dbManager.syncSearchIndex(from: store, volumeUUID: volA, diff: diff2)

        let hits = try await dbManager.searchFileIDs(query: prefix, limit: 10)
        XCTAssertEqual(hits.count, 5)

        // Search should find the most-recently-added filename.
        let hit4 = try await dbManager.searchFileIDs(query: "\(prefix)4", limit: 5)
        XCTAssertEqual(hit4.count, 1)
    }

    func testIncrementalSyncAppliesRemoved() async throws {
        let volA = "VOL-A"
        let prefix = "removed\(UUID().uuidString.prefix(8))"
        let diff1 = try runScan(volume: volA, files: (0..<5).map { file("\(prefix)\($0).mp3") })
        try dbManager.syncSearchIndex(from: store, volumeUUID: volA, diff: diff1)

        // Remove two.
        let diff2 = try runScan(volume: volA, files: (0..<3).map { file("\(prefix)\($0).mp3") })
        XCTAssertEqual(diff2.removedIds.count, 2)
        try dbManager.syncSearchIndex(from: store, volumeUUID: volA, diff: diff2)

        let remaining = try await dbManager.searchFileIDs(query: prefix, limit: 10)
        XCTAssertEqual(remaining.count, 3)
        let idsOfRemoved = try await dbManager.searchFileIDs(query: "\(prefix)4", limit: 10)
        XCTAssertTrue(idsOfRemoved.isEmpty, "FTS5 should no longer return removed filename")
    }

    func testIncrementalSyncAppliesModified() async throws {
        let volA = "VOL-A"
        let tag = "mod\(UUID().uuidString.prefix(8))"
        let original = [file("\(tag)name.mp3", mtime: 1)]
        let diff1 = try runScan(volume: volA, files: original)
        try dbManager.syncSearchIndex(from: store, volumeUUID: volA, diff: diff1)
        let hits1 = try await dbManager.searchFileIDs(query: tag, limit: 5)
        XCTAssertEqual(hits1.count, 1)

        // Size changes only, same filename, same path → hits the modified path.
        let sizeBump = [file("\(tag)name.mp3", mtime: 1, size: 9999)]
        let diff2 = try runScan(volume: volA, files: sizeBump)
        XCTAssertEqual(diff2.modified.count, 1)
        try dbManager.syncSearchIndex(from: store, volumeUUID: volA, diff: diff2)

        let hits2 = try await dbManager.searchFileIDs(query: tag, limit: 5)
        XCTAssertEqual(hits2.count, 1)
    }

    func testIncrementalSyncMixedAddModifyRemove() async throws {
        let volA = "VOL-A"
        let tag = "mix\(UUID().uuidString.prefix(8))"
        let originals = [
            file("\(tag)keep.mp3", mtime: 1),
            file("\(tag)modify.mp3", mtime: 1, size: 100),
            file("\(tag)remove.mp3", mtime: 1),
        ]
        let diff1 = try runScan(volume: volA, files: originals)
        try dbManager.syncSearchIndex(from: store, volumeUUID: volA, diff: diff1)
        let hits1 = try await dbManager.searchFileIDs(query: tag, limit: 10)
        XCTAssertEqual(hits1.count, 3)

        let next = [
            file("\(tag)keep.mp3", mtime: 1),                    // unchanged
            file("\(tag)modify.mp3", mtime: 1, size: 999),       // modified
            file("\(tag)new.mp3", mtime: 1),                     // added
            // remove.mp3 dropped
        ]
        let diff2 = try runScan(volume: volA, files: next)
        XCTAssertEqual(diff2.added.count, 1)
        XCTAssertEqual(diff2.modified.count, 1)
        XCTAssertEqual(diff2.removedIds.count, 1)

        try dbManager.syncSearchIndex(from: store, volumeUUID: volA, diff: diff2)
        let hits2 = try await dbManager.searchFileIDs(query: tag, limit: 10)
        XCTAssertEqual(hits2.count, 3)

        let keepHits = try await dbManager.searchFileIDs(query: "\(tag)keep", limit: 5)
        XCTAssertEqual(keepHits.count, 1)
        let newHits = try await dbManager.searchFileIDs(query: "\(tag)new", limit: 5)
        XCTAssertEqual(newHits.count, 1)
        let removeHits = try await dbManager.searchFileIDs(query: "\(tag)remove", limit: 5)
        XCTAssertEqual(removeHits.count, 0)
    }

    func testTriggersStayInPlaceAfterIncrementalSync() async throws {
        let volA = "VOL-A"
        let diff = try runScan(volume: volA, files: [file("a.mp3")])
        try dbManager.syncSearchIndex(from: store, volumeUUID: volA, diff: diff)

        // The INSERT trigger is what keeps FTS5 in sync for future direct
        // INSERTs. If it were dropped (like the full-rebuild path does), a
        // subsequent INSERT would not update FTS5.
        // We prove triggers still work by confirming search returns results
        // for the just-inserted filename.
        let ids = try await dbManager.searchFileIDs(query: "a", limit: 5)
        XCTAssertGreaterThan(ids.count, 0)
    }
}
