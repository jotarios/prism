//
//  IncrementalSyncTests.swift
//  prismTests
//
//  Covers the new incremental sync surface: beginScan/mergeAndDiff/applyDiff
//  on DuckDBStore and the diff-based syncSearchIndex on DatabaseManager.
//  One invariant per test; the invariants are enumerated in the plan.
//

import XCTest
import DuckDB
@testable import prism

final class IncrementalSyncTests: XCTestCase {

    var store: DuckDBStore!
    var testPath: String!
    let volA = "VOL-A"
    let volB = "VOL-B"

    override func setUp() async throws {
        testPath = FileManager.default.temporaryDirectory
            .appendingPathComponent("PrismIncremental_\(UUID().uuidString).duckdb").path
        store = try DuckDBStore(path: testPath)
    }

    override func tearDown() async throws {
        store = nil
        try? FileManager.default.removeItem(atPath: testPath)
        try? FileManager.default.removeItem(atPath: testPath + ".wal")
    }

    // MARK: - Helpers

    private func file(_ name: String, path dir: String = "/Volumes/Test/Music", size: Int64 = 1024, mtime: Int = 1_700_000_000) -> ScannedFile {
        ScannedFile(
            filename: name,
            parentPath: dir,
            ext: "mp3",
            sizeBytes: size,
            modTimeSec: mtime,
            createTimeSec: 1_700_000_000,
            isDirectory: false
        )
    }

    private func runScan(volume: String, files: [ScannedFile]) throws -> ScanDiff {
        _ = try store.beginScan(volumeUUID: volume)
        try store.ingestBatch(files, volumeUUID: volume)
        return try store.mergeAndDiff(volumeUUID: volume)
    }

    // MARK: - Staging isolation

    func testStagingWritesDoNotTouchFilesUntilMerge() throws {
        _ = try store.beginScan(volumeUUID: volA)
        try store.ingestBatch([file("a.mp3")], volumeUUID: volA)
        // Before merge: files table is still empty.
        XCTAssertEqual(try store.getFileCount(), 0)
        _ = try store.mergeAndDiff(volumeUUID: volA)
        XCTAssertEqual(try store.getFileCount(), 1)
    }

    // MARK: - Diff correctness

    func testDiffAllAddedOnFirstScan() throws {
        let diff = try runScan(volume: volA, files: (0..<10).map { file("f\($0).mp3") })
        XCTAssertEqual(diff.added.count, 10)
        XCTAssertEqual(diff.modified.count, 0)
        XCTAssertEqual(diff.removedIds.count, 0)
    }

    func testDiffEmptyOnUnchangedRescan() throws {
        let files = (0..<10).map { file("f\($0).mp3") }
        _ = try runScan(volume: volA, files: files)
        let diff2 = try runScan(volume: volA, files: files)
        XCTAssertTrue(diff2.isEmpty, "Unchanged rescan should produce empty diff, got added=\(diff2.added.count) modified=\(diff2.modified.count) removed=\(diff2.removedIds.count)")
    }

    func testDiffDetectsModifiedByMtime() throws {
        let original = (0..<5).map { file("f\($0).mp3", mtime: 1_700_000_000) }
        _ = try runScan(volume: volA, files: original)
        // Same paths, different mtimes on 2 of them.
        let changed = [
            file("f0.mp3", mtime: 1_700_000_000), // unchanged
            file("f1.mp3", mtime: 1_700_099_999), // modified
            file("f2.mp3", mtime: 1_700_099_999), // modified
            file("f3.mp3", mtime: 1_700_000_000), // unchanged
            file("f4.mp3", mtime: 1_700_000_000)  // unchanged
        ]
        let diff = try runScan(volume: volA, files: changed)
        XCTAssertEqual(diff.added.count, 0)
        XCTAssertEqual(diff.modified.count, 2)
        XCTAssertEqual(diff.removedIds.count, 0)
    }

    func testDiffDetectsModifiedBySize() throws {
        let original = [file("a.mp3", size: 1000)]
        _ = try runScan(volume: volA, files: original)
        let changed = [file("a.mp3", size: 2000)] // same mtime, different size
        let diff = try runScan(volume: volA, files: changed)
        XCTAssertEqual(diff.modified.count, 1)
    }

    func testDiffDetectsRemoved() throws {
        _ = try runScan(volume: volA, files: (0..<5).map { file("f\($0).mp3") })
        // Second scan sees only 3 of the original 5.
        let fewer = (0..<3).map { file("f\($0).mp3") }
        let diff = try runScan(volume: volA, files: fewer)
        XCTAssertEqual(diff.removedIds.count, 2)
        XCTAssertEqual(diff.added.count, 0)
    }

    func testDiffDetectsAdded() throws {
        _ = try runScan(volume: volA, files: (0..<3).map { file("f\($0).mp3") })
        let more = (0..<5).map { file("f\($0).mp3") }
        let diff = try runScan(volume: volA, files: more)
        XCTAssertEqual(diff.added.count, 2)
    }

    func testDiffEmptyStagingRemovesAllVolumeFiles() throws {
        _ = try runScan(volume: volA, files: (0..<10).map { file("f\($0).mp3") })
        XCTAssertEqual(try store.getFileCountByVolume(volA), 10)
        let diff = try runScan(volume: volA, files: [])
        XCTAssertEqual(diff.removedIds.count, 10)
        XCTAssertEqual(try store.getFileCountByVolume(volA), 0)
    }

    // MARK: - ID stability

    func testIdStableAcrossUnchangedRescan() throws {
        let files = [file("track.mp3")]
        _ = try runScan(volume: volA, files: files)
        let idBefore = try store.getFilesByIDs([PathHash.id(volumeUUID: volA, path: "/Volumes/Test/Music/track.mp3")])[0].id
        _ = try runScan(volume: volA, files: files)
        let idAfter = try store.getFilesByIDs([PathHash.id(volumeUUID: volA, path: "/Volumes/Test/Music/track.mp3")])[0].id
        XCTAssertEqual(idBefore, idAfter)
    }

    func testIdStableAcrossModifiedRescan() throws {
        let original = [file("track.mp3", mtime: 1_700_000_000)]
        _ = try runScan(volume: volA, files: original)
        let allBefore = try store.getAllFiles(limit: 10)
        let idBefore = allBefore[0].id

        let changed = [file("track.mp3", mtime: 1_700_099_999)]
        let diff = try runScan(volume: volA, files: changed)
        XCTAssertEqual(diff.modified.count, 1)
        XCTAssertEqual(diff.modified[0].id, idBefore, "modified row's id must be preserved")
    }

    // MARK: - Volume isolation

    func testScanningOneVolumeDoesNotTouchAnother() throws {
        _ = try runScan(volume: volA, files: (0..<5).map { file("a\($0).mp3", path: "/a") })
        _ = try runScan(volume: volB, files: (0..<3).map { file("b\($0).mp3", path: "/b") })
        XCTAssertEqual(try store.getFileCountByVolume(volA), 5)
        XCTAssertEqual(try store.getFileCountByVolume(volB), 3)

        // Capture all of VOL-B's row identities.
        let bIdsBefore = Set(try store.getAllFiles(limit: 100).filter { $0.volumeUUID == volB }.map(\.id))

        // Rescan VOL-A with completely different contents.
        _ = try runScan(volume: volA, files: [file("new.mp3", path: "/a")])

        let bIdsAfter = Set(try store.getAllFiles(limit: 100).filter { $0.volumeUUID == volB }.map(\.id))
        XCTAssertEqual(bIdsBefore, bIdsAfter)
        XCTAssertEqual(try store.getFileCountByVolume(volB), 3)
    }

    // MARK: - Concurrency guard

    func testBeginScanThrowsIfAlreadyInProgress() throws {
        _ = try store.beginScan(volumeUUID: volA)
        XCTAssertThrowsError(try store.beginScan(volumeUUID: volB)) { error in
            guard case IndexError.scanAlreadyInProgress(let held) = error else {
                return XCTFail("Expected scanAlreadyInProgress, got \(error)")
            }
            XCTAssertEqual(held, volA)
        }
        // Cleanup: release the slot.
        _ = try store.mergeAndDiff(volumeUUID: volA)
    }

    func testMergeReleasesSlotSoNextScanCanStart() throws {
        _ = try store.beginScan(volumeUUID: volA)
        _ = try store.mergeAndDiff(volumeUUID: volA)
        // Should now succeed.
        _ = try store.beginScan(volumeUUID: volA)
        _ = try store.mergeAndDiff(volumeUUID: volA)
    }

    // MARK: - Staging cleanup

    func testOrphanedStagingDroppedByCleanup() throws {
        // Simulate a crashed scan by manually creating a staging table.
        try store.connection_execute("""
            CREATE TABLE IF NOT EXISTS files_staging_ORPHAN_FAKE (id BIGINT)
        """)

        // Confirm it's there.
        let before = try store.connection_query("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'main' AND table_name LIKE 'files_staging_%'
        """)
        XCTAssertEqual(Int(before[0].cast(to: Int64.self)[0] ?? -1), 1)

        // Run cleanup directly (the same call init makes at startup).
        try store.cleanupOrphanedStaging()

        let after = try store.connection_query("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'main' AND table_name LIKE 'files_staging_%'
        """)
        XCTAssertEqual(Int(after[0].cast(to: Int64.self)[0] ?? -1), 0, "orphan staging table should have been dropped")
    }

    // MARK: - applyDiff cache consistency

    func testApplyDiffNoOpWhenCacheNotLoaded() throws {
        _ = try runScan(volume: volA, files: [file("a.mp3")])
        // Don't loadCache. applyDiff should be a safe no-op.
        let diff = ScanDiff(
            added: [.init(id: 1, filename: "x", ext: "mp3")],
            modified: [],
            removedIds: []
        )
        try store.applyDiff(diff)
        // Cache is still not populated; getAllCachedValues remains empty.
        XCTAssertTrue(store.getAllCachedValues().isEmpty)
    }

    func testApplyDiffAddsRowsToCache() throws {
        _ = try runScan(volume: volA, files: (0..<3).map { file("f\($0).mp3") })
        try store.loadCache()
        XCTAssertEqual(store.getAllCachedValues().count, 3)

        // Add two more, rescan → applyDiff should insert them into cache.
        let diff = try runScan(volume: volA, files: (0..<5).map { file("f\($0).mp3") })
        XCTAssertEqual(diff.added.count, 2)
        try store.applyDiff(diff)
        XCTAssertEqual(store.getAllCachedValues().count, 5)
    }

    func testApplyDiffRemovesRowsFromCache() throws {
        _ = try runScan(volume: volA, files: (0..<5).map { file("f\($0).mp3") })
        try store.loadCache()
        XCTAssertEqual(store.getAllCachedValues().count, 5)

        let diff = try runScan(volume: volA, files: (0..<2).map { file("f\($0).mp3") })
        XCTAssertEqual(diff.removedIds.count, 3)
        try store.applyDiff(diff)
        XCTAssertEqual(store.getAllCachedValues().count, 2)
    }

    func testApplyDiffUpdatesModifiedRowsInCache() throws {
        _ = try runScan(volume: volA, files: [file("a.mp3", size: 1000)])
        try store.loadCache()
        let before = store.getAllCachedValues()[0]
        XCTAssertEqual(before.sizeBytes, 1000)

        let diff = try runScan(volume: volA, files: [file("a.mp3", size: 2000)])
        XCTAssertEqual(diff.modified.count, 1)
        try store.applyDiff(diff)

        let after = store.getAllCachedValues()[0]
        XCTAssertEqual(after.sizeBytes, 2000)
        XCTAssertEqual(after.id, before.id, "id must be preserved across modify")
    }
}
