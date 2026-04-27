//
//  ClearIndexRaceTests.swift
//  prismTests
//
//  From /plan-eng-review outside-voice critique #3:
//  deleteFilesByVolume / clearAll race with direct-diff writers.
//  These tests lock in the invariant that concurrent calls don't corrupt
//  DuckDB state or drop rows unexpectedly.
//

import XCTest
import DuckDB
@testable import prism

final class ClearIndexRaceTests: XCTestCase {

    var store: DuckDBStore!
    var testPath: String!
    let volA = "VOL-A"

    override func setUp() async throws {
        testPath = FileManager.default.temporaryDirectory
            .appendingPathComponent("PrismClearIndexRace_\(UUID().uuidString).duckdb").path
        store = try DuckDBStore(path: testPath)
    }

    override func tearDown() async throws {
        store = nil
        try? FileManager.default.removeItem(atPath: testPath)
        try? FileManager.default.removeItem(atPath: testPath + ".wal")
    }

    private func entry(_ name: String, volume: String = "VOL-A") -> ScanDiff.Entry {
        let path = "/Volumes/Test/" + name
        return ScanDiff.Entry(
            id: PathHash.id(volumeUUID: volume, path: path),
            filename: name,
            path: path,
            volumeUUID: volume,
            ext: "mp3",
            sizeBytes: 1024,
            dateModified: 1_700_000_000,
            dateCreated: 1_700_000_000
        )
    }

    /// Baseline: clearAll + applyDirectDiff serialized must not lose rows.
    func testClearAllThenApplyDirectDiff() throws {
        try store.applyDirectDiff(
            ScanDiff(added: [entry("a.mp3")], modified: [], removedIds: []),
            volumeUUID: volA
        )
        XCTAssertEqual(try store.getFileCount(), 1)
        try store.clearAll()
        XCTAssertEqual(try store.getFileCount(), 0)
        try store.applyDirectDiff(
            ScanDiff(added: [entry("b.mp3")], modified: [], removedIds: []),
            volumeUUID: volA
        )
        XCTAssertEqual(try store.getFileCount(), 1)
    }

    /// deleteFilesByVolume + pending-batch drain: after clear, any queued
    /// batch must apply as fresh adds rather than as duplicates of since-
    /// deleted rows.
    func testClearVolumeThenDrainPending() throws {
        let diff = ScanDiff(added: [entry("a.mp3")], modified: [], removedIds: [])
        try store.applyDirectDiff(diff, volumeUUID: volA)
        // Simulate a batch that arrived while user was deleting.
        store.enqueuePendingBatch(volumeUUID: volA, diff: diff)
        try store.deleteFilesByVolume(volA)
        // Draining now is analogous to coordinator reapplying.
        let pending = store.drainPendingBatches(volumeUUID: volA)
        for batch in pending {
            try store.applyDirectDiff(batch, volumeUUID: volA)
        }
        // INSERT OR REPLACE semantics: the queued add re-appears cleanly.
        XCTAssertEqual(try store.getFileCount(), 1)
    }

    /// Concurrent: a scan is in flight while Clear Index runs.
    /// The plan's resolution is for SearchViewModel.clearVolumeFiles to
    /// call coordinator.stopWatching FIRST. This test exercises the DB
    /// layer's behavior if the call order is violated — we expect NO
    /// corruption (staging table gets cleaned up on next init; files
    /// table stays consistent with SQLite).
    func testDeleteDuringActiveStagingScanDoesNotCorrupt() throws {
        try store.beginScan(volumeUUID: volA)
        try store.ingestBatch(
            [ScannedFile(filename: "a.mp3", parentPath: "/Volumes/Test",
                         ext: "mp3", sizeBytes: 1024,
                         modTimeSec: 1_700_000_000, createTimeSec: 1_700_000_000,
                         isDirectory: false)],
            volumeUUID: volA
        )
        // Delete mid-scan. The merge hasn't run yet, so `files` is empty.
        // Existing deleteFilesByVolume only touches `files`, not staging.
        // mergeAndDiff will then JOIN staging vs empty `files` → all added.
        try store.deleteFilesByVolume(volA)
        let diff = try store.mergeAndDiff(volumeUUID: volA)
        XCTAssertEqual(diff.added.count, 1)
        XCTAssertEqual(try store.getFileCount(), 1)
    }

    /// cleanupOrphanedStaging at init drops tables from a crashed scan.
    /// After the refactor it must still sweep per-volume tables.
    func testOrphanedStagingDroppedOnNextInit() throws {
        try store.beginScan(volumeUUID: volA)
        // Simulate crash: close store without merging.
        store = nil
        // Re-open; orphan cleanup runs.
        store = try DuckDBStore(path: testPath)
        // No staging table → new beginScan must be allowed.
        XCTAssertNoThrow(try store.beginScan(volumeUUID: volA))
        _ = try store.mergeAndDiff(volumeUUID: volA)
    }
}
