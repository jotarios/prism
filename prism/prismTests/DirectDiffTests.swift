//
//  DirectDiffTests.swift
//  prismTests
//
//  Covers `DuckDBStore.applyDirectDiff` — the FSEvents hot path. Idempotency,
//  chunking, empty diffs, and pendingBatches queue/drain semantics.
//

import XCTest
import DuckDB
@testable import prism

final class DirectDiffTests: XCTestCase {

    var store: DuckDBStore!
    var testPath: String!
    let volA = "VOL-A"
    let volB = "VOL-B"

    override func setUp() async throws {
        testPath = FileManager.default.temporaryDirectory
            .appendingPathComponent("PrismDirectDiff_\(UUID().uuidString).duckdb").path
        store = try DuckDBStore(path: testPath)
    }

    override func tearDown() async throws {
        store = nil
        try? FileManager.default.removeItem(atPath: testPath)
        try? FileManager.default.removeItem(atPath: testPath + ".wal")
    }

    // MARK: - Helpers

    private func entry(_ name: String,
                       volume: String = "VOL-A",
                       size: Int64 = 1024,
                       mtime: Int64 = 1_700_000_000) -> ScanDiff.Entry {
        let path = "/Volumes/Test/" + name
        return ScanDiff.Entry(
            id: PathHash.id(volumeUUID: volume, path: path),
            filename: name,
            path: path,
            volumeUUID: volume,
            ext: "mp3",
            sizeBytes: size,
            dateModified: mtime,
            dateCreated: mtime
        )
    }

    // MARK: - Empty & basic

    func testEmptyDiffIsNoOp() throws {
        try store.applyDirectDiff(ScanDiff.empty, volumeUUID: volA)
        XCTAssertEqual(try store.getFileCount(), 0)
    }

    func testAddedRowsPersist() throws {
        let diff = ScanDiff(added: [entry("a.mp3"), entry("b.mp3")], modified: [], removedIds: [])
        try store.applyDirectDiff(diff, volumeUUID: volA)
        XCTAssertEqual(try store.getFileCount(), 2)
    }

    func testRemovedRowsDisappear() throws {
        let added = ScanDiff(added: [entry("a.mp3"), entry("b.mp3")], modified: [], removedIds: [])
        try store.applyDirectDiff(added, volumeUUID: volA)
        let removed = ScanDiff(added: [], modified: [], removedIds: [entry("a.mp3").id])
        try store.applyDirectDiff(removed, volumeUUID: volA)
        XCTAssertEqual(try store.getFileCount(), 1)
    }

    // MARK: - Idempotency (CRITICAL per /plan-eng-review)

    func testApplyingSameDiffTwiceIsNoOp() throws {
        let diff = ScanDiff(added: [entry("a.mp3")], modified: [], removedIds: [])
        try store.applyDirectDiff(diff, volumeUUID: volA)
        try store.applyDirectDiff(diff, volumeUUID: volA)
        XCTAssertEqual(try store.getFileCount(), 1)
    }

    // MARK: - Chunking: >1k rows

    func testLargeAddedDiffChunks() throws {
        let rows = (0..<2500).map { entry("f\($0).mp3") }
        let diff = ScanDiff(added: rows, modified: [], removedIds: [])
        try store.applyDirectDiff(diff, volumeUUID: volA)
        XCTAssertEqual(try store.getFileCount(), 2500)
    }

    // MARK: - pendingBatches queue

    func testEnqueueAndDrainPendingBatches() throws {
        // Direct queue manipulation — exercise the API that
        // LiveIndexCoordinator uses to defer FSEvents batches that arrive
        // during a full scan.
        let diff1 = ScanDiff(added: [entry("a.mp3")], modified: [], removedIds: [])
        let diff2 = ScanDiff(added: [entry("b.mp3")], modified: [], removedIds: [])
        store.enqueuePendingBatch(volumeUUID: volA, diff: diff1)
        store.enqueuePendingBatch(volumeUUID: volA, diff: diff2)
        let drained = store.drainPendingBatches(volumeUUID: volA)
        XCTAssertEqual(drained.count, 2)
        // Second drain should be empty.
        XCTAssertEqual(store.drainPendingBatches(volumeUUID: volA).count, 0)
    }

    func testPendingBatchesScopedByVolume() throws {
        let diffA = ScanDiff(added: [entry("a.mp3", volume: volA)], modified: [], removedIds: [])
        let diffB = ScanDiff(added: [entry("b.mp3", volume: volB)], modified: [], removedIds: [])
        store.enqueuePendingBatch(volumeUUID: volA, diff: diffA)
        store.enqueuePendingBatch(volumeUUID: volB, diff: diffB)
        XCTAssertEqual(store.drainPendingBatches(volumeUUID: volA).count, 1)
        XCTAssertEqual(store.drainPendingBatches(volumeUUID: volB).count, 1)
    }

    // MARK: - isScanning gate

    func testIsScanningReflectsSlotState() throws {
        XCTAssertFalse(store.isScanning(volumeUUID: volA))
        try store.beginScan(volumeUUID: volA)
        XCTAssertTrue(store.isScanning(volumeUUID: volA))
        _ = try store.mergeAndDiff(volumeUUID: volA)
        XCTAssertFalse(store.isScanning(volumeUUID: volA))
    }

    // MARK: - setVolumeOnline toggles is_online

    func testSetVolumeOnlineFlipsRowFlag() throws {
        try store.applyDirectDiff(
            ScanDiff(added: [entry("a.mp3")], modified: [], removedIds: []),
            volumeUUID: volA
        )
        try store.loadCache()
        XCTAssertEqual(store.getAllCachedValues().first?.isOnline, true)
        try store.setVolumeOnline(volA, isOnline: false)
        XCTAssertEqual(store.getAllCachedValues().first?.isOnline, false)
    }
}
