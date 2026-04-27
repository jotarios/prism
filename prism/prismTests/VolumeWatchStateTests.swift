//
//  VolumeWatchStateTests.swift
//  prismTests
//
//  Covers `volume_watch_state` table: event-id persistence, polling-mode
//  flag, maxDateModified heuristic input.
//

import XCTest
import DuckDB
@testable import prism

final class VolumeWatchStateTests: XCTestCase {

    var store: DuckDBStore!
    var testPath: String!
    let volA = "VOL-A"

    override func setUp() async throws {
        testPath = FileManager.default.temporaryDirectory
            .appendingPathComponent("PrismWatchState_\(UUID().uuidString).duckdb").path
        store = try DuckDBStore(path: testPath)
    }

    override func tearDown() async throws {
        store = nil
        try? FileManager.default.removeItem(atPath: testPath)
        try? FileManager.default.removeItem(atPath: testPath + ".wal")
    }

    // MARK: - Table lifecycle

    func testLoadReturnsNilForUnknownVolume() throws {
        let state = try store.loadWatchState(volumeUUID: volA)
        XCTAssertNil(state)
    }

    func testPersistAndLoadRoundtrip() throws {
        try store.persistEventId(volumeUUID: volA, lastEventId: 12345, reason: "test")
        let state = try store.loadWatchState(volumeUUID: volA)
        XCTAssertEqual(state?.lastEventId, 12345)
        XCTAssertEqual(state?.pollingMode, false)
    }

    func testPersistUpsertsRatherThanInserts() throws {
        try store.persistEventId(volumeUUID: volA, lastEventId: 100)
        try store.persistEventId(volumeUUID: volA, lastEventId: 200)
        let state = try store.loadWatchState(volumeUUID: volA)
        XCTAssertEqual(state?.lastEventId, 200)
    }

    // MARK: - Polling mode

    func testSetPollingModeCreatesRowIfMissing() throws {
        try store.setPollingMode(volumeUUID: volA, enabled: true)
        let state = try store.loadWatchState(volumeUUID: volA)
        XCTAssertEqual(state?.pollingMode, true)
        XCTAssertEqual(state?.lastEventId, 0) // never had an event yet
    }

    func testPersistEventIdPreservesPollingModeFlag() throws {
        // Enable polling first.
        try store.setPollingMode(volumeUUID: volA, enabled: true)
        // Persist a new event-id — should NOT clobber polling_mode.
        try store.persistEventId(volumeUUID: volA, lastEventId: 999)
        let state = try store.loadWatchState(volumeUUID: volA)
        XCTAssertEqual(state?.lastEventId, 999)
        XCTAssertEqual(state?.pollingMode, true)
    }

    // MARK: - maxDateModified heuristic input

    func testMaxDateModifiedReturnsNilWhenEmpty() throws {
        let max = try store.maxDateModified(volumeUUID: volA)
        XCTAssertNil(max)
    }

    func testMaxDateModifiedReflectsInsertedRows() throws {
        let entry1 = ScanDiff.Entry(
            id: PathHash.id(volumeUUID: volA, path: "/Volumes/Test/a.mp3"),
            filename: "a.mp3",
            path: "/Volumes/Test/a.mp3",
            volumeUUID: volA,
            ext: "mp3",
            sizeBytes: 1024,
            dateModified: 1_700_000_000,
            dateCreated: 1_700_000_000
        )
        let entry2 = ScanDiff.Entry(
            id: PathHash.id(volumeUUID: volA, path: "/Volumes/Test/b.mp3"),
            filename: "b.mp3",
            path: "/Volumes/Test/b.mp3",
            volumeUUID: volA,
            ext: "mp3",
            sizeBytes: 1024,
            dateModified: 1_750_000_000,  // higher
            dateCreated: 1_700_000_000
        )
        try store.applyDirectDiff(
            ScanDiff(added: [entry1, entry2], modified: [], removedIds: []),
            volumeUUID: volA
        )
        let max = try store.maxDateModified(volumeUUID: volA)
        XCTAssertEqual(max, 1_750_000_000)
    }
}
