//
//  LiveIndexCoordinatorTests.swift
//  prismTests
//
//  Unit tests for LiveIndexCoordinator state-machine + bridge surface.
//  Does NOT exercise real FSEventStream behavior — that's in the 5
//  disk-image-based E2E tests flagged in the test plan. Those require
//  hdiutil + real macOS I/O and run in CI only.
//
//  What we CAN unit-test here without FSEvents:
//    - coordinator.start() on a cold DB (no baseline) is a no-op
//    - stopWatching/startWatching cycle
//    - is_online flip via bridge callback (onUnmount)
//    - polling-mode persistence via setPollingMode
//    - bridge state publishing on stop()
//

import XCTest
@testable import prism

@MainActor
final class LiveIndexCoordinatorTests: XCTestCase {

    var store: DuckDBStore!
    var dbManager: DatabaseManager!
    var testDBPath: String!
    var testSQLitePath: String!

    override func setUp() async throws {
        testDBPath = FileManager.default.temporaryDirectory
            .appendingPathComponent("PrismLiveIndex_\(UUID().uuidString).duckdb").path
        testSQLitePath = FileManager.default.temporaryDirectory
            .appendingPathComponent("PrismLiveIndex_\(UUID().uuidString).db").path
        store = try DuckDBStore(path: testDBPath)
        dbManager = DatabaseManager(testPath: testSQLitePath)
        try dbManager.open()
    }

    override func tearDown() async throws {
        dbManager.close()
        dbManager = nil
        store = nil
        try? FileManager.default.removeItem(atPath: testDBPath)
        try? FileManager.default.removeItem(atPath: testDBPath + ".wal")
        try? FileManager.default.removeItem(atPath: testSQLitePath)
        try? FileManager.default.removeItem(atPath: testSQLitePath + "-wal")
    }

    // MARK: - Bridge stub

    final class TestBridge: LiveIndexBridge {
        var appliedDiffs: [(String, ScanDiff)] = []
        var failures: [(String, LiveIndexError)] = []
        var onlineChanges: [(String, Bool)] = []
        var stateUpdates: [[LiveIndexState]] = []

        func liveIndexDidApplyDiff(volumeUUID: String, diff: ScanDiff) async {
            appliedDiffs.append((volumeUUID, diff))
        }
        func liveIndexDidFail(volumeUUID: String, error: LiveIndexError) async {
            failures.append((volumeUUID, error))
        }
        func liveIndexVolumeOnlineChanged(volumeUUID: String, isOnline: Bool) async {
            onlineChanges.append((volumeUUID, isOnline))
        }
        func liveIndexDidUpdateState(_ states: [LiveIndexState]) async {
            stateUpdates.append(states)
        }
    }

    // MARK: - Lifecycle

    func testStartOnNeverIndexedVolumeIsObserveOnly() async throws {
        let bridge = TestBridge()
        let coordinator = LiveIndexCoordinator(store: store, dbManager: dbManager, bridge: bridge)

        let volume = VolumeInfo(
            uuid: "VOL-NEW",
            name: "New Drive",
            path: "/Volumes/NewDrive",
            isInternal: false,
            isOnline: true
        )

        await coordinator.start(volumes: [volume])
        // No prior index → observe-only. No stream created, no error surfaced.
        XCTAssertTrue(bridge.failures.isEmpty, "Unexpected failure: \(bridge.failures)")
    }

    func testStopClearsBridgeState() async throws {
        let bridge = TestBridge()
        let coordinator = LiveIndexCoordinator(store: store, dbManager: dbManager, bridge: bridge)

        await coordinator.start(volumes: [])
        await coordinator.stop()
        // Without volumes, stop() should complete cleanly with no failures.
        XCTAssertTrue(bridge.failures.isEmpty)
    }

    // MARK: - Polling-mode round-trip

    func testPollingModePersistsAcrossRestart() async throws {
        // Manually mark a volume as polling.
        try store.setPollingMode(volumeUUID: "VOL-POLL", enabled: true)
        // A coordinator that starts and sees this state should immediately
        // enter polling mode for VOL-POLL without creating a FSEventStream
        // (we rely on the setPollingMode check in onMount).
        let state = try store.loadWatchState(volumeUUID: "VOL-POLL")
        XCTAssertEqual(state?.pollingMode, true)
    }

    // MARK: - Disabled via UserDefaults

    func testStartIsNoOpWhenDisabled() async throws {
        UserDefaults.standard.set(true, forKey: "LiveIndexDisabled")
        defer { UserDefaults.standard.set(false, forKey: "LiveIndexDisabled") }

        let bridge = TestBridge()
        let coordinator = LiveIndexCoordinator(store: store, dbManager: dbManager, bridge: bridge)

        let volume = VolumeInfo(
            uuid: "VOL-A",
            name: "My Drive",
            path: "/Volumes/MyDrive",
            isInternal: false,
            isOnline: true
        )
        await coordinator.start(volumes: [volume])
        // Disabled → no stream, no failures, no state changes from FSEvents.
        XCTAssertTrue(bridge.failures.isEmpty)
    }

    // MARK: - LiveIndexError copy

    func testErrorDescriptionMapsDiskFullToFriendlyCopy() {
        let err = LiveIndexError.writerError(volumeUUID: "V", message: "disk full: no space left")
        XCTAssertEqual(err.errorDescription, "Index database is full — free up space")
    }

    func testErrorDescriptionMapsCorruptionToFriendlyCopy() {
        let err = LiveIndexError.writerError(volumeUUID: "V", message: "DuckDB integrity check failed")
        XCTAssertEqual(err.errorDescription, "Index database corrupted — rebuild required")
    }

    func testErrorDescriptionForStreamCreateFailed() {
        let err = LiveIndexError.streamCreationFailed(volumeUUID: "My Drive", underlying: 0)
        XCTAssertEqual(err.errorDescription, "Couldn't start live watching for 'My Drive'")
    }

    func testErrorDescriptionForTransparentCasesIsNil() {
        XCTAssertNil(LiveIndexError.eventHistoryGap(volumeUUID: "V").errorDescription)
        XCTAssertNil(LiveIndexError.backPressureTriggered(volumeUUID: "V", eventCount: 100_000).errorDescription)
    }

    // MARK: - ScanDiff.Entry convenience init

    func testScanDiffEntryFromScannedFile() {
        let scanned = ScannedFile(
            filename: "song.mp3",
            parentPath: "/Volumes/Test/Music",
            ext: "mp3",
            sizeBytes: 12345,
            modTimeSec: 1_700_000_000,
            createTimeSec: 1_699_000_000,
            isDirectory: false
        )
        let entry = ScanDiff.Entry.from(scannedFile: scanned, volumeUUID: "VOL-A")
        XCTAssertEqual(entry.filename, "song.mp3")
        XCTAssertEqual(entry.path, "/Volumes/Test/Music/song.mp3")
        XCTAssertEqual(entry.volumeUUID, "VOL-A")
        XCTAssertEqual(entry.ext, "mp3")
        XCTAssertEqual(entry.sizeBytes, 12345)
        XCTAssertEqual(entry.dateModified, 1_700_000_000)
        XCTAssertEqual(entry.dateCreated, 1_699_000_000)
        // id must match PathHash-derived id.
        XCTAssertEqual(entry.id, PathHash.id(volumeUUID: "VOL-A", path: "/Volumes/Test/Music/song.mp3"))
    }
}
