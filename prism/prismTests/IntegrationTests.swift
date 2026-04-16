//
//  IntegrationTests.swift
//  prismTests
//

import XCTest
@testable import prism

final class IntegrationTests: XCTestCase {

    var testDirectory: URL!
    var duckDBStore: DuckDBStore!
    var dbManager: DatabaseManager!
    var duckDBPath: String!

    override func setUp() async throws {
        let id = UUID().uuidString
        testDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent("PrismIntegration_\(id)")
        try FileManager.default.createDirectory(at: testDirectory, withIntermediateDirectories: true)

        duckDBPath = FileManager.default.temporaryDirectory
            .appendingPathComponent("PrismIntegration_\(id).duckdb").path
        duckDBStore = try DuckDBStore(path: duckDBPath)

        dbManager = DatabaseManager.shared
        do { try dbManager.open() } catch { }
        try dbManager.rebuildDatabase()
    }

    override func tearDown() async throws {
        dbManager.close()
        duckDBStore = nil
        try? FileManager.default.removeItem(at: testDirectory)
        try? FileManager.default.removeItem(atPath: duckDBPath)
        try? FileManager.default.removeItem(atPath: duckDBPath + ".wal")
    }

    private func createLargeTree() throws {
        let exts = ["mp3", "wav", "flac", "aac", "m4a", "ogg", "aiff"]
        let nonAudio = ["jpg", "png", "txt", "pdf"]

        for d in 0..<10 {
            let dir = testDirectory.appendingPathComponent("dir_\(d)")
            try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)

            for f in 0..<100 {
                let ext = exts[f % exts.count]
                try "data".write(to: dir.appendingPathComponent("audio_\(f).\(ext)"), atomically: true, encoding: .utf8)
            }
            for f in 0..<20 {
                let ext = nonAudio[f % nonAudio.count]
                try "data".write(to: dir.appendingPathComponent("other_\(f).\(ext)"), atomically: true, encoding: .utf8)
            }

            let sub = dir.appendingPathComponent("sub")
            try FileManager.default.createDirectory(at: sub, withIntermediateDirectories: true)
            for f in 0..<50 {
                try "data".write(to: sub.appendingPathComponent("track_\(f).mp3"), atomically: true, encoding: .utf8)
            }
        }
    }

    func testFullPipeline() async throws {
        try createLargeTree()
        // 10 dirs * (100 audio + 50 sub-audio) = 1500 audio files

        let volumeUUID = VolumeManager.shared.getVolumeUUID(for: testDirectory.path) ?? "TEST"

        let coordinator = ParallelScanCoordinator(
            rootPath: testDirectory.path,
            volumeUUID: volumeUUID,
            maxConcurrency: 8
        )

        let totalFiles = try await coordinator.scanStreaming(into: duckDBStore) { count, _ in }

        XCTAssertEqual(totalFiles, 1500, "Should find 1500 audio files")

        let duckDBCount = try duckDBStore.getFileCount()
        XCTAssertEqual(duckDBCount, 1500)

        try dbManager.rebuildSearchIndex(from: duckDBStore)

        let sqliteCount = try await dbManager.getFileCount()
        XCTAssertEqual(sqliteCount, duckDBCount, "SQLite and DuckDB counts must match")

        let ids = try await dbManager.searchFileIDs(query: "audio", limit: 100)
        XCTAssertGreaterThan(ids.count, 0, "Search should return results")

        let results = try duckDBStore.getFilesByIDs(ids)
        XCTAssertEqual(results.count, ids.count, "DuckDB should return metadata for all IDs")

        for result in results {
            XCTAssertFalse(result.filename.isEmpty)
            XCTAssertFalse(result.path.isEmpty)
            XCTAssertGreaterThan(result.sizeBytes, 0)
        }
    }

    /// REGRESSION GUARD: rescanning an unchanged tree must be O(0) —
    /// produces an empty diff and syncs in well under the per-scan budget.
    /// This is the test that fails loudly if anyone accidentally reverts
    /// the incremental path to a full rebuild.
    func testBackToBackScanProducesEmptyDiff() async throws {
        try createLargeTree()

        let volumeUUID = VolumeManager.shared.getVolumeUUID(for: testDirectory.path) ?? "TEST"

        // First scan — full cost, populates DuckDB + SQLite.
        _ = try duckDBStore.beginScan(volumeUUID: volumeUUID)
        let coord1 = ParallelScanCoordinator(
            rootPath: testDirectory.path,
            volumeUUID: volumeUUID,
            maxConcurrency: 8
        )
        _ = try await coord1.scanStreaming(into: duckDBStore) { _, _ in }
        let diff1 = try duckDBStore.mergeAndDiff(volumeUUID: volumeUUID)
        try dbManager.syncSearchIndex(from: duckDBStore, volumeUUID: volumeUUID, diff: diff1)
        XCTAssertEqual(diff1.added.count, 1500)
        XCTAssertEqual(diff1.modified.count, 0)
        XCTAssertEqual(diff1.removedIds.count, 0)

        // Capture ids so we can prove stability.
        let firstRowIds = try duckDBStore.getAllFiles(limit: 1500).map(\.id).sorted()

        // Second scan — no filesystem changes.
        _ = try duckDBStore.beginScan(volumeUUID: volumeUUID)
        let coord2 = ParallelScanCoordinator(
            rootPath: testDirectory.path,
            volumeUUID: volumeUUID,
            maxConcurrency: 8
        )
        _ = try await coord2.scanStreaming(into: duckDBStore) { _, _ in }

        let mergeStart = CFAbsoluteTimeGetCurrent()
        let diff2 = try duckDBStore.mergeAndDiff(volumeUUID: volumeUUID)
        let mergeTime = CFAbsoluteTimeGetCurrent() - mergeStart

        XCTAssertTrue(diff2.isEmpty, "Unchanged rescan must produce empty diff (added=\(diff2.added.count) modified=\(diff2.modified.count) removed=\(diff2.removedIds.count))")

        let syncStart = CFAbsoluteTimeGetCurrent()
        try dbManager.syncSearchIndex(from: duckDBStore, volumeUUID: volumeUUID, diff: diff2)
        let syncTime = CFAbsoluteTimeGetCurrent() - syncStart

        XCTAssertLessThan(syncTime, 0.1, "Empty-diff sync should be near-instant, was \(syncTime)s")
        XCTAssertLessThan(mergeTime, 1.0, "Merge at 1500 rows should be well under a second, was \(mergeTime)s")

        // ID stability: same ids, same order.
        let secondRowIds = try duckDBStore.getAllFiles(limit: 1500).map(\.id).sorted()
        XCTAssertEqual(firstRowIds, secondRowIds, "row ids must be byte-for-byte stable across unchanged rescan")
    }

    func testFullPipelineBenchmark() async throws {
        try createLargeTree()

        let volumeUUID = VolumeManager.shared.getVolumeUUID(for: testDirectory.path) ?? "TEST"

        let scanStart = CFAbsoluteTimeGetCurrent()
        let coordinator = ParallelScanCoordinator(
            rootPath: testDirectory.path,
            volumeUUID: volumeUUID,
            maxConcurrency: 8
        )
        let totalFiles = try await coordinator.scanStreaming(into: duckDBStore) { _, _ in }
        let scanTime = CFAbsoluteTimeGetCurrent() - scanStart

        let syncStart = CFAbsoluteTimeGetCurrent()
        try dbManager.rebuildSearchIndex(from: duckDBStore)
        let syncTime = CFAbsoluteTimeGetCurrent() - syncStart

        let searchStart = CFAbsoluteTimeGetCurrent()
        let ids = try await dbManager.searchFileIDs(query: "track", limit: 1000)
        let _ = try duckDBStore.getFilesByIDs(ids)
        let searchTime = CFAbsoluteTimeGetCurrent() - searchStart

        let totalTime = scanTime + syncTime

        let output = """
        ── Full Pipeline Benchmark (1500 files) ──
          Scan (getattrlistbulk parallel → DuckDB): \(String(format: "%.3f", scanTime))s
          Sync (DuckDB → SQLite FTS5): \(String(format: "%.3f", syncTime))s
          Total ingestion: \(String(format: "%.3f", totalTime))s
          Search (FTS5 → DuckDB lookup): \(String(format: "%.3f", searchTime))s
          Files: \(totalFiles)
          Search results: \(ids.count)
          Throughput: \(String(format: "%.0f", Double(totalFiles) / totalTime)) files/sec
        """
        print(output)
        try? output.write(toFile: "/tmp/prism_benchmark_pipeline.txt", atomically: true, encoding: .utf8)

        XCTAssertLessThan(totalTime, 5.0, "Full pipeline should complete in under 5 seconds for 1500 files")
        XCTAssertLessThan(searchTime, 0.1, "Search should be under 100ms")
    }
}
