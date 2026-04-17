//
//  StagingPathBenchmark.swift
//  prismTests
//
//  Benchmark the production ingest path (beginScan → staging → mergeAndDiff)
//  at the same 100k-row scale as testAppenderThroughput. The existing
//  testAppenderThroughput hits the direct-to-files Appender path, which is
//  now slower because of the UNIQUE(volume_uuid, path) index. The production
//  scan path doesn't touch that index until merge, so this is the number
//  that matters for real scans.
//

import XCTest
@testable import prism

final class StagingPathBenchmark: XCTestCase {

    var store: DuckDBStore!
    var testPath: String!

    override func setUp() async throws {
        testPath = FileManager.default.temporaryDirectory
            .appendingPathComponent("StagingBench_\(UUID().uuidString).duckdb").path
        store = try DuckDBStore(path: testPath)
    }

    override func tearDown() async throws {
        store = nil
        try? FileManager.default.removeItem(atPath: testPath)
        try? FileManager.default.removeItem(atPath: testPath + ".wal")
    }

    private func makeFiles(count: Int, volumeUUID: String = "BENCH-VOL") -> [ScannedFile] {
        (0..<count).map { i in
            ScannedFile(
                filename: "file_\(i).mp3",
                parentPath: "/Volumes/Test/Music",
                ext: "mp3",
                sizeBytes: Int64(1024 + i),
                modTimeSec: 1_700_000_000 + i,
                createTimeSec: 1_700_000_000,
                isDirectory: false
            )
        }
    }

    func testFirstScanThroughput100K() throws {
        let files = makeFiles(count: 100_000)

        let beginStart = CFAbsoluteTimeGetCurrent()
        _ = try store.beginScan(volumeUUID: "BENCH-VOL")
        try store.ingestBatch(files, volumeUUID: "BENCH-VOL")
        let ingestTime = CFAbsoluteTimeGetCurrent() - beginStart

        let mergeStart = CFAbsoluteTimeGetCurrent()
        let diff = try store.mergeAndDiff(volumeUUID: "BENCH-VOL")
        let mergeTime = CFAbsoluteTimeGetCurrent() - mergeStart

        let total = ingestTime + mergeTime
        print("""
        ── First-scan path (100K rows) ──
          beginScan + ingestBatch → staging: \(String(format: "%.3f", ingestTime))s (\(String(format: "%.0f", Double(files.count)/ingestTime)) rows/sec)
          mergeAndDiff → files:              \(String(format: "%.3f", mergeTime))s
          Total:                             \(String(format: "%.3f", total))s
          Diff: added=\(diff.added.count) modified=\(diff.modified.count) removed=\(diff.removedIds.count)
        """)

        XCTAssertEqual(diff.added.count, 100_000)
        XCTAssertEqual(try store.getFileCount(), 100_000)
    }

    func testBackToBackScanThroughput100K() throws {
        let files = makeFiles(count: 100_000)

        // First scan — populates files.
        _ = try store.beginScan(volumeUUID: "BENCH-VOL")
        try store.ingestBatch(files, volumeUUID: "BENCH-VOL")
        _ = try store.mergeAndDiff(volumeUUID: "BENCH-VOL")

        // Second scan — everything unchanged. This is the "headline"
        // incremental rescan path.
        let rescanStart = CFAbsoluteTimeGetCurrent()
        _ = try store.beginScan(volumeUUID: "BENCH-VOL")
        try store.ingestBatch(files, volumeUUID: "BENCH-VOL")
        let ingestTime = CFAbsoluteTimeGetCurrent() - rescanStart

        let mergeStart = CFAbsoluteTimeGetCurrent()
        let diff = try store.mergeAndDiff(volumeUUID: "BENCH-VOL")
        let mergeTime = CFAbsoluteTimeGetCurrent() - mergeStart

        let total = ingestTime + mergeTime
        print("""
        ── Back-to-back-scan (100K rows, no changes) ──
          beginScan + ingestBatch → staging: \(String(format: "%.3f", ingestTime))s
          mergeAndDiff → diff.isEmpty:       \(String(format: "%.3f", mergeTime))s
          Total rescan:                      \(String(format: "%.3f", total))s
        """)

        XCTAssertTrue(diff.isEmpty, "Unchanged rescan must produce empty diff")
        XCTAssertLessThan(total, 5.0, "100K-row empty-diff rescan should be under 5s")
    }

    func testPointLookupAfterScan100K() throws {
        let files = makeFiles(count: 100_000)

        _ = try store.beginScan(volumeUUID: "BENCH-VOL")
        try store.ingestBatch(files, volumeUUID: "BENCH-VOL")
        _ = try store.mergeAndDiff(volumeUUID: "BENCH-VOL")

        // Invalidate cache so getFilesByIDs hits the DuckDB path, not the cache.
        store.invalidateCache()

        let allFiles = try store.getAllFiles(limit: 100_000)
        let sampleIDs = Array(allFiles.prefix(1000).map(\.id))

        _ = try store.getFilesByIDs(sampleIDs) // warmup

        var times: [Double] = []
        for _ in 0..<10 {
            let start = CFAbsoluteTimeGetCurrent()
            let results = try store.getFilesByIDs(sampleIDs)
            times.append(CFAbsoluteTimeGetCurrent() - start)
            XCTAssertEqual(results.count, 1000)
        }

        let avg = times.reduce(0, +) / 10.0
        print("DuckDB point lookup after merged scan (1000 IDs from 100K): avg=\(String(format: "%.4f", avg))s")
        XCTAssertLessThan(avg, 0.5)
    }
}
