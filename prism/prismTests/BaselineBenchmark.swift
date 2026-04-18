//
//  BaselineBenchmark.swift
//  prismTests
//
//  Emits a single CSV capturing every metric §5.1 of plan.md cares about.
//  Run with:
//    xcodebuild test -scheme prism -only-testing:prismTests/BaselineBenchmark
//  Then copy /tmp/prism_baseline.csv into bench/ under the right name.
//

import XCTest
@testable import prism

final class BaselineBenchmark: XCTestCase {

    static var csvRows: [String] = ["bench,variant,metric,value,unit,notes"]

    var store: DuckDBStore!
    var testPath: String!

    override func setUp() async throws {
        testPath = FileManager.default.temporaryDirectory
            .appendingPathComponent("Baseline_\(UUID().uuidString).duckdb").path
        store = try DuckDBStore(path: testPath)
    }

    override func tearDown() async throws {
        store = nil
        try? FileManager.default.removeItem(atPath: testPath)
        try? FileManager.default.removeItem(atPath: testPath + ".wal")

        let csv = BaselineBenchmark.csvRows.joined(separator: "\n") + "\n"
        let path = "/tmp/prism_baseline.csv"
        try? csv.write(toFile: path, atomically: true, encoding: .utf8)
    }

    private func row(bench: String, variant: String, metric: String, value: Double, unit: String, notes: String = "") {
        let v = String(format: "%.4f", value)
        BaselineBenchmark.csvRows.append("\(bench),\(variant),\(metric),\(v),\(unit),\(notes)")
        print("[BASELINE] \(bench),\(variant),\(metric)=\(v) \(unit) \(notes)")
    }

    private func makeFiles(count: Int) -> [ScannedFile] {
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

    // MARK: - Appender / staging path (100K rows)

    func testAppenderThroughput() throws {
        let rows = 100_000
        let files = makeFiles(count: rows)

        try store.beginScan(volumeUUID: "BENCH")
        let start = CFAbsoluteTimeGetCurrent()
        try store.ingestBatch(files, volumeUUID: "BENCH")
        let ingestTime = CFAbsoluteTimeGetCurrent() - start

        let mergeStart = CFAbsoluteTimeGetCurrent()
        let diff = try store.mergeAndDiff(volumeUUID: "BENCH")
        let mergeTime = CFAbsoluteTimeGetCurrent() - mergeStart

        row(bench: "appender", variant: "staging-100k", metric: "ingest_s", value: ingestTime, unit: "s")
        row(bench: "appender", variant: "staging-100k", metric: "ingest_rows_per_sec", value: Double(rows) / ingestTime, unit: "rows/s")
        row(bench: "appender", variant: "staging-100k", metric: "merge_s", value: mergeTime, unit: "s")
        XCTAssertEqual(diff.added.count, rows)
    }

    // MARK: - Empty-diff rescan (the "<0.3s post-scan" guarantee)

    func testEmptyRescanPostScan() throws {
        let files = makeFiles(count: 100_000)

        try store.beginScan(volumeUUID: "BENCH")
        try store.ingestBatch(files, volumeUUID: "BENCH")
        _ = try store.mergeAndDiff(volumeUUID: "BENCH")
        try store.loadCache()

        try store.beginScan(volumeUUID: "BENCH")
        try store.ingestBatch(files, volumeUUID: "BENCH")

        let start = CFAbsoluteTimeGetCurrent()
        let diff = try store.mergeAndDiff(volumeUUID: "BENCH")
        try store.applyDiff(diff)
        let postScan = CFAbsoluteTimeGetCurrent() - start

        row(bench: "rescan", variant: "empty-diff-100k", metric: "post_scan_s", value: postScan, unit: "s")
        XCTAssertTrue(diff.isEmpty)
    }

    // MARK: - getFileCount / getFileCountByVolume

    func testCountLatencies() throws {
        let files = makeFiles(count: 100_000)
        try store.beginScan(volumeUUID: "BENCH")
        try store.ingestBatch(files, volumeUUID: "BENCH")
        _ = try store.mergeAndDiff(volumeUUID: "BENCH")

        _ = try store.getFileCount()
        _ = try store.getFileCountByVolume("BENCH")

        let iterations = 100
        var totalCount: Double = 0
        var totalByVol: Double = 0
        for _ in 0..<iterations {
            let t1 = CFAbsoluteTimeGetCurrent()
            _ = try store.getFileCount()
            totalCount += CFAbsoluteTimeGetCurrent() - t1

            let t2 = CFAbsoluteTimeGetCurrent()
            _ = try store.getFileCountByVolume("BENCH")
            totalByVol += CFAbsoluteTimeGetCurrent() - t2
        }
        let avgCountMs = (totalCount / Double(iterations)) * 1000
        let avgByVolMs = (totalByVol / Double(iterations)) * 1000
        row(bench: "reader", variant: "idle", metric: "getFileCount_ms", value: avgCountMs, unit: "ms")
        row(bench: "reader", variant: "idle", metric: "getFileCountByVolume_ms", value: avgByVolMs, unit: "ms")
    }

    // MARK: - Cache-miss getFilesByIDs (TEMP table path)

    func testGetFilesByIDsCacheMiss() throws {
        let files = makeFiles(count: 100_000)
        try store.beginScan(volumeUUID: "BENCH")
        try store.ingestBatch(files, volumeUUID: "BENCH")
        _ = try store.mergeAndDiff(volumeUUID: "BENCH")

        let all = try store.getAllFiles(limit: 100_000)
        let sample = Array(all.prefix(1000).map(\.id))
        store.invalidateCache()

        _ = try store.getFilesByIDs(sample)

        let iterations = 10
        var total: Double = 0
        for _ in 0..<iterations {
            let start = CFAbsoluteTimeGetCurrent()
            let r = try store.getFilesByIDs(sample)
            total += CFAbsoluteTimeGetCurrent() - start
            XCTAssertEqual(r.count, 1000)
        }
        let avgMs = (total / Double(iterations)) * 1000
        row(bench: "reader", variant: "idle", metric: "getFilesByIDs_miss_ms", value: avgMs, unit: "ms", notes: "1000-IDs-from-100K")
    }

    // MARK: - Cache-hit getFilesByIDs (warm search path)

    func testGetFilesByIDsCacheHit() throws {
        let files = makeFiles(count: 100_000)
        try store.beginScan(volumeUUID: "BENCH")
        try store.ingestBatch(files, volumeUUID: "BENCH")
        _ = try store.mergeAndDiff(volumeUUID: "BENCH")
        try store.loadCache()

        let all = try store.getAllFiles(limit: 100_000)
        let sample = Array(all.prefix(1000).map(\.id))

        _ = try store.getFilesByIDs(sample)

        let iterations = 100
        var total: Double = 0
        for _ in 0..<iterations {
            let start = CFAbsoluteTimeGetCurrent()
            _ = try store.getFilesByIDs(sample)
            total += CFAbsoluteTimeGetCurrent() - start
        }
        let avgMs = (total / Double(iterations)) * 1000
        row(bench: "reader", variant: "idle", metric: "getFilesByIDs_hit_ms", value: avgMs, unit: "ms", notes: "1000-IDs-from-100K")
    }

    // MARK: - loadCache cold-start

    func testLoadCache() throws {
        let files = makeFiles(count: 100_000)
        try store.beginScan(volumeUUID: "BENCH")
        try store.ingestBatch(files, volumeUUID: "BENCH")
        _ = try store.mergeAndDiff(volumeUUID: "BENCH")

        let start = CFAbsoluteTimeGetCurrent()
        try store.loadCache()
        let elapsed = CFAbsoluteTimeGetCurrent() - start
        row(bench: "cache", variant: "100k", metric: "loadCache_s", value: elapsed, unit: "s")
    }
}
