//
//  MultiConnectionBenchmark.swift
//  prismTests
//
//  Search-while-scanning benchmarks. Run with:
//    xcodebuild test -scheme prism -only-testing:prismTests/MultiConnectionBenchmark
//  CSV lands at /tmp/prism_multiconn.csv.
//

import XCTest
import DuckDB
@testable import prism

final class MultiConnectionBenchmark: XCTestCase {

    static var csvRows: [String] = ["bench,readerCount,metric,value,unit,notes"]

    override func tearDown() async throws {
        let csv = MultiConnectionBenchmark.csvRows.joined(separator: "\n") + "\n"
        let path = "/tmp/prism_multiconn.csv"
        try? csv.write(toFile: path, atomically: true, encoding: .utf8)
    }

    private func row(bench: String, readerCount: Int, metric: String, value: Double, unit: String, notes: String = "") {
        let v = String(format: "%.4f", value)
        MultiConnectionBenchmark.csvRows.append("\(bench),\(readerCount),\(metric),\(v),\(unit),\(notes)")
        print("[MULTICONN] \(bench),N=\(readerCount),\(metric)=\(v) \(unit) \(notes)")
    }

    private func percentiles(_ xs: [Double]) -> (p50: Double, p99: Double) {
        guard !xs.isEmpty else { return (0, 0) }
        let sorted = xs.sorted()
        let p50 = sorted[sorted.count / 2]
        let p99Idx = min(sorted.count - 1, Int(Double(sorted.count) * 0.99))
        let p99 = sorted[p99Idx]
        return (p50, p99)
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

    private func makeStore(readerCount: Int) throws -> (DuckDBStore, String) {
        let path = FileManager.default.temporaryDirectory
            .appendingPathComponent("MultiBench_\(UUID().uuidString).duckdb").path
        let store = try DuckDBStore(path: path, readerCount: readerCount)
        return (store, path)
    }

    private func cleanup(_ path: String) {
        try? FileManager.default.removeItem(atPath: path)
        try? FileManager.default.removeItem(atPath: path + ".wal")
    }

    // MARK: - Search-during-scan: idle baseline, then under write pressure

    private func prefill(_ store: DuckDBStore, rows: Int) throws {
        let files = makeFiles(count: rows)
        try store.beginScan(volumeUUID: "V")
        try store.ingestBatch(files, volumeUUID: "V")
        _ = try store.mergeAndDiff(volumeUUID: "V")
    }

    /// Timed-loop workload: while `isRunning` is true, run `getFileCount()`
    /// and `getFilesByIDs(cache-miss)` as fast as possible; collect latency
    /// samples. Returns combined samples (ms).
    private func runSearchWorkload(_ store: DuckDBStore, sampleIDs: [Int64], duration: Double) -> [Double] {
        var samples: [Double] = []
        samples.reserveCapacity(10_000)
        let start = CFAbsoluteTimeGetCurrent()
        while CFAbsoluteTimeGetCurrent() - start < duration {
            let t1 = CFAbsoluteTimeGetCurrent()
            _ = (try? store.getFileCount()) ?? -1
            samples.append((CFAbsoluteTimeGetCurrent() - t1) * 1000)

            let t2 = CFAbsoluteTimeGetCurrent()
            _ = (try? store.getFilesByIDs(sampleIDs)) ?? []
            samples.append((CFAbsoluteTimeGetCurrent() - t2) * 1000)
        }
        return samples
    }

    /// Runs the search-during-scan bench for a given `readerCount`. Emits
    /// baseline p50/p99 (solo) and under-scan p50/p99 + overhead_factor.
    private func runBench(readerCount: Int, label: String) throws {
        let (store, path) = try makeStore(readerCount: readerCount)
        defer { cleanup(path) }

        // 50K rows committed; another 50K to be ingested during the bench.
        try prefill(store, rows: 50_000)
        store.invalidateCache()   // forces getFilesByIDs onto the cache-miss reader path

        let all = try store.getAllFiles(limit: 50_000)
        let sampleIDs = Array(all.prefix(200).map(\.id))

        // --- Baseline: solo search workload, no scan in flight.
        _ = runSearchWorkload(store, sampleIDs: sampleIDs, duration: 0.3) // warmup
        let baseline = runSearchWorkload(store, sampleIDs: sampleIDs, duration: 2.0)
        let (baseP50, baseP99) = percentiles(baseline)

        // --- Under scan: start a background scan that writes 50K more rows,
        //     then time the same workload concurrently.
        let scanFiles = (50_000..<100_000).map { i in
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

        let scanDoneExpectation = XCTestExpectation(description: "scan done")
        DispatchQueue.global().async { [store] in
            do {
                try store.beginScan(volumeUUID: "V2")
                // Ingest in 5K chunks to keep the writer queue busy throughout.
                for chunk in stride(from: 0, to: scanFiles.count, by: 5_000) {
                    let end = min(chunk + 5_000, scanFiles.count)
                    try store.ingestBatch(Array(scanFiles[chunk..<end]), volumeUUID: "V2")
                }
                _ = try store.mergeAndDiff(volumeUUID: "V2")
            } catch {
                XCTFail("scan failed: \(error)")
            }
            scanDoneExpectation.fulfill()
        }

        // Tiny delay so the scan gets going before we start measuring.
        usleep(50_000)
        let underScan = runSearchWorkload(store, sampleIDs: sampleIDs, duration: 2.0)
        wait(for: [scanDoneExpectation], timeout: 60)
        let (scanP50, scanP99) = percentiles(underScan)

        let overheadFactor = scanP99 / max(baseP99, 0.001)

        row(bench: label, readerCount: readerCount, metric: "baseline_p50_ms", value: baseP50, unit: "ms")
        row(bench: label, readerCount: readerCount, metric: "baseline_p99_ms", value: baseP99, unit: "ms")
        row(bench: label, readerCount: readerCount, metric: "during_scan_p50_ms", value: scanP50, unit: "ms")
        row(bench: label, readerCount: readerCount, metric: "during_scan_p99_ms", value: scanP99, unit: "ms")
        row(bench: label, readerCount: readerCount, metric: "overhead_factor", value: overheadFactor, unit: "×")
        row(bench: label, readerCount: readerCount, metric: "baseline_ops", value: Double(baseline.count), unit: "ops")
        row(bench: label, readerCount: readerCount, metric: "during_scan_ops", value: Double(underScan.count), unit: "ops")
    }

    func testSearchDuringScanDefault() throws {
        try runBench(readerCount: DuckDBStore.defaultReaderCount, label: "SearchDuringScan")
    }

    /// Hot-path variant: cache is loaded, so `getFilesByIDs` returns from
    /// the dictionary without touching DuckDB. This matches the production
    /// search path and produces ~10 000× more samples than the cache-miss
    /// variant, so p50/p99 are statistically meaningful.
    func testSearchDuringScanHotPath() throws {
        let (store, path) = try makeStore(readerCount: DuckDBStore.defaultReaderCount)
        defer { cleanup(path) }
        try prefill(store, rows: 50_000)
        try store.loadCache()   // cache-hit path — the hot one

        let all = try store.getAllFiles(limit: 50_000)
        let sampleIDs = Array(all.prefix(50).map(\.id))   // typical FTS5 result size

        _ = runSearchWorkload(store, sampleIDs: sampleIDs, duration: 0.5)
        let baseline = runSearchWorkload(store, sampleIDs: sampleIDs, duration: 2.0)
        let (baseP50, baseP99) = percentiles(baseline)

        let scanFiles = (50_000..<100_000).map { i in
            ScannedFile(
                filename: "file_\(i).mp3", parentPath: "/Volumes/Test/Music",
                ext: "mp3", sizeBytes: Int64(1024 + i),
                modTimeSec: 1_700_000_000 + i, createTimeSec: 1_700_000_000,
                isDirectory: false
            )
        }

        let scanDone = XCTestExpectation(description: "scan done")
        DispatchQueue.global().async { [store] in
            do {
                try store.beginScan(volumeUUID: "V2")
                for chunk in stride(from: 0, to: scanFiles.count, by: 5_000) {
                    let end = min(chunk + 5_000, scanFiles.count)
                    try store.ingestBatch(Array(scanFiles[chunk..<end]), volumeUUID: "V2")
                }
                _ = try store.mergeAndDiff(volumeUUID: "V2")
            } catch {
                XCTFail("scan failed: \(error)")
            }
            scanDone.fulfill()
        }
        usleep(50_000)

        let underScan = runSearchWorkload(store, sampleIDs: sampleIDs, duration: 2.0)
        wait(for: [scanDone], timeout: 60)
        let (scanP50, scanP99) = percentiles(underScan)

        row(bench: "SearchDuringScanHot", readerCount: DuckDBStore.defaultReaderCount, metric: "baseline_p50_ms", value: baseP50, unit: "ms")
        row(bench: "SearchDuringScanHot", readerCount: DuckDBStore.defaultReaderCount, metric: "baseline_p99_ms", value: baseP99, unit: "ms")
        row(bench: "SearchDuringScanHot", readerCount: DuckDBStore.defaultReaderCount, metric: "during_scan_p50_ms", value: scanP50, unit: "ms")
        row(bench: "SearchDuringScanHot", readerCount: DuckDBStore.defaultReaderCount, metric: "during_scan_p99_ms", value: scanP99, unit: "ms")
        row(bench: "SearchDuringScanHot", readerCount: DuckDBStore.defaultReaderCount, metric: "overhead_factor", value: scanP99 / max(baseP99, 0.001), unit: "×")
        row(bench: "SearchDuringScanHot", readerCount: DuckDBStore.defaultReaderCount, metric: "baseline_ops", value: Double(baseline.count), unit: "ops")
        row(bench: "SearchDuringScanHot", readerCount: DuckDBStore.defaultReaderCount, metric: "during_scan_ops", value: Double(underScan.count), unit: "ops")
    }

    // MARK: - Pool-size sweep

    func testReaderPoolSizeSweep() throws {
        for n in [1, 2, 3, 4, 5, 8] {
            try runBench(readerCount: n, label: "ReaderPoolSize")
        }
    }

    // MARK: - Temp-table isolation (readers using CREATE OR REPLACE TEMP TABLE)

    func testTempTableContention() throws {
        let (store, path) = try makeStore(readerCount: 3)
        defer { cleanup(path) }

        try prefill(store, rows: 50_000)
        store.invalidateCache()

        let all = try store.getAllFiles(limit: 50_000)
        let sampleIDs = Array(all.prefix(200).map(\.id))

        // Serial baseline.
        let serialStart = CFAbsoluteTimeGetCurrent()
        for _ in 0..<50 {
            _ = try store.getFilesByIDs(sampleIDs)
        }
        let serialElapsed = CFAbsoluteTimeGetCurrent() - serialStart

        // Concurrent: 4 tasks each doing 50 iterations.
        let concurrentStart = CFAbsoluteTimeGetCurrent()
        let group = DispatchGroup()
        var errors: [Error] = []
        let errorsLock = NSLock()
        for _ in 0..<4 {
            group.enter()
            DispatchQueue.global().async {
                do {
                    for _ in 0..<50 {
                        _ = try store.getFilesByIDs(sampleIDs)
                    }
                } catch {
                    errorsLock.lock()
                    errors.append(error)
                    errorsLock.unlock()
                }
                group.leave()
            }
        }
        group.wait()
        let concurrentElapsed = CFAbsoluteTimeGetCurrent() - concurrentStart

        XCTAssertTrue(errors.isEmpty, "concurrent TEMP table reads must not fail: \(errors)")

        let serialOpsPerSec = 50.0 / serialElapsed
        let concurrentOpsPerSec = 200.0 / concurrentElapsed
        let speedup = concurrentOpsPerSec / serialOpsPerSec

        row(bench: "TempTableContention", readerCount: 3, metric: "serial_ops_per_sec", value: serialOpsPerSec, unit: "ops/s")
        row(bench: "TempTableContention", readerCount: 3, metric: "concurrent_ops_per_sec", value: concurrentOpsPerSec, unit: "ops/s")
        row(bench: "TempTableContention", readerCount: 3, metric: "speedup", value: speedup, unit: "×")
    }
}
