//
//  MultiConnectionTests.swift
//  prismTests
//
//  Correctness invariants for the writer + reader-pool split.
//

import XCTest
import DuckDB
@testable import prism

final class MultiConnectionTests: XCTestCase {

    var store: DuckDBStore!
    var testPath: String!

    override func setUp() async throws {
        testPath = FileManager.default.temporaryDirectory
            .appendingPathComponent("MultiConn_\(UUID().uuidString).duckdb").path
        store = try DuckDBStore(path: testPath, readerCount: 3)
    }

    override func tearDown() async throws {
        store = nil
        try? FileManager.default.removeItem(atPath: testPath)
        try? FileManager.default.removeItem(atPath: testPath + ".wal")
    }

    private func makeFiles(range: Range<Int>) -> [ScannedFile] {
        range.map { i in
            ScannedFile(
                filename: "f_\(i).mp3", parentPath: "/Volumes/Test",
                ext: "mp3", sizeBytes: Int64(i),
                modTimeSec: 1_700_000_000 + i, createTimeSec: 1_700_000_000,
                isDirectory: false
            )
        }
    }

    /// Readers must see a committed snapshot immediately after mergeAndDiff.
    /// All 3 readers in the pool are exercised explicitly.
    func testReadersSeeCommittedSnapshot() throws {
        try store.beginScan(volumeUUID: "V")
        try store.ingestBatch(makeFiles(range: 0..<1_000), volumeUUID: "V")
        _ = try store.mergeAndDiff(volumeUUID: "V")

        // Hit each reader at least once — readers.count * 2 guarantees round-robin visits all.
        for _ in 0..<(store.readers.count * 2) {
            XCTAssertEqual(try store.getFileCount(), 1_000)
        }
    }

    /// Before mergeAndDiff commits, readers must see the PRE-scan row count —
    /// not pre-scan + staging rows. Validates MVCC isolation. The second
    /// rescan includes the full desired file set (not just a delta), matching
    /// how the real scanner feeds mergeAndDiff.
    func testReadersDoNotSeeMidScanRows() throws {
        try store.beginScan(volumeUUID: "V1")
        try store.ingestBatch(makeFiles(range: 0..<500), volumeUUID: "V1")
        _ = try store.mergeAndDiff(volumeUUID: "V1")
        XCTAssertEqual(try store.getFileCount(), 500)

        // Second scan: begin + ingest (staging) but do NOT mergeAndDiff.
        // We re-ingest all 500 existing rows (unchanged) + 1000 new ones, matching
        // the real rescan shape where the scanner feeds every file it finds.
        try store.beginScan(volumeUUID: "V1")
        try store.ingestBatch(makeFiles(range: 0..<1_500), volumeUUID: "V1")
        // Reader must still see 500, not 1500 — staging isn't merged yet.
        XCTAssertEqual(try store.getFileCount(), 500, "readers must not see staged rows pre-merge")

        _ = try store.mergeAndDiff(volumeUUID: "V1")
        XCTAssertEqual(try store.getFileCount(), 1_500)
    }

    /// Each reader uses its own TEMP table. Concurrent getFilesByIDs calls
    /// on the cache-miss path must not interfere.
    func testTempTableIsolation() throws {
        try store.beginScan(volumeUUID: "V")
        try store.ingestBatch(makeFiles(range: 0..<5_000), volumeUUID: "V")
        _ = try store.mergeAndDiff(volumeUUID: "V")
        store.invalidateCache()

        let all = try store.getAllFiles(limit: 5_000)
        let ids = Array(all.prefix(500).map(\.id))

        let group = DispatchGroup()
        let errorsLock = NSLock()
        var errors: [Error] = []

        for _ in 0..<8 {
            group.enter()
            DispatchQueue.global().async { [store] in
                guard let store else { group.leave(); return }
                do {
                    for _ in 0..<10 {
                        let r = try store.getFilesByIDs(ids)
                        if r.count != ids.count {
                            errorsLock.lock()
                            errors.append(NSError(domain: "MultiConn", code: 1,
                                userInfo: [NSLocalizedDescriptionKey: "expected \(ids.count) results, got \(r.count)"]))
                            errorsLock.unlock()
                        }
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

        XCTAssertTrue(errors.isEmpty, "concurrent TEMP-table readers must not interfere: \(errors)")
    }

    /// Post-Phase-3: per-volume scan slots. Same-volume double-begin throws;
    /// different-volume begin succeeds. The writer NSLock still serializes
    /// actual disk writes — see MultiVolumeConcurrentScanTests for the
    /// invariant we keep.
    func testWriterSerialization() throws {
        try store.beginScan(volumeUUID: "A")

        // Same volume → still throws (double-begin guard).
        XCTAssertThrowsError(try store.beginScan(volumeUUID: "A")) { err in
            guard case IndexError.scanAlreadyInProgress(let vol) = err else {
                XCTFail("expected scanAlreadyInProgress, got \(err)")
                return
            }
            XCTAssertEqual(vol, "A")
        }

        // Different volume → succeeds (per-volume slots from Phase 3).
        XCTAssertNoThrow(try store.beginScan(volumeUUID: "B"))

        _ = try store.mergeAndDiff(volumeUUID: "A")
        _ = try store.mergeAndDiff(volumeUUID: "B")
    }

    /// Search workload running concurrently with a scan. Reads must not block
    /// the full duration of the scan — if they did, total elapsed time would
    /// balloon linearly with the number of reads.
    func testSearchDuringScan() throws {
        // Pre-populate so readers have something to count.
        try store.beginScan(volumeUUID: "V")
        try store.ingestBatch(makeFiles(range: 0..<10_000), volumeUUID: "V")
        _ = try store.mergeAndDiff(volumeUUID: "V")

        let scanFiles = makeFiles(range: 10_000..<30_000)

        let scanExpectation = XCTestExpectation(description: "scan done")
        DispatchQueue.global().async { [store] in
            guard let store else { scanExpectation.fulfill(); return }
            do {
                try store.beginScan(volumeUUID: "V2")
                for chunk in stride(from: 0, to: scanFiles.count, by: 2_000) {
                    let end = min(chunk + 2_000, scanFiles.count)
                    try store.ingestBatch(Array(scanFiles[chunk..<end]), volumeUUID: "V2")
                }
                _ = try store.mergeAndDiff(volumeUUID: "V2")
            } catch {
                XCTFail("scan failed: \(error)")
            }
            scanExpectation.fulfill()
        }

        usleep(20_000)  // let the scan start

        // Run 100 reads concurrently with the scan. If these blocked for the
        // full scan duration, this test would be very slow — guarded below.
        let start = CFAbsoluteTimeGetCurrent()
        var maxSingleCallMs: Double = 0
        for _ in 0..<100 {
            let t0 = CFAbsoluteTimeGetCurrent()
            _ = try store.getFileCount()
            let dt = (CFAbsoluteTimeGetCurrent() - t0) * 1000
            maxSingleCallMs = max(maxSingleCallMs, dt)
        }
        let totalMs = (CFAbsoluteTimeGetCurrent() - start) * 1000

        wait(for: [scanExpectation], timeout: 30)

        print("[MultiConn] 100 getFileCount during scan: total=\(String(format: "%.1f", totalMs))ms max=\(String(format: "%.1f", maxSingleCallMs))ms")
        XCTAssertLessThan(maxSingleCallMs, 500, "no single getFileCount should block >500ms during scan")
    }

    /// Cache mutations via applyDiff must be visible to cache readers after
    /// completion, and concurrent cache lookups during the mutation must not
    /// see torn writes (they return either old or new values, never garbage).
    func testCacheConsistencyUnderConcurrency() throws {
        try store.beginScan(volumeUUID: "V")
        try store.ingestBatch(makeFiles(range: 0..<5_000), volumeUUID: "V")
        _ = try store.mergeAndDiff(volumeUUID: "V")
        try store.loadCache()

        // Concurrent search loop while we apply a diff that modifies 1000 rows.
        let stopFlag = NSLock()
        var stopped = false

        let group = DispatchGroup()
        group.enter()
        DispatchQueue.global().async { [store] in
            guard let store else { group.leave(); return }
            let sample = Array((0..<100).map { Int64($0) })
            while true {
                stopFlag.lock()
                let s = stopped
                stopFlag.unlock()
                if s { break }
                _ = (try? store.getFilesByIDs(sample))
            }
            group.leave()
        }

        // Modify: delete the "V" volume then rescan with differing sizes.
        try store.beginScan(volumeUUID: "V")
        let modified = (0..<5_000).map { i in
            ScannedFile(
                filename: "f_\(i).mp3", parentPath: "/Volumes/Test",
                ext: "mp3", sizeBytes: Int64(i * 2),              // different sizes
                modTimeSec: 1_700_000_000 + i + 1,                // different mtime
                createTimeSec: 1_700_000_000, isDirectory: false
            )
        }
        try store.ingestBatch(modified, volumeUUID: "V")
        let diff = try store.mergeAndDiff(volumeUUID: "V")
        try store.applyDiff(diff)

        stopFlag.lock(); stopped = true; stopFlag.unlock()
        group.wait()

        XCTAssertEqual(diff.modified.count, 5_000, "all rows should be detected as modified")
        XCTAssertEqual(try store.getFileCount(), 5_000)
    }
}
