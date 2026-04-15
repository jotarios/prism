//
//  DuckDBStoreTests.swift
//  prismTests
//

import XCTest
@testable import prism

final class DuckDBStoreTests: XCTestCase {

    var store: DuckDBStore!
    var testPath: String!

    override func setUp() async throws {
        testPath = FileManager.default.temporaryDirectory
            .appendingPathComponent("PrismDuckDBTest_\(UUID().uuidString).duckdb").path
        store = try DuckDBStore(path: testPath)
    }

    override func tearDown() async throws {
        store = nil
        try? FileManager.default.removeItem(atPath: testPath)
        try? FileManager.default.removeItem(atPath: testPath + ".wal")
    }

    private func makeScanFiles(count: Int, volumeUUID: String = "VOL-1") -> [ScannedFile] {
        (0..<count).map { i in
            ScannedFile(
                filename: "file_\(i).mp3",
                parentPath: "/Volumes/Test/Music",
                ext: "mp3",
                sizeBytes: Int64(1024 + i),
                modTimeSec: 1700000000 + i,
                createTimeSec: 1700000000,
                isDirectory: false
            )
        }
    }

    func testIngestAndCount() throws {
        let files = makeScanFiles(count: 100)
        try store.ingestBatch(files, volumeUUID: "VOL-1")
        XCTAssertEqual(try store.getFileCount(), 100)
    }

    func testIngestLargeBatch() throws {
        let files = makeScanFiles(count: 10_000)
        let start = CFAbsoluteTimeGetCurrent()
        try store.ingestBatch(files, volumeUUID: "VOL-1")
        let elapsed = CFAbsoluteTimeGetCurrent() - start
        print("Ingested 10K files in \(String(format: "%.3f", elapsed))s")
        XCTAssertEqual(try store.getFileCount(), 10_000)
        XCTAssertLessThan(elapsed, 5.0)
    }

    func testGetAllFiles() throws {
        let files = makeScanFiles(count: 50)
        try store.ingestBatch(files, volumeUUID: "VOL-1")
        let results = try store.getAllFiles(limit: 10)
        XCTAssertEqual(results.count, 10)
    }

    func testGetFilesByIDs() throws {
        let files = makeScanFiles(count: 100)
        try store.ingestBatch(files, volumeUUID: "VOL-1")

        let allFiles = try store.getAllFiles(limit: 100)
        let targetIDs = Array(allFiles.prefix(5).map(\.id))
        let fetched = try store.getFilesByIDs(targetIDs)
        XCTAssertEqual(fetched.count, 5)
    }

    func testGetFilesByIDsEmpty() throws {
        let results = try store.getFilesByIDs([])
        XCTAssertEqual(results.count, 0)
    }

    func testVolumeOperations() throws {
        try store.ingestBatch(makeScanFiles(count: 50, volumeUUID: "VOL-A"), volumeUUID: "VOL-A")
        try store.ingestBatch(makeScanFiles(count: 30, volumeUUID: "VOL-B"), volumeUUID: "VOL-B")

        XCTAssertEqual(try store.getFileCount(), 80)
        XCTAssertEqual(try store.getFileCountByVolume("VOL-A"), 50)
        XCTAssertEqual(try store.getFileCountByVolume("VOL-B"), 30)

        try store.deleteFilesByVolume("VOL-A")
        XCTAssertEqual(try store.getFileCount(), 30)
        XCTAssertEqual(try store.getFileCountByVolume("VOL-A"), 0)
    }

    func testClearAll() throws {
        try store.ingestBatch(makeScanFiles(count: 100), volumeUUID: "VOL-1")
        XCTAssertEqual(try store.getFileCount(), 100)

        try store.clearAll()
        XCTAssertEqual(try store.getFileCount(), 0)
    }

    func testOnDiskStore() throws {
        // Verify the store is writing to disk (file exists after ingest)
        try store.ingestBatch(makeScanFiles(count: 10), volumeUUID: "VOL-1")
        XCTAssertTrue(FileManager.default.fileExists(atPath: testPath))
        XCTAssertEqual(try store.getFileCount(), 10)
    }

    func testIterateAllForSync() throws {
        try store.ingestBatch(makeScanFiles(count: 250), volumeUUID: "VOL-1")

        var totalRecords = 0
        var batchCalls = 0
        try store.iterateAllForSync(batchSize: 100) { batch in
            totalRecords += batch.count
            batchCalls += 1
        }

        XCTAssertEqual(totalRecords, 250)
        XCTAssertEqual(batchCalls, 3) // 100 + 100 + 50
    }

    func testAppenderThroughput() throws {
        let files = makeScanFiles(count: 100_000)
        let start = CFAbsoluteTimeGetCurrent()
        try store.ingestBatch(files, volumeUUID: "BENCH-VOL")
        let elapsed = CFAbsoluteTimeGetCurrent() - start
        let count = try store.getFileCount()
        print("DuckDB Appender: \(count) rows in \(String(format: "%.3f", elapsed))s (\(String(format: "%.0f", Double(count)/elapsed)) rows/sec)")
        XCTAssertEqual(count, 100_000)
        XCTAssertLessThan(elapsed, 10.0)
    }

    func testPointLookupLatency() throws {
        try store.ingestBatch(makeScanFiles(count: 100_000), volumeUUID: "BENCH-VOL")
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
        print("DuckDB point lookup (1000 IDs from 100K): avg=\(String(format: "%.4f", avg))s")
        XCTAssertLessThan(avg, 0.5)
    }
}
