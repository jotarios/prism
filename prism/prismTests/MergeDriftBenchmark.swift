//
//  MergeDriftBenchmark.swift
//  prismTests
//
//  Measures whether mergeAndDiff slows down across successive Clear→rescan
//  cycles on the same DuckDB file. Production logs showed merge going
//  1.52s → 1.45s → 1.76s → 2.35s over 4 cycles — unclear whether that's
//  real drift from deleted-page accumulation or just system noise.
//

import XCTest
@testable import prism

final class MergeDriftBenchmark: XCTestCase {

    static var csvRows: [String] = ["cycle,metric,value,unit"]

    var store: DuckDBStore!
    var dbManager: DatabaseManager!
    var duckPath: String!
    var sqlitePath: String!

    override func setUp() async throws {
        let uuid = UUID().uuidString
        duckPath = FileManager.default.temporaryDirectory
            .appendingPathComponent("MergeDrift_\(uuid).duckdb").path
        sqlitePath = FileManager.default.temporaryDirectory
            .appendingPathComponent("MergeDrift_\(uuid).index.db").path
        store = try DuckDBStore(path: duckPath)
        dbManager = DatabaseManager(testPath: sqlitePath)
        try dbManager.open()
    }

    override func tearDown() async throws {
        dbManager?.close()
        dbManager = nil
        store = nil
        try? FileManager.default.removeItem(atPath: duckPath)
        try? FileManager.default.removeItem(atPath: duckPath + ".wal")
        try? FileManager.default.removeItem(atPath: sqlitePath)
        try? FileManager.default.removeItem(atPath: sqlitePath + "-shm")
        try? FileManager.default.removeItem(atPath: sqlitePath + "-wal")

        let csv = MergeDriftBenchmark.csvRows.joined(separator: "\n") + "\n"
        try? csv.write(toFile: "/tmp/prism_merge_drift.csv", atomically: true, encoding: .utf8)
    }

    private func row(cycle: Int, metric: String, value: Double, unit: String) {
        let v = String(format: "%.4f", value)
        MergeDriftBenchmark.csvRows.append("\(cycle),\(metric),\(v),\(unit)")
        print("[MERGE-DRIFT] cycle=\(cycle) \(metric)=\(v) \(unit)")
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

    private func duckDBFileBytes() -> Int64 {
        let attrs = try? FileManager.default.attributesOfItem(atPath: duckPath)
        return (attrs?[.size] as? Int64) ?? 0
    }

    /// 10 consecutive Clear→rescan cycles on the same DB file. Measures
    /// mergeAndDiff time per cycle plus the DuckDB file size to see if the
    /// DB is accumulating unreclaimed pages.
    func testClearRescanDrift10Cycles() throws {
        let rowCount = 27_604   // matches the user's drive
        let volume = "V"
        let files = makeFiles(count: rowCount)

        // Cycle 0: first-ever population (no prior Clear).
        try store.beginScan(volumeUUID: volume)
        try store.ingestBatch(files, volumeUUID: volume)
        let m0Start = CFAbsoluteTimeGetCurrent()
        let d0 = try store.mergeAndDiff(volumeUUID: volume)
        let m0 = CFAbsoluteTimeGetCurrent() - m0Start
        try dbManager.syncSearchIndex(from: store, volumeUUID: volume, diff: d0)
        try store.loadCache()
        row(cycle: 0, metric: "merge_s", value: m0, unit: "s")
        row(cycle: 0, metric: "duckdb_bytes", value: Double(duckDBFileBytes()), unit: "bytes")

        // Cycles 1..10: Clear the volume, re-populate, time the merge.
        for i in 1...10 {
            try store.deleteFilesByVolume(volume)
            let sizeAfterDelete = duckDBFileBytes()
            row(cycle: i, metric: "bytes_after_delete", value: Double(sizeAfterDelete), unit: "bytes")

            try store.beginScan(volumeUUID: volume)
            try store.ingestBatch(files, volumeUUID: volume)

            let mStart = CFAbsoluteTimeGetCurrent()
            let diff = try store.mergeAndDiff(volumeUUID: volume)
            let mT = CFAbsoluteTimeGetCurrent() - mStart
            row(cycle: i, metric: "merge_s", value: mT, unit: "s")
            row(cycle: i, metric: "duckdb_bytes", value: Double(duckDBFileBytes()), unit: "bytes")

            try dbManager.syncSearchIndex(from: store, volumeUUID: volume, diff: diff)
            try store.applyDiff(diff)
        }
    }
}
