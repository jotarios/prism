//
//  VacuumBenchmark.swift
//  prismTests
//
//  A/B benchmark for the VACUUM step in rebuildSearchIndex. Runs against
//  an *isolated* DatabaseManager bound to a temp index.db — never touches
//  the shared singleton or production data at
//  ~/Library/Application Support/Prism/index.db.
//
//  Two scenarios per run so the comparison is meaningful:
//    A: populate → Clear (rebuild WITHOUT VACUUM) → rescan
//    B: populate → Clear (rebuild WITH VACUUM)    → rescan
//  Same data, same sequence — only VACUUM differs.
//
//  Stores are held as test-case properties and torn down in tearDown; this
//  avoids the isolated-deinit libmalloc corruption that fires when
//  nonisolated DuckDBStore values get deallocated via tuple unwinding in a
//  function scope.
//

import XCTest
@testable import prism

final class VacuumBenchmark: XCTestCase {

    static var csvRows: [String] = ["scenario,metric,value,unit,notes"]

    // Held as properties so XCTest's MainActor tearDown path releases them
    // in the order DuckDB.swift expects.
    var store: DuckDBStore!
    var dbManager: DatabaseManager!
    var duckPath: String!
    var sqlitePath: String!

    override func setUp() async throws {
        let uuid = UUID().uuidString
        duckPath = FileManager.default.temporaryDirectory
            .appendingPathComponent("VacBench_\(uuid).duckdb").path
        sqlitePath = FileManager.default.temporaryDirectory
            .appendingPathComponent("VacBench_\(uuid).index.db").path

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

        let csv = VacuumBenchmark.csvRows.joined(separator: "\n") + "\n"
        try? csv.write(toFile: "/tmp/prism_vacuum.csv", atomically: true, encoding: .utf8)
    }

    private func row(scenario: String, metric: String, value: Double, unit: String, notes: String = "") {
        let v = String(format: "%.4f", value)
        VacuumBenchmark.csvRows.append("\(scenario),\(metric),\(v),\(unit),\(notes)")
        print("[VACUUM-BENCH] \(scenario),\(metric)=\(v) \(unit) \(notes)")
    }

    private func fileSize(at path: String) -> Int64 {
        let attrs = try? FileManager.default.attributesOfItem(atPath: path)
        return (attrs?[.size] as? Int64) ?? 0
    }

    private func makeFiles(count: Int) -> [ScannedFile] {
        (0..<count).map { i in
            ScannedFile(
                filename: "file_\(i).mp3",
                parentPath: "/Volumes/Test",
                ext: "mp3",
                sizeBytes: 1024,
                modTimeSec: 1_700_000_000 + i,
                createTimeSec: 1_700_000_000,
                isDirectory: false
            )
        }
    }

    private func populate(count: Int, volumeUUID: String) throws {
        let files = makeFiles(count: count)
        try store.beginScan(volumeUUID: volumeUUID)
        try store.ingestBatch(files, volumeUUID: volumeUUID)
        let diff = try store.mergeAndDiff(volumeUUID: volumeUUID)
        try dbManager.syncSearchIndex(from: store, volumeUUID: volumeUUID, diff: diff)
    }

    /// One populate → Clear → rescan cycle with a given VACUUM policy.
    /// The caller is responsible for resetting state (recreating the store)
    /// between A and B runs if they want each to start from an empty file.
    private func runCycle(vacuumAfterClear: Bool, rowCount: Int, label: String) throws {
        let volume = "V-\(label)"

        try populate(count: rowCount, volumeUUID: volume)
        let sizeBeforeClear = fileSize(at: sqlitePath)

        try store.deleteFilesByVolume(volume)
        let rebuildStart = CFAbsoluteTimeGetCurrent()
        try dbManager.rebuildSearchIndex(from: store, vacuumAfter: vacuumAfterClear)
        let rebuildS = CFAbsoluteTimeGetCurrent() - rebuildStart

        let sizeAfterClear = fileSize(at: sqlitePath)

        let rescanStart = CFAbsoluteTimeGetCurrent()
        try populate(count: rowCount, volumeUUID: volume)
        let rescanS = CFAbsoluteTimeGetCurrent() - rescanStart

        let sizeAfterRescan = fileSize(at: sqlitePath)

        row(scenario: label, metric: "rebuild_s", value: rebuildS, unit: "s")
        row(scenario: label, metric: "rescan_s", value: rescanS, unit: "s")
        row(scenario: label, metric: "size_before_clear_b", value: Double(sizeBeforeClear), unit: "bytes")
        row(scenario: label, metric: "size_after_clear_b", value: Double(sizeAfterClear), unit: "bytes")
        row(scenario: label, metric: "size_after_rescan_b", value: Double(sizeAfterRescan), unit: "bytes")
    }

    // MARK: - A/B: VACUUM off vs VACUUM on, same DB file

    /// Runs WITHOUT-VACUUM first, then WITH-VACUUM in the same DB file.
    /// A different volume UUID each cycle keeps the rows distinct. This is
    /// the closest match to the production pathology (same DB, Clear then
    /// rescan).
    func testClearRescanWithAndWithoutVacuum() throws {
        let rowCount = 27_604  // matches the production log scenario
        try runCycle(vacuumAfterClear: false, rowCount: rowCount, label: "without-vacuum")
        try runCycle(vacuumAfterClear: true, rowCount: rowCount, label: "with-vacuum")
    }

    // MARK: - VACUUM cost at larger scale

    /// Measures whether VACUUM is affordable on a larger DB — 100K rows.
    /// If VACUUM is 30×+ more expensive than the rebuild it follows, that's
    /// a signal to switch to PRAGMA wal_checkpoint(TRUNCATE) or skip VACUUM
    /// on the first-scan path.
    func testVacuumCostAt100K() throws {
        let rowCount = 100_000
        let volume = "V"

        // Populate once; deleteFilesByVolume before each rebuild.
        try populate(count: rowCount, volumeUUID: volume)
        let sizeFull = fileSize(at: sqlitePath)
        try store.deleteFilesByVolume(volume)

        let withoutStart = CFAbsoluteTimeGetCurrent()
        try dbManager.rebuildSearchIndex(from: store, vacuumAfter: false)
        let withoutS = CFAbsoluteTimeGetCurrent() - withoutStart
        let sizeNoVac = fileSize(at: sqlitePath)

        // Repopulate so VACUUM has work to do.
        try populate(count: rowCount, volumeUUID: volume)
        try store.deleteFilesByVolume(volume)

        let withStart = CFAbsoluteTimeGetCurrent()
        try dbManager.rebuildSearchIndex(from: store, vacuumAfter: true)
        let withS = CFAbsoluteTimeGetCurrent() - withStart
        let sizeWithVac = fileSize(at: sqlitePath)

        row(scenario: "scale-100k", metric: "rebuild_without_vacuum_s", value: withoutS, unit: "s")
        row(scenario: "scale-100k", metric: "rebuild_with_vacuum_s", value: withS, unit: "s")
        row(scenario: "scale-100k", metric: "vacuum_cost_s", value: withS - withoutS, unit: "s")
        row(scenario: "scale-100k", metric: "size_full_b", value: Double(sizeFull), unit: "bytes")
        row(scenario: "scale-100k", metric: "size_without_vacuum_b", value: Double(sizeNoVac), unit: "bytes")
        row(scenario: "scale-100k", metric: "size_with_vacuum_b", value: Double(sizeWithVac), unit: "bytes")
        row(scenario: "scale-100k", metric: "bytes_reclaimed", value: Double(sizeNoVac - sizeWithVac), unit: "bytes")
    }
}
