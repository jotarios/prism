//
//  LookupBenchmark.swift
//  prismTests
//
//  Compare DuckDB point-lookup strategies at scale.
//

import XCTest
import DuckDB
@testable import prism

final class LookupBenchmark: XCTestCase {

    var store: DuckDBStore!
    var testPath: String!
    let rowCount = 500_000

    override func setUp() async throws {
        testPath = FileManager.default.temporaryDirectory
            .appendingPathComponent("LookupBench_\(UUID().uuidString).duckdb").path
        store = try DuckDBStore(path: testPath)

        let words = ["dua", "lipa", "madonna", "love", "dance", "remix", "club", "mix", "beat", "bass"]
        let batchSize = 50_000
        var ingested = 0
        while ingested < rowCount {
            let count = min(batchSize, rowCount - ingested)
            let files = (0..<count).map { i in
                let idx = ingested + i
                return ScannedFile(
                    filename: "\(words[idx % words.count]) track \(idx).mp3",
                    parentPath: "/Volumes/Test/dir_\(idx / 1000)",
                    ext: "mp3", sizeBytes: Int64(1024 + idx),
                    modTimeSec: 1700000000 + idx, createTimeSec: 1700000000, isDirectory: false
                )
            }
            try store.ingestBatch(files, volumeUUID: "BENCH")
            ingested += count
        }
        print("[Setup] \(rowCount) rows ingested")
    }

    override func tearDown() async throws {
        store = nil
        try? FileManager.default.removeItem(atPath: testPath)
        try? FileManager.default.removeItem(atPath: testPath + ".wal")
    }

    func testAllStrategies() throws {
        let allFiles = try store.getAllFiles(limit: rowCount)
        let sampleIDs = Array(allFiles.prefix(1000).map(\.id))
        let smallIDs = Array(sampleIDs.prefix(40))

        var output = "\n══════════════════════════════════════════════\n"
        output += "  DUCKDB LOOKUP STRATEGIES — \(rowCount/1000)K rows\n"
        output += "══════════════════════════════════════════════\n\n"

        // --- Strategy 1: Single IN(...) with all IDs (current) ---
        let t1 = try measure(iterations: 5) {
            let placeholders = sampleIDs.map { String($0) }.joined(separator: ",")
            let _ = try self.store.writer_query("""
                SELECT id, filename, path, volume_uuid, extension,
                       size_bytes, date_modified, date_created, is_online
                FROM files WHERE id IN (\(placeholders))
            """)
        }
        output += "  1) IN(1000 literals):       \(String(format: "%7.1f", t1))ms\n"

        // --- Strategy 2: Batched IN(...) — 10 batches of 100 ---
        let t2 = try measure(iterations: 5) {
            var results: [SearchResult] = []
            for batch in stride(from: 0, to: sampleIDs.count, by: 100) {
                let batchIDs = Array(sampleIDs[batch..<min(batch+100, sampleIDs.count)])
                let placeholders = batchIDs.map { String($0) }.joined(separator: ",")
                let r = try self.store.writer_query("""
                    SELECT id, filename, path, volume_uuid, extension,
                           size_bytes, date_modified, date_created, is_online
                    FROM files WHERE id IN (\(placeholders))
                """)
                results.append(contentsOf: self.store.extractResults(from: r))
            }
        }
        output += "  2) 10× IN(100 literals):    \(String(format: "%7.1f", t2))ms\n"

        // --- Strategy 3: Temp table join ---
        let t3 = try measure(iterations: 5) {
            try self.store.writer_execute("CREATE OR REPLACE TEMP TABLE lookup_ids (id BIGINT)")
            try self.store.withWriterAppender(table: "lookup_ids") { appender in
                for id in sampleIDs {
                    try appender.append(id)
                    try appender.endRow()
                }
                try appender.flush()
            }
            let _ = try self.store.writer_query("""
                SELECT f.id, f.filename, f.path, f.volume_uuid, f.extension,
                       f.size_bytes, f.date_modified, f.date_created, f.is_online
                FROM files f JOIN lookup_ids l ON f.id = l.id
            """)
            try self.store.writer_execute("DROP TABLE IF EXISTS lookup_ids")
        }
        output += "  3) Temp table + JOIN:       \(String(format: "%7.1f", t3))ms\n"

        // --- Strategy 4: Only 40 IDs (visible rows) ---
        let t4 = try measure(iterations: 5) {
            let placeholders = smallIDs.map { String($0) }.joined(separator: ",")
            let _ = try self.store.writer_query("""
                SELECT id, filename, path, volume_uuid, extension,
                       size_bytes, date_modified, date_created, is_online
                FROM files WHERE id IN (\(placeholders))
            """)
        }
        output += "  4) IN(40 literals):         \(String(format: "%7.1f", t4))ms\n"

        // --- Strategy 5: Cache (dictionary lookup) ---
        try store.loadCache()
        let t5 = try measure(iterations: 5) {
            let _ = sampleIDs.compactMap { self.store.cachedResult(for: $0) }
        }
        output += "  5) In-memory cache:         \(String(format: "%7.1f", t5))ms\n"

        output += "\n══════════════════════════════════════════════\n"
        print(output)
        try? output.write(toFile: "/tmp/prism_lookup_strategies.txt", atomically: true, encoding: .utf8)
    }

    private func measure(iterations: Int, block: () throws -> Void) throws -> Double {
        // warmup
        try? block()
        var times: [Double] = []
        for _ in 0..<iterations {
            let start = CFAbsoluteTimeGetCurrent()
            try block()
            times.append((CFAbsoluteTimeGetCurrent() - start) * 1000)
        }
        return times.reduce(0, +) / Double(iterations)
    }
}
