//
//  ScaleTests.swift
//  prismTests
//
//  Benchmark search strategies at scale to project 100M behavior.
//

import XCTest
import DuckDB
@testable import prism

final class ScaleTests: XCTestCase {

    func testSearchStrategiesAt100K() async throws {
        try await runComparison(rowCount: 100_000)
    }

    func testSearchStrategiesAt500K() async throws {
        try await runComparison(rowCount: 500_000)
    }

    func testSearchStrategiesAt1M() async throws {
        try await runComparison(rowCount: 1_000_000)
    }

    private func runComparison(rowCount: Int) async throws {
        let words = ["dua", "lipa", "madonna", "love", "dance", "remix", "club", "mix", "beat", "bass",
                     "deep", "house", "tech", "soul", "funk", "jazz", "rock", "pop", "edm", "trap"]

        var output = "\n══════════════════════════════════════════════\n"
        output += "  SEARCH STRATEGY COMPARISON — \(rowCount / 1000)K files\n"
        output += "══════════════════════════════════════════════\n\n"

        // --- Setup DuckDB ---
        let duckPath = FileManager.default.temporaryDirectory
            .appendingPathComponent("ScaleTest_\(rowCount)_\(UUID().uuidString).duckdb").path
        defer {
            try? FileManager.default.removeItem(atPath: duckPath)
            try? FileManager.default.removeItem(atPath: duckPath + ".wal")
        }

        let store = try DuckDBStore(path: duckPath)
        let batchSize = 50_000
        var ingested = 0

        let ingestStart = CFAbsoluteTimeGetCurrent()
        while ingested < rowCount {
            let count = min(batchSize, rowCount - ingested)
            let files = (0..<count).map { i in
                let idx = ingested + i
                let w1 = words[idx % words.count]
                let w2 = words[(idx / 20) % words.count]
                return ScannedFile(
                    filename: "\(w1) \(w2) track \(idx).mp3",
                    parentPath: "/Volumes/Test/Music/dir_\(idx / 1000)",
                    ext: "mp3",
                    sizeBytes: Int64(1024 + idx % 10000),
                    modTimeSec: 1700000000 + idx,
                    createTimeSec: 1700000000,
                    isDirectory: false
                )
            }
            try store.ingestBatch(files, volumeUUID: "SCALE-TEST")
            ingested += count
        }
        let ingestTime = CFAbsoluteTimeGetCurrent() - ingestStart
        output += "  Ingestion: \(rowCount) rows in \(String(format: "%.2f", ingestTime))s (\(String(format: "%.0f", Double(rowCount) / ingestTime)) rows/sec)\n\n"

        // --- Setup SQLite FTS5 ---
        let dbManager = DatabaseManager.shared
        do { try dbManager.open() } catch { }
        try dbManager.rebuildDatabase()

        let syncStart = CFAbsoluteTimeGetCurrent()
        try dbManager.syncSearchIndex(from: store)
        let syncTime = CFAbsoluteTimeGetCurrent() - syncStart
        output += "  FTS5 sync: \(String(format: "%.2f", syncTime))s\n"

        // --- Setup cache ---
        let cacheStart = CFAbsoluteTimeGetCurrent()
        try store.loadCache()
        let cacheTime = CFAbsoluteTimeGetCurrent() - cacheStart
        let cacheMemMB = Double(rowCount * 200) / 1_000_000.0
        output += "  Cache load: \(String(format: "%.2f", cacheTime))s (~\(String(format: "%.0f", cacheMemMB))MB RAM)\n\n"

        let queries = ["dua", "love", "remix track", "x"]
        let iterations = 5

        output += "  ┌─────────────────┬──────────┬──────────┬──────────┬─────────┐\n"
        output += "  │ Query           │ FTS5+Duck│ FTS5+Cash│ CacheOnly│ DuckOnly│\n"
        output += "  │                 │ (ms)     │ (ms)     │ (ms)     │ (ms)    │\n"
        output += "  ├─────────────────┼──────────┼──────────┼──────────┼─────────┤\n"

        for query in queries {
            // Strategy 1: FTS5 → DuckDB point lookup (no cache)
            store.invalidateCache()
            var ftsDBTimes: [Double] = []
            for _ in 0..<iterations {
                let start = CFAbsoluteTimeGetCurrent()
                let ids = try await dbManager.searchFileIDs(query: query, limit: 1000)
                if !ids.isEmpty {
                    let _ = try store.getFilesByIDs(ids)
                }
                ftsDBTimes.append((CFAbsoluteTimeGetCurrent() - start) * 1000)
            }

            // Strategy 2: FTS5 → Cache lookup
            try store.loadCache()
            var ftsCacheTimes: [Double] = []
            for _ in 0..<iterations {
                let start = CFAbsoluteTimeGetCurrent()
                let ids = try await dbManager.searchFileIDs(query: query, limit: 1000)
                if !ids.isEmpty {
                    let _ = try store.getFilesByIDs(ids)
                }
                ftsCacheTimes.append((CFAbsoluteTimeGetCurrent() - start) * 1000)
            }

            // Strategy 3: Cache-only brute force
            let allCached = Array(store.getAllCachedValues())
            let queryLower = query.lowercased()
            var cacheOnlyTimes: [Double] = []
            for _ in 0..<iterations {
                let start = CFAbsoluteTimeGetCurrent()
                var results: [SearchResult] = []
                for item in allCached {
                    if item.filename.lowercased().contains(queryLower) {
                        results.append(item)
                        if results.count >= 1000 { break }
                    }
                }
                cacheOnlyTimes.append((CFAbsoluteTimeGetCurrent() - start) * 1000)
            }

            // Strategy 4: DuckDB-only (ILIKE)
            store.invalidateCache()
            var duckOnlyTimes: [Double] = []
            for _ in 0..<iterations {
                let start = CFAbsoluteTimeGetCurrent()
                let _ = try store.searchByFilename(query: query, limit: 1000)
                duckOnlyTimes.append((CFAbsoluteTimeGetCurrent() - start) * 1000)
            }

            let ftsDBAvg = ftsDBTimes.reduce(0, +) / Double(iterations)
            let ftsCacheAvg = ftsCacheTimes.reduce(0, +) / Double(iterations)
            let cacheAvg = cacheOnlyTimes.reduce(0, +) / Double(iterations)
            let duckAvg = duckOnlyTimes.reduce(0, +) / Double(iterations)

            let q = query.padding(toLength: 15, withPad: " ", startingAt: 0)
            output += "  │ \(q) │ \(String(format: "%7.1f", ftsDBAvg))  │ \(String(format: "%7.1f", ftsCacheAvg))  │ \(String(format: "%7.1f", cacheAvg))  │ \(String(format: "%6.1f", duckAvg))  │\n"
        }

        output += "  └─────────────────┴──────────┴──────────┴──────────┴─────────┘\n"

        let factor = 100_000_000.0 / Double(rowCount)
        output += "\n  Projected at 100M (\(String(format: "%.0f", factor))× scale):\n"
        output += "    Cache RAM: ~\(String(format: "%.0f", cacheMemMB * factor / 1000))GB\n"
        output += "    FTS5: constant (indexed)\n"
        output += "    Cache brute force: ~\(String(format: "%.0f", factor))× current\n"
        output += "    DuckDB ILIKE: scales with table scan\n"
        output += "\n══════════════════════════════════════════════\n"

        print(output)
        try? output.write(toFile: "/tmp/prism_scale_\(rowCount / 1000)K.txt", atomically: true, encoding: .utf8)

        dbManager.close()
    }
}
