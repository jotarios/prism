//
//  ReaderPoolTests.swift
//  prismTests
//

import XCTest
import DuckDB
@testable import prism

final class ReaderPoolTests: XCTestCase {

    var database: Database!
    var writer: WriterConnection!
    var readers: ReaderPool!
    var testPath: String!

    override func setUp() async throws {
        testPath = FileManager.default.temporaryDirectory
            .appendingPathComponent("ReaderPool_\(UUID().uuidString).duckdb").path
        database = try Database(store: .file(at: URL(fileURLWithPath: testPath)))
        writer = try WriterConnection(database: database)
        readers = try ReaderPool(database: database, count: 3)

        try writer.sync { conn in
            try conn.execute("CREATE TABLE t (id BIGINT)")
            try conn.execute("INSERT INTO t VALUES (1), (2), (3), (4), (5)")
        }
    }

    override func tearDown() async throws {
        readers = nil
        writer = nil
        database = nil
        try? FileManager.default.removeItem(atPath: testPath)
        try? FileManager.default.removeItem(atPath: testPath + ".wal")
    }

    func testReadersSeeCommittedData() throws {
        // Run 3 separate reads — each lands on a different reader via
        // round-robin. All must see the 5 rows committed in setUp.
        for _ in 0..<3 {
            let count: Int = try readers.sync { conn in
                let result = try conn.query("SELECT COUNT(*) FROM t")
                return Int(result[0].cast(to: Int64.self)[0] ?? 0)
            }
            XCTAssertEqual(count, 5)
        }
    }

    func testRoundRobinDispatch() throws {
        // Pin a query to each index explicitly. If the index out-of-range
        // assertion tripped or queues were shared, this would crash.
        for idx in 0..<6 {
            let count: Int = try readers.syncOn(index: idx) { conn in
                let result = try conn.query("SELECT COUNT(*) FROM t")
                return Int(result[0].cast(to: Int64.self)[0] ?? 0)
            }
            XCTAssertEqual(count, 5, "reader index=\(idx) should return 5")
        }
    }

    func testOneSlowReaderDoesNotBlockOthers() async throws {
        // Reader 0 does a slow query; readers 1 and 2 should still return
        // quickly. If the pool used a single shared queue, all three would
        // stall behind the slow query.
        async let slow: Int = withCheckedContinuation { continuation in
            DispatchQueue.global().async { [readers] in
                guard let readers else { continuation.resume(returning: -1); return }
                let count: Int = (try? readers.syncOn(index: 0) { conn in
                    // Synthetic slowdown — a sequence-heavy query.
                    let r = try conn.query("SELECT SUM(i) FROM range(0, 1000000) tbl(i)")
                    _ = r[0].cast(to: Int64.self)[0]
                    let r2 = try conn.query("SELECT COUNT(*) FROM t")
                    return Int(r2[0].cast(to: Int64.self)[0] ?? 0)
                }) ?? -1
                continuation.resume(returning: count)
            }
        }

        // While the slow read is in flight on reader 0, these should still
        // return quickly.
        let fastStart = CFAbsoluteTimeGetCurrent()
        let fast1: Int = try readers.syncOn(index: 1) { conn in
            let r = try conn.query("SELECT COUNT(*) FROM t")
            return Int(r[0].cast(to: Int64.self)[0] ?? 0)
        }
        let fast2: Int = try readers.syncOn(index: 2) { conn in
            let r = try conn.query("SELECT COUNT(*) FROM t")
            return Int(r[0].cast(to: Int64.self)[0] ?? 0)
        }
        let fastElapsed = CFAbsoluteTimeGetCurrent() - fastStart

        XCTAssertEqual(fast1, 5)
        XCTAssertEqual(fast2, 5)
        XCTAssertLessThan(fastElapsed, 0.5, "fast reads on idle readers should not be blocked")

        let slowResult = await slow
        XCTAssertEqual(slowResult, 5)
    }
}
