//
//  WriterConnectionTests.swift
//  prismTests
//

import XCTest
import DuckDB
@testable import prism

final class WriterConnectionTests: XCTestCase {

    var database: Database!
    var writer: WriterConnection!
    var testPath: String!

    override func setUp() async throws {
        testPath = FileManager.default.temporaryDirectory
            .appendingPathComponent("WriterConn_\(UUID().uuidString).duckdb").path
        database = try Database(store: .file(at: URL(fileURLWithPath: testPath)))
        writer = try WriterConnection(database: database)
    }

    override func tearDown() async throws {
        writer = nil
        database = nil
        try? FileManager.default.removeItem(atPath: testPath)
        try? FileManager.default.removeItem(atPath: testPath + ".wal")
    }

    func testRoundTrip() throws {
        try writer.sync { conn in
            try conn.execute("CREATE TABLE t (id BIGINT, name VARCHAR)")
            try conn.execute("INSERT INTO t VALUES (1, 'hello'), (2, 'world')")
        }

        let count: Int = try writer.sync { conn in
            let result = try conn.query("SELECT COUNT(*) FROM t")
            let col = result[0].cast(to: Int64.self)
            return Int(col[0] ?? 0)
        }
        XCTAssertEqual(count, 2)
    }

    func testSerialization() async throws {
        try writer.sync { conn in
            try conn.execute("CREATE TABLE counter (v BIGINT)")
            try conn.execute("INSERT INTO counter VALUES (0)")
        }

        // Fire 50 tasks that each read-modify-write the counter. If the
        // writer is not serialized, we'll see lost updates.
        let iterations = 50
        await withTaskGroup(of: Void.self) { group in
            for _ in 0..<iterations {
                group.addTask { [writer] in
                    guard let writer else { return }
                    try? writer.sync { conn in
                        let result = try conn.query("SELECT v FROM counter")
                        let v = result[0].cast(to: Int64.self)[0] ?? 0
                        try conn.execute("UPDATE counter SET v = \(v + 1)")
                    }
                }
            }
        }

        let final: Int = try writer.sync { conn in
            let result = try conn.query("SELECT v FROM counter")
            return Int(result[0].cast(to: Int64.self)[0] ?? 0)
        }
        XCTAssertEqual(final, iterations, "Writer queue must serialize mutations; expected no lost updates")
    }
}
