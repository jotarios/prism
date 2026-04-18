//
//  ReaderPool.swift
//  prism
//

import Foundation
import DuckDB

nonisolated final class ReaderPool {
    nonisolated private final class Reader {
        let connection: Connection
        let lock = NSLock()
        init(connection: Connection) { self.connection = connection }
    }

    private let readers: [Reader]
    private let counterLock = NSLock()
    private var counter: Int = 0

    var count: Int { readers.count }

    init(database: Database, count: Int) throws {
        precondition(count >= 1, "ReaderPool must have at least one connection")
        var built: [Reader] = []
        built.reserveCapacity(count)
        for _ in 0..<count {
            let conn = try database.connect()
            built.append(Reader(connection: conn))
        }
        self.readers = built
    }

    private func nextIndex() -> Int {
        counterLock.lock()
        defer { counterLock.unlock() }
        let idx = counter % readers.count
        counter = (counter &+ 1) % readers.count
        return idx
    }

    func sync<T>(_ body: (Connection) throws -> T) rethrows -> T {
        let reader = readers[nextIndex()]
        reader.lock.lock()
        defer { reader.lock.unlock() }
        return try body(reader.connection)
    }

    func syncOn<T>(index: Int, _ body: (Connection) throws -> T) rethrows -> T {
        let reader = readers[index % readers.count]
        reader.lock.lock()
        defer { reader.lock.unlock() }
        return try body(reader.connection)
    }
}
