//
//  WriterConnection.swift
//  prism
//

import Foundation
import DuckDB

nonisolated final class WriterConnection {
    let connection: Connection
    private let lock = NSLock()

    init(database: Database) throws {
        self.connection = try database.connect()
    }

    func sync<T>(_ body: (Connection) throws -> T) rethrows -> T {
        lock.lock()
        defer { lock.unlock() }
        return try body(connection)
    }
}
