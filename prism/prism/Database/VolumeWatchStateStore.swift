//
//  VolumeWatchStateStore.swift
//  prism
//
//  volume_watch_state CRUD. Schema lives in DuckDBStore.createSchema.
//

import Foundation
import DuckDB

nonisolated final class VolumeWatchStateStore: @unchecked Sendable {
    private let writer: WriterConnection
    private let readers: ReaderPool

    init(writer: WriterConnection, readers: ReaderPool) {
        self.writer = writer
        self.readers = readers
    }

    func persistEventId(volumeUUID: String, lastEventId: UInt64, reason: String? = nil) throws {
        try writer.sync { conn in
            let now = Int64(Date().timeIntervalSince1970)
            let stmt = try PreparedStatement(connection: conn, query: """
                INSERT OR REPLACE INTO volume_watch_state
                    (volume_uuid, last_event_id, last_seen_at, polling_mode, last_reason)
                VALUES ($1, $2, $3, COALESCE(
                    (SELECT polling_mode FROM volume_watch_state WHERE volume_uuid = $1),
                    FALSE
                ), $4)
            """)
            try stmt.bind(volumeUUID, at: 1)
            try stmt.bind(Int64(bitPattern: lastEventId), at: 2)
            try stmt.bind(now, at: 3)
            // Empty string for nil reason — NULL bind isn't exposed by the
            // current DuckDB Swift wrapper. last_reason is diagnostic-only.
            try stmt.bind(reason ?? "", at: 4)
            _ = try stmt.execute()
        }
    }

    func load(volumeUUID: String) throws -> (lastEventId: UInt64, pollingMode: Bool)? {
        try readers.sync { conn in
            let stmt = try PreparedStatement(connection: conn, query: """
                SELECT last_event_id, polling_mode FROM volume_watch_state WHERE volume_uuid = $1
            """)
            try stmt.bind(volumeUUID, at: 1)
            let result = try stmt.execute()
            guard result.rowCount > 0 else { return nil }
            let eventIdCol = result[0].cast(to: Int64.self)
            let pollingCol = result[1].cast(to: Bool.self)
            guard let rawEventId = eventIdCol[0] else { return nil }
            return (UInt64(bitPattern: rawEventId), pollingCol[0] ?? false)
        }
    }

    func setPollingMode(volumeUUID: String, enabled: Bool) throws {
        try writer.sync { conn in
            let now = Int64(Date().timeIntervalSince1970)
            let stmt = try PreparedStatement(connection: conn, query: """
                INSERT OR REPLACE INTO volume_watch_state
                    (volume_uuid, last_event_id, last_seen_at, polling_mode, last_reason)
                VALUES ($1,
                    COALESCE((SELECT last_event_id FROM volume_watch_state WHERE volume_uuid = $1), 0),
                    $2,
                    $3,
                    COALESCE((SELECT last_reason FROM volume_watch_state WHERE volume_uuid = $1), '')
                )
            """)
            try stmt.bind(volumeUUID, at: 1)
            try stmt.bind(now, at: 2)
            try stmt.bind(enabled, at: 3)
            _ = try stmt.execute()
        }
    }

    /// Feeds the E+poll heuristic. Returns nil when the volume has no rows.
    func maxDateModified(volumeUUID: String) throws -> Int64? {
        try readers.sync { conn in
            let stmt = try PreparedStatement(connection: conn, query: """
                SELECT MAX(date_modified) FROM files WHERE volume_uuid = $1
            """)
            try stmt.bind(volumeUUID, at: 1)
            let result = try stmt.execute()
            guard result.rowCount > 0 else { return nil }
            let col = result[0].cast(to: Int64.self)
            return col[0]
        }
    }
}
