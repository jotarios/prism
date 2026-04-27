//
//  LiveIndexError.swift
//  prism
//

import Foundation

enum LiveIndexError: Error, LocalizedError, Equatable {
    case streamCreationFailed(volumeUUID: String, underlying: OSStatus)
    case eventHistoryGap(volumeUUID: String)
    case backPressureTriggered(volumeUUID: String, eventCount: Int)
    case coordinatorNotReady
    case sandboxAccessDenied(volumeUUID: String)
    case writerError(volumeUUID: String, message: String)

    // User-facing copy for the sidebar banner. Must not leak DuckDB
    // internals or error identifiers.
    var errorDescription: String? {
        switch self {
        case .streamCreationFailed(let uuid, _):
            return "Couldn't start live watching for '\(uuid)'"
        case .eventHistoryGap, .backPressureTriggered:
            return nil   // transparent rescan, no banner
        case .coordinatorNotReady:
            return "Live index is still starting up"
        case .sandboxAccessDenied(let uuid):
            return "Need permission to watch '\(uuid)'"
        case .writerError(_, let message):
            let lower = message.lowercased()
            if lower.contains("disk full") || lower.contains("no space") {
                return "Index database is full — free up space"
            }
            if lower.contains("integrity") || lower.contains("corrupt") {
                return "Index database corrupted — rebuild required"
            }
            return "Live index paused"
        }
    }
}
