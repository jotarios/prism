//
//  VolumeInfo.swift
//  prism
//

import Foundation

/// Identity is the volume UUID — a stable filesystem-assigned id, not a
/// fresh UUID per construction. SwiftUI relies on Identifiable.id to track
/// rows across data refreshes; a fresh per-call UUID would make every
/// loadVolumes() look like "all old rows removed, all new rows added"
/// even when nothing actually changed.
struct VolumeInfo: Identifiable, Equatable {
    var id: String { uuid }
    let uuid: String
    let name: String
    let path: String
    let isInternal: Bool
    var isOnline: Bool

    init(uuid: String, name: String, path: String, isInternal: Bool, isOnline: Bool = true) {
        self.uuid = uuid
        self.name = name
        self.path = path
        self.isInternal = isInternal
        self.isOnline = isOnline
    }

    static func == (lhs: VolumeInfo, rhs: VolumeInfo) -> Bool {
        lhs.uuid == rhs.uuid
    }
}
