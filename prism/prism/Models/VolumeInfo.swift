//
//  VolumeInfo.swift
//  prism
//

import Foundation

/// Represents a mounted volume
struct VolumeInfo: Identifiable, Equatable {
    let id: UUID
    let uuid: String
    let name: String
    let path: String
    let isInternal: Bool
    var isOnline: Bool

    init(uuid: String, name: String, path: String, isInternal: Bool, isOnline: Bool = true) {
        self.id = UUID()
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
