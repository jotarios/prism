//
//  LiveIndexBridge.swift
//  prism
//

import Foundation

@MainActor
protocol LiveIndexBridge: AnyObject {
    func liveIndexDidApplyDiff(volumeUUID: String, diff: ScanDiff) async
    func liveIndexDidFail(volumeUUID: String, error: LiveIndexError) async
    func liveIndexVolumeOnlineChanged(volumeUUID: String, isOnline: Bool) async
    func liveIndexDidUpdateState(_ states: [LiveIndexState]) async
}
