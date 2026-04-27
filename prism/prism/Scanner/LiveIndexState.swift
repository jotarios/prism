//
//  LiveIndexState.swift
//  prism
//

import Foundation

struct LiveIndexState: Equatable, Sendable {
    // listening: stream active, no batch in flight.
    // reconnecting: first 3s post-mount, awaiting HistoryDone.
    // scanning: full scan via beginScan/mergeAndDiff.
    // polling: FSEvents deemed unreliable; 5-minute poll fallback.
    // offline: volume not mounted.
    // error: writer error (disk full, corruption) — banner.
    enum Mode: Equatable, Sendable {
        case listening
        case reconnecting
        case scanning
        case polling
        case offline
        case error
    }

    let volumeUUID: String
    let mode: Mode
    let lastEventAt: Date?
    let eventsInLast5Min: Int
}
