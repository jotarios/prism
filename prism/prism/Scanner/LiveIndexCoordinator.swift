//
//  LiveIndexCoordinator.swift
//  prism
//

import Foundation
import CoreServices
import Darwin

// State machine per volume:
//
//                      ┌──────────────┐
//                      │  UNTRACKED   │  (no entry in volume_watch_state)
//                      └──────┬───────┘
//                             │ user clicks Scan
//                             ▼
//  ┌──────────────────┐    ┌────────┐     ┌─────────────┐
//  │ OFFLINE          │◀── │ ONLINE │ ──▶ │ SCANNING    │
//  │ is_online=false  │willUn-│ (listening │ scan  │ (full scan)│
//  └──────┬───────────┘ mount │ or polling)│ start │             │
//         │ didMount     └───┬────┘     └──────┬──────┘
//         ▼                  │ FSEvents        │ scan done
//  ┌──────────────────┐      │ batch fires     │
//  │ REMOUNT_VERIFY   │      ▼                 │
//  │ ≤3s budget       │  ┌──────────┐          │
//  └──────┬───────────┘  │ APPLYING │◀─────────┘
//         │ HistoryDone  │ writer   │
//         └─────────────▶│ .sync    │
//                        └──────────┘
//                             │ commit ok
//                             ▼ (back to ONLINE)
//
// FSEvents flag matrix (condensed):
//   ItemCreated+ItemIsFile → stat batch → added
//   ItemModified+ItemIsFile → stat batch → modified
//   ItemRemoved+ItemIsFile → id-only → removed
//   ItemRenamed+ItemIsFile → remove(old) + add(new) (transient duplicate OK)
//   ItemIsDir+ItemCreated → staging-path fallback
//   MustScanSubDirs / UserDropped / KernelDropped → staging-path fallback or full rescan
//   RootChanged → stream teardown, await remount
//   HistoryDone → persist event_id, switch to live mode
//
// Threading: methods are actor-isolated. FSEventStream callbacks fire on
// `fsEventsQueue`, hop into the actor via `Task`. NSWorkspace notifications
// land on MainActor, same pattern. No `await` inside `writer.sync`.
actor LiveIndexCoordinator {

    // MARK: - Dependencies

    private let store: DuckDBStore
    private let dbManager: DatabaseManager
    private weak var bridge: (any LiveIndexBridge)?

    // Dedicated dispatch queue for FSEventStream callbacks. Each stream is
    // bound to this queue via FSEventStreamSetDispatchQueue. Serial — so
    // all callbacks for all streams execute one at a time on one thread.
    private let fsEventsQueue = DispatchQueue(label: "com.jotarios.prism.liveindex.fsevents", qos: .utility)

    // MARK: - Per-volume state

    private var streams: [String: FSEventsStreamHandle] = [:]

    // FSEventsStreamHandle doesn't expose the path it was created with;
    // we need it to map callback paths back to a volume UUID.
    private var mountPaths: [String: String] = [:]

    private var pendingEvents: [String: [PendingEvent]] = [:]
    private var batchStartedAt: [String: Date] = [:]
    private var quiescenceTimers: [String: DispatchSourceTimer] = [:]
    private var pollingTimers: [String: DispatchSourceTimer] = [:]
    private var mountTimes: [String: Date] = [:]
    private var reconnectDeadlines: [String: Date] = [:]

    // Activity-meter rolling window. Pruned on each publishState.
    private var eventTimestamps: [String: [Date]] = [:]

    private var modes: [String: LiveIndexState.Mode] = [:]

    private let backPressureThreshold = 100_000
    private var inBackPressureRescan: Set<String> = []

    private var coordinatorStartedAt: Date = .distantPast
    private var isDisabled = false
    private var testMode = false

    private static let quiescenceWindow: TimeInterval = 2.0
    private static let maxWindow: TimeInterval = 10.0
    private static let sizeCap = 10_000
    // 3s is the "user attention drifts" threshold past which we flip to polling.
    private static let reconnectBudget: TimeInterval = 3.0
    private static let pollingInterval: TimeInterval = 5 * 60
    private static let activityWindow: TimeInterval = 5 * 60

    init(store: DuckDBStore, dbManager: DatabaseManager, bridge: (any LiveIndexBridge)? = nil) {
        self.store = store
        self.dbManager = dbManager
        self.bridge = bridge
    }

    func attachBridge(_ bridge: any LiveIndexBridge) {
        self.bridge = bridge
    }

    // MARK: - Lifecycle

    func start(volumes: [VolumeInfo]) {
        isDisabled = UserDefaults.standard.bool(forKey: "LiveIndexDisabled")
        if isDisabled {
            Log.info("LiveIndexCoordinator.start: disabled via UserDefaults, no-op")
            return
        }
        coordinatorStartedAt = Date()
        for volume in volumes where !volume.isInternal {
            onMount(volume: volume)
        }
        Task { await self.publishState() }
    }

    func stop() {
        for (uuid, _) in streams {
            flushPendingEventsImmediate(volumeUUID: uuid, reason: "coordinator_stop")
            stopWatching(volumeUUID: uuid)
        }
        streams.removeAll()
        pendingEvents.removeAll()
        batchStartedAt.removeAll()
        for timer in quiescenceTimers.values { timer.cancel() }
        quiescenceTimers.removeAll()
        for timer in pollingTimers.values { timer.cancel() }
        pollingTimers.removeAll()
        modes.removeAll()
        Task { await self.publishState() }
    }

    /// Used by clearVolumeFiles to prevent a live-index batch from racing
    /// with the DELETE.
    func stopWatching(volumeUUID: String) {
        flushPendingEventsImmediate(volumeUUID: volumeUUID, reason: "stop_watching")
        if let handle = streams.removeValue(forKey: volumeUUID) {
            handle.tearDown()
        }
        quiescenceTimers.removeValue(forKey: volumeUUID)?.cancel()
        pollingTimers.removeValue(forKey: volumeUUID)?.cancel()
        pendingEvents.removeValue(forKey: volumeUUID)
        batchStartedAt.removeValue(forKey: volumeUUID)
        modes[volumeUUID] = nil
        Log.info("LiveIndex.stopWatching \(volumeUUID)")
    }

    func startWatching(volume: VolumeInfo) {
        onMount(volume: volume)
    }

    func onMount(volume: VolumeInfo) {
        guard !isDisabled else { return }
        guard !volume.isInternal else { return }
        guard streams[volume.uuid] == nil else {
            Log.debug("LiveIndex.onMount: \(volume.uuid) already watched, skipping")
            return
        }

        mountTimes[volume.uuid] = Date()

        // Never-indexed volume → observe only, no auto-rescan.
        let fileCount = (try? store.getFileCountByVolume(volume.uuid)) ?? 0
        guard fileCount > 0 else {
            Log.info("LiveIndex.onMount: \(volume.name) never indexed, observing only")
            modes[volume.uuid] = .offline
            Task { await self.publishState() }
            return
        }

        let priorState: (lastEventId: UInt64, pollingMode: Bool)?
        do {
            priorState = try store.loadWatchState(volumeUUID: volume.uuid)
        } catch {
            priorState = nil
            Log.error("LiveIndex.onMount: loadWatchState failed: \(error)")
        }

        let sinceWhen: FSEventStreamEventId = priorState.map { $0.lastEventId } ?? UInt64(kFSEventStreamEventIdSinceNow)

        // Volume previously deemed unreliable → skip FSEvents entirely.
        // Clear Index + Scan resets the flag.
        if priorState?.pollingMode == true {
            Log.info("LiveIndex.onMount: \(volume.name) was previously polling → start polling timer")
            modes[volume.uuid] = .polling
            mark(volume.uuid, online: true)
            startPollingTimer(volume: volume)
            Task { await self.publishState() }
            return
        }

        guard !testMode,
              let handle = FSEventsStreamHandle(
                path: volume.path,
                sinceWhen: sinceWhen,
                callback: fsEventsCallback,
                callbackInfo: Unmanaged.passUnretained(self).toOpaque()
              ) else {
            if testMode {
                modes[volume.uuid] = .reconnecting
                mountPaths[volume.uuid] = volume.path
                Task { await self.publishState() }
                return
            }
            let err = LiveIndexError.streamCreationFailed(volumeUUID: volume.uuid, underlying: 0)
            Log.error("LiveIndex.onMount: FSEventStreamCreate returned nil for \(volume.name)")
            modes[volume.uuid] = .error
            Task {
                await self.bridge?.liveIndexDidFail(volumeUUID: volume.uuid, error: err)
                await self.publishState()
            }
            return
        }

        guard handle.start(on: fsEventsQueue) else {
            handle.tearDown()
            let err = LiveIndexError.streamCreationFailed(volumeUUID: volume.uuid, underlying: 1)
            modes[volume.uuid] = .error
            Task {
                await self.bridge?.liveIndexDidFail(volumeUUID: volume.uuid, error: err)
                await self.publishState()
            }
            return
        }

        streams[volume.uuid] = handle
        mountPaths[volume.uuid] = volume.path

        // No prior event_id → nothing to replay → HistoryDone may never fire.
        // Skip the reconnect-verify wait and go straight to Listening.
        let isReplaying = priorState?.lastEventId != nil
        if isReplaying {
            modes[volume.uuid] = .reconnecting
            reconnectDeadlines[volume.uuid] = Date().addingTimeInterval(Self.reconnectBudget)
            scheduleReconnectWatchdog(volumeUUID: volume.uuid)
        } else {
            modes[volume.uuid] = .listening
        }
        mark(volume.uuid, online: true)

        Log.info("LiveIndex.onMount: \(volume.name) stream started, replay since \(sinceWhen) (replaying=\(isReplaying))")
        Task { await self.publishState() }
    }

    func onUnmount(volumeUUID: String) {
        // Capture pending events before tearing down.
        flushPendingEventsImmediate(volumeUUID: volumeUUID, reason: "unmount")
        stopWatching(volumeUUID: volumeUUID)
        modes[volumeUUID] = .offline
        mark(volumeUUID, online: false)
        Task { await self.publishState() }
    }

    /// Sleep/wake handling — persist + stop streams on sleep; restart on wake.
    func willSleep() {
        for (uuid, _) in streams {
            flushPendingEventsImmediate(volumeUUID: uuid, reason: "willSleep")
        }
    }

    func didWake(mounted: [VolumeInfo]) {
        // Re-evaluate: for each mounted indexed volume, restart a stream.
        // Prior event-id is persisted; the stream will replay.
        for volume in mounted where streams[volume.uuid] == nil {
            onMount(volume: volume)
        }
    }

    /// Called synchronously from applicationWillTerminate. Safe because the
    /// main thread is terminating; nothing else is waiting on the actor.
    nonisolated func flushOnTerminate() {
        let semaphore = DispatchSemaphore(value: 0)
        Task {
            await self.flushAllSynchronous()
            semaphore.signal()
        }
        _ = semaphore.wait(timeout: .now() + 2.0)
    }

    private func flushAllSynchronous() {
        for uuid in Array(streams.keys) {
            flushPendingEventsImmediate(volumeUUID: uuid, reason: "terminate")
        }
    }

    // MARK: - FSEvents callback

    /// Invoked on fsEventsQueue via the C callback trampoline at the bottom
    /// of this file. Classifies flags, dispatches into the actor.
    func handleCallback(
        numEvents: Int,
        eventPaths: [String],
        eventFlags: [UInt32],
        eventIds: [UInt64]
    ) {
        var classified: [String: (flags: UInt32, id: UInt64)] = [:]
        var mustScanPaths: [String] = []
        var dirCreatedPaths: [String] = []
        var historyDone = false
        var userDropped = false
        var rootChanged = false
        var eventIdsWrapped = false

        for i in 0..<numEvents {
            let flags = eventFlags[i]
            let path = eventPaths[i]
            let id = eventIds[i]

            if flags & UInt32(kFSEventStreamEventFlagHistoryDone) != 0 { historyDone = true }
            if flags & UInt32(kFSEventStreamEventFlagUserDropped) != 0 { userDropped = true }
            if flags & UInt32(kFSEventStreamEventFlagKernelDropped) != 0 { userDropped = true }
            if flags & UInt32(kFSEventStreamEventFlagRootChanged) != 0 { rootChanged = true }
            if flags & UInt32(kFSEventStreamEventFlagEventIdsWrapped) != 0 { eventIdsWrapped = true }
            if flags & UInt32(kFSEventStreamEventFlagMustScanSubDirs) != 0 { mustScanPaths.append(path) }
            if (flags & UInt32(kFSEventStreamEventFlagItemIsDir) != 0) &&
               (flags & UInt32(kFSEventStreamEventFlagItemCreated) != 0) {
                dirCreatedPaths.append(path)
            }

            classified[path] = (flags, id)
        }

        guard let volumeUUID = findVolumeForEventPaths(eventPaths) else {
            Log.debug("LiveIndex.callback: couldn't map events to a watched volume, dropping")
            return
        }

        Task { [classified, mustScanPaths, dirCreatedPaths, historyDone, userDropped, rootChanged, eventIdsWrapped] in
            await self.handleClassifiedEvents(
                volumeUUID: volumeUUID,
                classified: classified,
                mustScanPaths: mustScanPaths,
                dirCreatedPaths: dirCreatedPaths,
                historyDone: historyDone,
                userDropped: userDropped,
                rootChanged: rootChanged,
                eventIdsWrapped: eventIdsWrapped
            )
        }
    }

    private func findVolumeForEventPaths(_ paths: [String]) -> String? {
        for (uuid, _) in streams {
            if let mountPath = mountPaths[uuid], paths.first(where: { $0.hasPrefix(mountPath) }) != nil {
                return uuid
            }
        }
        return nil
    }

    // MARK: - Event handling (actor-isolated)

    private func handleClassifiedEvents(
        volumeUUID: String,
        classified: [String: (flags: UInt32, id: UInt64)],
        mustScanPaths: [String],
        dirCreatedPaths: [String],
        historyDone: Bool,
        userDropped: Bool,
        rootChanged: Bool,
        eventIdsWrapped: Bool
    ) async {
        // Unmount raced ahead of this callback.
        guard streams[volumeUUID] != nil else {
            Log.debug("LiveIndex.handleEvents: volume \(volumeUUID) stream gone, discarding batch")
            return
        }

        if rootChanged {
            Log.info("LiveIndex: RootChanged for \(volumeUUID), treating as unmount")
            onUnmount(volumeUUID: volumeUUID)
            return
        }

        if historyDone {
            await handleHistoryDone(volumeUUID: volumeUUID, eventsSeenInReplay: classified.count)
            return
        }

        if userDropped || eventIdsWrapped {
            Log.info("LiveIndex: events dropped for \(volumeUUID); full rescan")
            await triggerFullRescan(volumeUUID: volumeUUID, reason: "fsevents_drops")
            return
        }

        // ItemIsDir events and MustScanSubDirs both route through the
        // staging path — bulk-scanning an arbitrary subtree from the
        // direct-diff hot path isn't worth the complexity.
        if !mustScanPaths.isEmpty || !dirCreatedPaths.isEmpty {
            Log.info("LiveIndex: subtree rescan for \(volumeUUID): \(mustScanPaths.count + dirCreatedPaths.count) paths")
            await triggerFullRescan(volumeUUID: volumeUUID, reason: "must_scan_subdirs")
            return
        }

        if inBackPressureRescan.contains(volumeUUID) {
            Log.debug("LiveIndex: \(volumeUUID) in back-pressure rescan; dropping \(classified.count) events")
            return
        }

        let audioExts = BulkScanner.audioExtensions
        // ItemInodeMetaMod-only events are atime/xattr changes (e.g. just
        // reading the file). Skip them — they're not content changes and
        // the user shouldn't see them in the activity meter.
        let metaModOnlyMask = UInt32(kFSEventStreamEventFlagItemInodeMetaMod)
        let contentFlagsMask = UInt32(kFSEventStreamEventFlagItemCreated)
                             | UInt32(kFSEventStreamEventFlagItemRemoved)
                             | UInt32(kFSEventStreamEventFlagItemModified)
                             | UInt32(kFSEventStreamEventFlagItemRenamed)

        var pending = pendingEvents[volumeUUID] ?? []
        for (path, info) in classified {
            let isFile = info.flags & UInt32(kFSEventStreamEventFlagItemIsFile) != 0
            guard isFile else { continue }

            // Skip metadata-only events (atime/xattr changes from a read).
            let hasContentChange = info.flags & contentFlagsMask != 0
            let metaOnly = info.flags & metaModOnlyMask != 0 && !hasContentChange
            if metaOnly { continue }

            // Skip non-audio paths so .DS_Store etc. don't reach the diff builder.
            let ext = (path as NSString).pathExtension.lowercased()
            guard audioExts.contains(ext) else { continue }

            pending.append(PendingEvent(path: path, flags: info.flags, eventId: info.id))
        }
        pendingEvents[volumeUUID] = pending

        if batchStartedAt[volumeUUID] == nil {
            batchStartedAt[volumeUUID] = Date()
        }

        // Meter increment moved to flushPendingEvents — only count diff
        // outcomes (real adds/modifies/removes after stat compares against
        // cache). FSEvents flag-level counting double-counts atime touches.

        if pending.count > backPressureThreshold {
            Log.info("LiveIndex: back-pressure for \(volumeUUID) (\(pending.count) events) → full rescan")
            pendingEvents[volumeUUID] = []
            batchStartedAt[volumeUUID] = nil
            await triggerFullRescan(volumeUUID: volumeUUID, reason: "backpressure")
            return
        }

        if pending.count >= Self.sizeCap {
            await flushPendingEvents(volumeUUID: volumeUUID, reason: "size_cap")
            return
        }

        if let started = batchStartedAt[volumeUUID],
           Date().timeIntervalSince(started) >= Self.maxWindow {
            await flushPendingEvents(volumeUUID: volumeUUID, reason: "max_window")
            return
        }

        rescheduleQuiescenceTimer(volumeUUID: volumeUUID)
        await publishState()
    }

    private func handleHistoryDone(volumeUUID: String, eventsSeenInReplay: Int) async {
        reconnectDeadlines[volumeUUID] = nil

        // E+poll heuristic: empty replay + stale MAX(date_modified) → FSEvents
        // unreliable (ExFAT-class filesystems). Compare against mountTime,
        // NOT last_seen_at — ExFAT stores local time, and comparing across
        // timezone/DST boundaries produces false positives.
        let mountTime = mountTimes[volumeUUID] ?? Date()
        let looksInconsistent: Bool
        if eventsSeenInReplay == 0 {
            do {
                if let maxMod = try store.maxDateModified(volumeUUID: volumeUUID) {
                    looksInconsistent = maxMod < Int64(mountTime.timeIntervalSince1970)
                } else {
                    looksInconsistent = false
                }
            } catch {
                Log.error("LiveIndex.handleHistoryDone: maxDateModified failed: \(error)")
                looksInconsistent = false
            }
        } else {
            looksInconsistent = false
        }

        if looksInconsistent {
            Log.info("LiveIndex: \(volumeUUID) HistoryDone empty but files stale → polling mode")
            try? store.setPollingMode(volumeUUID: volumeUUID, enabled: true)
            modes[volumeUUID] = .polling
            if let path = mountPaths[volumeUUID] {
                let volume = VolumeInfo(uuid: volumeUUID, name: (path as NSString).lastPathComponent, path: path, isInternal: false, isOnline: true)
                startPollingTimer(volume: volume)
            }
        } else {
            modes[volumeUUID] = .listening
        }

        await publishState()
    }

    // MARK: - Quiescence + flush

    private func rescheduleQuiescenceTimer(volumeUUID: String) {
        quiescenceTimers[volumeUUID]?.cancel()
        let timer = DispatchSource.makeTimerSource(queue: fsEventsQueue)
        timer.schedule(deadline: .now() + Self.quiescenceWindow)
        timer.setEventHandler { [weak self] in
            guard let self else { return }
            Task {
                await self.flushPendingEvents(volumeUUID: volumeUUID, reason: "quiescence")
            }
        }
        quiescenceTimers[volumeUUID] = timer
        timer.resume()
    }

    private func flushPendingEvents(volumeUUID: String, reason: String) async {
        guard let pending = pendingEvents[volumeUUID], !pending.isEmpty else { return }
        pendingEvents[volumeUUID] = []
        batchStartedAt[volumeUUID] = nil
        quiescenceTimers.removeValue(forKey: volumeUUID)?.cancel()

        Log.debug("LiveIndex.flush volume=\(volumeUUID) reason=\(reason) events=\(pending.count)")

        let diff = makeDiffBuilder(for: volumeUUID).build(from: pending)
        let lastEventId = pending.map { $0.eventId }.max() ?? 0

        // Increment the meter only by what the diff actually changes.
        // No-op events (atime touches that pass flag filtering but stat
        // identically to cache) produce an empty diff and don't tick.
        let realChanges = diff.added.count + diff.modified.count + diff.removedIds.count
        if realChanges > 0 {
            let now = Date()
            eventTimestamps[volumeUUID, default: []].append(contentsOf: Array(repeating: now, count: realChanges))
        }

        await applyAndPublish(volumeUUID: volumeUUID, diff: diff, lastEventId: lastEventId)
    }

    /// Synchronous flush for terminate/sleep boundaries. No bridge callback.
    private func flushPendingEventsImmediate(volumeUUID: String, reason: String) {
        guard let pending = pendingEvents[volumeUUID], !pending.isEmpty else { return }
        pendingEvents[volumeUUID] = nil
        batchStartedAt[volumeUUID] = nil

        let diff = makeDiffBuilder(for: volumeUUID).build(from: pending)
        let lastEventId = pending.map { $0.eventId }.max() ?? 0

        do {
            if store.isScanning(volumeUUID: volumeUUID) {
                store.enqueuePendingBatch(volumeUUID: volumeUUID, diff: diff)
            } else {
                try store.applyDirectDiff(diff, volumeUUID: volumeUUID)
                try dbManager.syncSearchIndex(from: store, volumeUUID: volumeUUID, diff: diff)
                try store.applyDiff(diff)
                try store.persistEventId(volumeUUID: volumeUUID, lastEventId: lastEventId, reason: reason)
            }
        } catch {
            Log.error("LiveIndex.flushImmediate: \(error)")
        }
    }

    private func applyAndPublish(volumeUUID: String, diff: ScanDiff, lastEventId: UInt64) async {
        // Empty diff → nothing actually changed. Don't take the writer lock,
        // don't fire the bridge, don't ripple into UI re-renders.
        guard !diff.isEmpty else {
            await publishState()
            return
        }

        // Defer if mergeAndDiff is mid-flight on this volume — applying now
        // would race with the merge SQL.
        if store.isScanning(volumeUUID: volumeUUID) {
            store.enqueuePendingBatch(volumeUUID: volumeUUID, diff: diff)
            Log.debug("LiveIndex: \(volumeUUID) scanning → enqueued batch")
            return
        }

        do {
            try store.applyDirectDiff(diff, volumeUUID: volumeUUID)
            try dbManager.syncSearchIndex(from: store, volumeUUID: volumeUUID, diff: diff)
            try store.applyDiff(diff)
            // Persist event-id only AFTER both commits. A crash between
            // DuckDB and SQLite commits would otherwise advance event-id
            // past rows the SQLite side never saw.
            try store.persistEventId(volumeUUID: volumeUUID, lastEventId: lastEventId)

            await bridge?.liveIndexDidApplyDiff(volumeUUID: volumeUUID, diff: diff)
        } catch {
            Log.error("LiveIndex.applyAndPublish: \(error)")
            let live = LiveIndexError.writerError(volumeUUID: volumeUUID, message: "\(error)")
            modes[volumeUUID] = .error
            await bridge?.liveIndexDidFail(volumeUUID: volumeUUID, error: live)
        }

        await publishState()
    }

    private func makeDiffBuilder(for volumeUUID: String) -> LiveIndexDiffBuilder {
        LiveIndexDiffBuilder(
            volumeUUID: volumeUUID,
            cacheProbe: { [store] id in
                guard let cached = store.cachedResult(for: id) else { return nil }
                return LiveIndexDiffBuilder.CachedStat(
                    sizeBytes: cached.sizeBytes,
                    dateModified: Int64(cached.dateModified.timeIntervalSince1970)
                )
            }
        )
    }

    // MARK: - Full-rescan fallback

    private func triggerFullRescan(volumeUUID: String, reason: String) async {
        // Drop incoming batches for this volume while the rescan runs —
        // mergeAndDiff at the end reconciles the current filesystem state.
        inBackPressureRescan.insert(volumeUUID)
        defer { inBackPressureRescan.remove(volumeUUID) }

        guard let mountPath = mountPaths[volumeUUID] else {
            Log.error("LiveIndex.triggerFullRescan: no mount path for \(volumeUUID)")
            return
        }

        Log.info("LiveIndex.triggerFullRescan \(volumeUUID) reason=\(reason) path=\(mountPath)")

        do {
            try store.beginScan(volumeUUID: volumeUUID)
        } catch {
            Log.error("LiveIndex.triggerFullRescan: beginScan failed: \(error)")
            return
        }

        let coordinator = ParallelScanCoordinator(
            rootPath: mountPath,
            volumeUUID: volumeUUID,
            maxConcurrency: 4
        )

        do {
            _ = try await coordinator.scanStreaming(into: store) { _, _, _ in }
            let diff = try store.mergeAndDiff(volumeUUID: volumeUUID)
            try dbManager.syncSearchIndex(from: store, volumeUUID: volumeUUID, diff: diff)
            try store.applyDiff(diff)
            try store.persistEventId(volumeUUID: volumeUUID, lastEventId: FSEventsGetCurrentEventId(), reason: reason)

            // Drop queued direct-diff batches. Rescan already caught their effect.
            _ = store.drainPendingBatches(volumeUUID: volumeUUID)

            await bridge?.liveIndexDidApplyDiff(volumeUUID: volumeUUID, diff: diff)
        } catch {
            Log.error("LiveIndex.triggerFullRescan: \(error)")
            let live = LiveIndexError.writerError(volumeUUID: volumeUUID, message: "\(error)")
            modes[volumeUUID] = .error
            await bridge?.liveIndexDidFail(volumeUUID: volumeUUID, error: live)
        }

        if modes[volumeUUID] != .error {
            modes[volumeUUID] = .listening
        }
        await publishState()
    }

    // MARK: - Polling timer (E+poll fallback)

    private func startPollingTimer(volume: VolumeInfo) {
        pollingTimers[volume.uuid]?.cancel()
        let timer = DispatchSource.makeTimerSource(queue: fsEventsQueue)
        timer.schedule(deadline: .now() + Self.pollingInterval, repeating: Self.pollingInterval)
        timer.setEventHandler { [weak self] in
            guard let self else { return }
            Task { await self.onPollingTick(volume: volume) }
        }
        pollingTimers[volume.uuid] = timer
        timer.resume()
    }

    private func onPollingTick(volume: VolumeInfo) async {
        guard !store.isScanning(volumeUUID: volume.uuid) else {
            Log.debug("LiveIndex.polling: \(volume.uuid) scan in progress, skipping tick")
            return
        }
        Log.info("LiveIndex.polling: tick for \(volume.uuid)")
        await triggerFullRescan(volumeUUID: volume.uuid, reason: "polling_tick")
    }

    // MARK: - Reconnect watchdog

    // One-shot timer per mount; not stored. The deadline handler re-checks
    // whether we're still in reconnecting state before acting.
    private func scheduleReconnectWatchdog(volumeUUID: String) {
        let timer = DispatchSource.makeTimerSource(queue: fsEventsQueue)
        timer.schedule(deadline: .now() + Self.reconnectBudget)
        timer.setEventHandler { [weak self] in
            guard let self else { return }
            Task { await self.onReconnectDeadline(volumeUUID: volumeUUID) }
        }
        timer.resume()
    }

    private func onReconnectDeadline(volumeUUID: String) async {
        guard modes[volumeUUID] == .reconnecting else { return }
        guard let deadline = reconnectDeadlines[volumeUUID], deadline <= Date() else { return }

        // HistoryDone never arrived in 3s. Assume inconsistent → polling.
        Log.info("LiveIndex.reconnect: \(volumeUUID) budget exceeded → polling mode")
        try? store.setPollingMode(volumeUUID: volumeUUID, enabled: true)
        modes[volumeUUID] = .polling
        if let path = mountPaths[volumeUUID] {
            let vol = VolumeInfo(uuid: volumeUUID, name: (path as NSString).lastPathComponent, path: path, isInternal: false, isOnline: true)
            startPollingTimer(volume: vol)
        }
        reconnectDeadlines[volumeUUID] = nil
        await publishState()
    }

    // MARK: - is_online helper

    private func mark(_ volumeUUID: String, online: Bool) {
        do {
            try store.setVolumeOnline(volumeUUID, isOnline: online)
        } catch {
            Log.error("LiveIndex.mark: setVolumeOnline(\(online)) failed: \(error)")
        }
        Task { await self.bridge?.liveIndexVolumeOnlineChanged(volumeUUID: volumeUUID, isOnline: online) }
    }

    // MARK: - Publish state to bridge

    private func publishState() async {
        let now = Date()
        var states: [LiveIndexState] = []
        for uuid in Set(modes.keys) {
            let mode = modes[uuid] ?? .offline

            eventTimestamps[uuid] = (eventTimestamps[uuid] ?? []).filter {
                now.timeIntervalSince($0) < Self.activityWindow
            }
            let count = eventTimestamps[uuid]?.count ?? 0
            let lastEvent = eventTimestamps[uuid]?.last

            states.append(LiveIndexState(
                volumeUUID: uuid,
                mode: mode,
                lastEventAt: lastEvent,
                eventsInLast5Min: count
            ))
        }

        await bridge?.liveIndexDidUpdateState(states)
    }

    func configureForTesting(testMode: Bool) {
        self.testMode = testMode
    }
}

// FSEventStream C-convention callback. Lives here (not in FSEventsStreamHandle)
// because it closes over LiveIndexCoordinator via Unmanaged.
private let fsEventsCallback: FSEventStreamCallback = { _, info, numEvents, eventPaths, eventFlags, eventIds in
    guard let info = info else { return }
    let coordinator = Unmanaged<LiveIndexCoordinator>.fromOpaque(info).takeUnretainedValue()

    let pathsCFArray = Unmanaged<CFArray>.fromOpaque(eventPaths).takeUnretainedValue()
    let count = CFArrayGetCount(pathsCFArray)
    var paths: [String] = []
    paths.reserveCapacity(count)
    for i in 0..<count {
        let cfPath = Unmanaged<CFString>.fromOpaque(CFArrayGetValueAtIndex(pathsCFArray, i)).takeUnretainedValue()
        paths.append(cfPath as String)
    }

    var flagsArr: [UInt32] = []
    flagsArr.reserveCapacity(numEvents)
    var idsArr: [UInt64] = []
    idsArr.reserveCapacity(numEvents)
    for i in 0..<numEvents {
        flagsArr.append(eventFlags[i])
        idsArr.append(eventIds[i])
    }

    Task {
        await coordinator.handleCallback(
            numEvents: numEvents,
            eventPaths: paths,
            eventFlags: flagsArr,
            eventIds: idsArr
        )
    }
}
