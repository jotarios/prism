//
//  SearchViewModel.swift
//  prism
//

import Foundation
import Combine
import AppKit

@MainActor
class SearchViewModel: ObservableObject, LiveIndexBridge {
    static let shared = SearchViewModel()

    @Published var searchQuery: String = ""
    @Published var results: [SearchResult] = []
    @Published var resultsUpdateID = UUID()
    @Published var volumes: [VolumeInfo] = []
    @Published var isScanning: Bool = false
    @Published var scanProgress: String = ""
    @Published var totalFilesIndexed: Int = 0

    @Published var liveIndexStates: [String: LiveIndexState] = [:]
    @Published var liveIndexError: LiveIndexError? = nil

    private let dbManager = DatabaseManager.shared
    private let volumeManager = VolumeManager.shared
    private var duckDBStore: DuckDBStore?
    private var liveIndex: LiveIndexCoordinator?

    private var searchDebounce: AnyCancellable?
    private var searchTask: Task<Void, Never>?

    // Target-action (not AsyncSequence) for NSWorkspace notifications —
    // AsyncSequence leaks observers unless the task is explicitly cancelled.
    // Removed in tearDownLiveIndex on applicationWillTerminate.
    private var observerTokens: [NSObjectProtocol] = []

    private init() {
        loadVolumes()

        // Open databases off-main so cold-start doesn't block first window paint.
        Task.detached { [weak self] in
            guard let self else { return }
            do {
                try self.dbManager.open()
                Log.debug("SQLite opened")
            } catch {
                Log.error("SQLite open failed: \(error)")
            }

            do {
                let store = try DuckDBStore()
                let count = (try? store.getFileCount()) ?? 0
                if count > 0 {
                    try? store.loadCache()
                }
                Log.debug("DuckDB opened at \(store.dbPath), \(count) files")
                await MainActor.run {
                    self.duckDBStore = store
                    self.totalFilesIndexed = count
                    self.setUpLiveIndex(store: store)
                }
                await self.performSearch("")
            } catch {
                Log.error("DuckDB open failed: \(error)")
            }
        }

        searchDebounce = $searchQuery
            .removeDuplicates()
            .debounce(for: .milliseconds(150), scheduler: RunLoop.main)
            .sink { [weak self] query in
                self?.searchTask?.cancel()
                self?.searchTask = Task.detached {
                    await self?.performSearch(query)
                }
            }
    }

    /// Merge mounted-now volumes with the existing list. Mounted volumes
    /// stay/become online; previously-known volumes that are no longer
    /// mounted flip to offline (so indexed offline drives stay searchable).
    func loadVolumes() {
        let fresh = volumeManager.getMountedVolumes()
        let mountedUUIDs = Set(fresh.map { $0.uuid })

        // Update existing rows for currently-mounted volumes; preserve
        // offline rows that aren't in the fresh list.
        var updated: [VolumeInfo] = []
        for vol in volumes {
            if let live = fresh.first(where: { $0.uuid == vol.uuid }) {
                updated.append(live)
            } else {
                var off = vol
                off.isOnline = false
                updated.append(off)
            }
        }
        // Append any newly-mounted volumes not already in the list.
        for vol in fresh where !updated.contains(where: { $0.uuid == vol.uuid }) {
            updated.append(vol)
        }
        // First-load case: no prior list.
        if volumes.isEmpty {
            updated = fresh
        }
        // Hide volumes that are offline AND have no indexed files (never used).
        // This keeps freshly-disconnected drives in the list (they have files)
        // while not accumulating ghost rows for unrelated unmounts.
        updated = updated.filter { vol in
            if vol.isOnline { return true }
            let hasFiles = (try? duckDBStore?.getFileCountByVolume(vol.uuid)) ?? 0 > 0
            return hasFiles
        }
        _ = mountedUUIDs   // silence unused warning if compiler complains
        volumes = updated
    }

    // MARK: - Live Index

    private func setUpLiveIndex(store: DuckDBStore) {
        let coordinator = LiveIndexCoordinator(store: store, dbManager: dbManager, bridge: self)
        self.liveIndex = coordinator

        // Subscribe before start() — otherwise we could miss a mount that
        // fires during coordinator init.
        let wsCenter = NSWorkspace.shared.notificationCenter

        let mountToken = wsCenter.addObserver(
            forName: NSWorkspace.didMountNotification, object: nil, queue: .main
        ) { [weak self] note in
            guard let self else { return }
            self.handleMountNotification(note)
        }
        let willUnmountToken = wsCenter.addObserver(
            forName: NSWorkspace.willUnmountNotification, object: nil, queue: .main
        ) { [weak self] note in
            guard let self else { return }
            self.handleUnmountNotification(note)
        }
        let didUnmountToken = wsCenter.addObserver(
            forName: NSWorkspace.didUnmountNotification, object: nil, queue: .main
        ) { [weak self] note in
            guard let self else { return }
            self.handleUnmountNotification(note)
        }
        let willSleepToken = wsCenter.addObserver(
            forName: NSWorkspace.willSleepNotification, object: nil, queue: .main
        ) { [weak self] _ in
            guard let self else { return }
            Task { await self.liveIndex?.willSleep() }
        }
        let didWakeToken = wsCenter.addObserver(
            forName: NSWorkspace.didWakeNotification, object: nil, queue: .main
        ) { [weak self] _ in
            guard let self else { return }
            let mounted = self.volumeManager.getMountedVolumes()
            Task { await self.liveIndex?.didWake(mounted: mounted) }
        }

        observerTokens = [mountToken, willUnmountToken, didUnmountToken, willSleepToken, didWakeToken]

        let mounted = volumes
        Task { await coordinator.start(volumes: mounted) }
    }

    /// Called from prismApp on applicationWillTerminate.
    func tearDownLiveIndex() {
        let wsCenter = NSWorkspace.shared.notificationCenter
        for token in observerTokens {
            wsCenter.removeObserver(token)
        }
        observerTokens.removeAll()

        liveIndex?.flushOnTerminate()
    }

    private func handleMountNotification(_ note: Notification) {
        guard let url = note.userInfo?[NSWorkspace.volumeURLUserInfoKey] as? URL else { return }
        // Pull the freshly-mounted volume from the system and merge it in,
        // preserving any offline rows we want to keep showing for indexed
        // volumes.
        let fresh = volumeManager.getMountedVolumes()
        guard let mounted = fresh.first(where: { $0.path == url.path }) else { return }
        if let idx = volumes.firstIndex(where: { $0.uuid == mounted.uuid }) {
            volumes[idx] = mounted
        } else {
            volumes.append(mounted)
        }
        Task { await liveIndex?.onMount(volume: mounted) }
    }

    private func handleUnmountNotification(_ note: Notification) {
        guard let url = note.userInfo?[NSWorkspace.volumeURLUserInfoKey] as? URL else { return }
        // Find by path, then flip to offline IN PLACE. Don't reload from
        // mountedVolumeURLs — that would drop the row entirely (or worse,
        // re-bind it to whatever volume now occupies the slot, e.g. the
        // boot drive resolving as "external" after a USB unmount).
        guard let idx = volumes.firstIndex(where: { $0.path == url.path }) else { return }
        let uuid = volumes[idx].uuid
        volumes[idx].isOnline = false
        Task { await liveIndex?.onUnmount(volumeUUID: uuid) }
    }

    // MARK: - LiveIndexBridge

    nonisolated func liveIndexDidApplyDiff(volumeUUID: String, diff: ScanDiff) async {
        await MainActor.run {
            if let count = try? self.duckDBStore?.getFileCount() {
                self.totalFilesIndexed = count
            }
        }
        // Only re-run the search when membership might have changed (adds
        // or removes). Modified-only diffs keep their displayed metadata
        // current via the cache patch — re-searching on every modify
        // forces the results table to re-render and dismisses any
        // attached UI like QuickLook mid-interaction.
        let membershipChanged = !diff.added.isEmpty || !diff.removedIds.isEmpty
        guard membershipChanged else { return }
        await self.performSearch(self.searchQuery)
    }

    nonisolated func liveIndexDidFail(volumeUUID: String, error: LiveIndexError) async {
        await MainActor.run {
            self.liveIndexError = error
        }
    }

    nonisolated func liveIndexVolumeOnlineChanged(volumeUUID: String, isOnline: Bool) async {
        await MainActor.run {
            if let idx = self.volumes.firstIndex(where: { $0.uuid == volumeUUID }) {
                self.volumes[idx].isOnline = isOnline
            }
        }
    }

    nonisolated func liveIndexDidUpdateState(_ states: [LiveIndexState]) async {
        await MainActor.run {
            var map: [String: LiveIndexState] = [:]
            for s in states { map[s.volumeUUID] = s }
            self.liveIndexStates = map
        }
    }

    func retryLiveIndex() {
        liveIndexError = nil
        // onMount is idempotent on volumes already streaming.
        let mounted = volumes
        Task { await liveIndex?.start(volumes: mounted) }
    }

    func setLiveIndexEnabled(_ enabled: Bool) {
        UserDefaults.standard.set(!enabled, forKey: "LiveIndexDisabled")
        if enabled {
            let mounted = volumes
            Task { await liveIndex?.start(volumes: mounted) }
        } else {
            Task { await liveIndex?.stop() }
        }
    }

    var isLiveIndexEnabled: Bool {
        !UserDefaults.standard.bool(forKey: "LiveIndexDisabled")
    }

    // MARK: - Search

    func performSearch(_ query: String) async {
        do {
            try Task.checkCancellation()

            let searchStart = CFAbsoluteTimeGetCurrent()
            guard let store = await MainActor.run(body: { self.duckDBStore }) else { return }

            let fetchedResults: [SearchResult]

            if query.isEmpty {
                fetchedResults = try store.getAllFiles(limit: 1000)
            } else {
                let ftsStart = CFAbsoluteTimeGetCurrent()
                let ids = try await dbManager.searchFileIDs(query: query, limit: 1000)
                let ftsTime = CFAbsoluteTimeGetCurrent() - ftsStart

                if ids.isEmpty {
                    fetchedResults = []
                } else {
                    let lookupStart = CFAbsoluteTimeGetCurrent()
                    fetchedResults = try store.getFilesByIDs(ids)
                    let lookupTime = CFAbsoluteTimeGetCurrent() - lookupStart
                    Log.debug("Search '\(query)': FTS5=\(String(format: "%.1f", ftsTime*1000))ms Cache=\(String(format: "%.1f", lookupTime*1000))ms total=\(String(format: "%.1f", (CFAbsoluteTimeGetCurrent()-searchStart)*1000))ms results=\(fetchedResults.count)")
                }
            }

            try Task.checkCancellation()

            await MainActor.run {
                self.results = fetchedResults
                self.resultsUpdateID = UUID()
            }
        } catch is CancellationError {
            return
        } catch {
            Log.error("Search error: \(error)")
            await MainActor.run {
                self.results = []
                self.resultsUpdateID = UUID()
            }
        }
    }

    func loadAllFiles() {
        Task {
            await performSearch("")
        }
    }

    func scanVolume(_ volume: VolumeInfo) {
        isScanning = true
        scanProgress = "Starting scan of \(volume.name)..."

        Task.detached {
            do {
                guard let store = await MainActor.run(body: { self.duckDBStore }) else {
                    await MainActor.run {
                        self.scanProgress = "DuckDB store not initialized"
                        self.isScanning = false
                    }
                    return
                }

                let pipelineStart = CFAbsoluteTimeGetCurrent()
                try store.beginScan(volumeUUID: volume.uuid)

                let concurrency = volume.isInternal ? 8 : 4
                let coordinator = ParallelScanCoordinator(
                    rootPath: volume.path,
                    volumeUUID: volume.uuid,
                    maxConcurrency: concurrency
                )
                Log.debug("Scanning with \(concurrency) workers (\(volume.isInternal ? "internal" : "external") drive)")

                let scanStart = CFAbsoluteTimeGetCurrent()
                let totalFiles = try await coordinator.scanStreaming(into: store) { count, dirs, path in
                    let dirName = (path as NSString).lastPathComponent
                    let suffix = dirName.isEmpty ? "" : " • \(dirName)"
                    let label = "Scanning: \(dirs) dirs, \(count) audio files\(suffix)"
                    await MainActor.run {
                        self.scanProgress = label
                    }
                }
                let scanTime = CFAbsoluteTimeGetCurrent() - scanStart
                Log.debug("Scan complete: \(totalFiles) files in \(String(format: "%.2f", scanTime))s (\(String(format: "%.0f", Double(totalFiles) / max(scanTime, 0.001))) files/sec)")

                await MainActor.run {
                    self.scanProgress = "Building search index..."
                }

                let postStart = CFAbsoluteTimeGetCurrent()
                let mergeStart = CFAbsoluteTimeGetCurrent()
                let diff = try store.mergeAndDiff(volumeUUID: volume.uuid)
                let mergeTime = CFAbsoluteTimeGetCurrent() - mergeStart
                Log.debug("ScanDiff: added=\(diff.added.count) modified=\(diff.modified.count) removed=\(diff.removedIds.count)")

                var syncTime: Double = 0
                var applyDiffTime: Double = 0
                if !diff.isEmpty {
                    let syncStart = CFAbsoluteTimeGetCurrent()
                    try self.dbManager.syncSearchIndex(from: store, volumeUUID: volume.uuid, diff: diff)
                    syncTime = CFAbsoluteTimeGetCurrent() - syncStart

                    let applyDiffStart = CFAbsoluteTimeGetCurrent()
                    try store.applyDiff(diff)
                    applyDiffTime = CFAbsoluteTimeGetCurrent() - applyDiffStart
                }

                // Queued FSEvents batches — mostly redundant with the scan,
                // but applyDirectDiff is idempotent so apply defensively.
                let pending = store.drainPendingBatches(volumeUUID: volume.uuid)
                for batch in pending {
                    do {
                        try store.applyDirectDiff(batch, volumeUUID: volume.uuid)
                        try self.dbManager.syncSearchIndex(from: store, volumeUUID: volume.uuid, diff: batch)
                        try store.applyDiff(batch)
                    } catch {
                        Log.error("Draining pending batch failed: \(error)")
                    }
                }

                // Cold start / first scan: applyDiff was a no-op above
                // because cache hadn't loaded yet. One-time full hydration.
                var loadCacheTime: Double = 0
                if store.getAllCachedValues().isEmpty && (try? store.getFileCount()) ?? 0 > 0 {
                    let loadStart = CFAbsoluteTimeGetCurrent()
                    try store.loadCache()
                    loadCacheTime = CFAbsoluteTimeGetCurrent() - loadStart
                }

                let postTime = CFAbsoluteTimeGetCurrent() - postStart
                Log.debug("Post-scan breakdown: merge=\(String(format: "%.2f", mergeTime))s sync=\(String(format: "%.2f", syncTime))s applyDiff=\(String(format: "%.2f", applyDiffTime))s loadCache=\(String(format: "%.2f", loadCacheTime))s total=\(String(format: "%.2f", postTime))s")

                let totalTime = CFAbsoluteTimeGetCurrent() - pipelineStart
                Log.debug("Total pipeline: \(String(format: "%.2f", totalTime))s for \(totalFiles) files (\(String(format: "%.0f", Double(totalFiles) / max(totalTime, 0.001))) files/sec)")

                let count = try store.getFileCount()
                await MainActor.run {
                    self.totalFilesIndexed = count
                    self.scanProgress = "Done! \(totalFiles) files in \(String(format: "%.1f", totalTime))s"
                    self.loadAllFiles()
                }

                // First-time-indexed volume: now eligible for live index.
                await self.liveIndex?.startWatching(volume: volume)

                try await Task.sleep(nanoseconds: 2_000_000_000)
                await MainActor.run {
                    self.isScanning = false
                    self.scanProgress = ""
                }
            } catch {
                await MainActor.run {
                    self.scanProgress = "Scan failed: \(error.localizedDescription)"
                }
                try? await Task.sleep(nanoseconds: 3_000_000_000)
                await MainActor.run {
                    self.isScanning = false
                    self.scanProgress = ""
                }
            }
        }
    }

    /// Stop watching before the DELETE so an in-flight live-index batch
    /// can't race it. Re-attach after.
    func clearVolumeFiles(_ volumeUUID: String) throws {
        Task { await liveIndex?.stopWatching(volumeUUID: volumeUUID) }

        try duckDBStore?.deleteFilesByVolume(volumeUUID)
        if let store = duckDBStore {
            try dbManager.rebuildSearchIndex(from: store)
        }
        results = []
        resultsUpdateID = UUID()

        if let vol = volumes.first(where: { $0.uuid == volumeUUID }) {
            Task { await liveIndex?.startWatching(volume: vol) }
        }
    }

    func getVolumeFileCount(_ volumeUUID: String) throws -> Int {
        try duckDBStore?.getFileCountByVolume(volumeUUID) ?? 0
    }

    func getVolumeLastScannedAt(_ volumeUUID: String) throws -> Foundation.Date? {
        try duckDBStore?.lastScannedAt(volumeUUID: volumeUUID)
    }

    func getStoredFileCount() throws -> Int {
        try duckDBStore?.getFileCount() ?? 0
    }

    func rebuildIndex() async {
        do {
            await liveIndex?.stop()
            try duckDBStore?.clearAll()
            try dbManager.rebuildDatabase()
            totalFilesIndexed = 0
            results = []
            resultsUpdateID = UUID()
            let mounted = volumes
            await liveIndex?.start(volumes: mounted)
        } catch {
            Log.error("Failed to rebuild: \(error)")
        }
    }
}
