//
//  SearchViewModel.swift
//  prism
//

import Foundation
import Combine

@MainActor
class SearchViewModel: ObservableObject {
    static let shared = SearchViewModel()

    @Published var searchQuery: String = ""
    @Published var results: [SearchResult] = []
    @Published var resultsUpdateID = UUID()
    @Published var volumes: [VolumeInfo] = []
    @Published var isScanning: Bool = false
    @Published var scanProgress: String = ""
    @Published var totalFilesIndexed: Int = 0

    private let dbManager = DatabaseManager.shared
    private let volumeManager = VolumeManager.shared
    private var duckDBStore: DuckDBStore?

    private var searchDebounce: AnyCancellable?
    private var searchTask: Task<Void, Never>?

    private init() {
        loadVolumes()

        // Open databases off the main thread so cold-start doesn't block
        // the first window paint on large metadata files.
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

    func loadVolumes() {
        volumes = volumeManager.getMountedVolumes()
    }

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

                // Incremental flow: beginScan acquires the scan slot;
                // ingestBatch (called by scanStreaming) writes into the
                // per-volume staging table; mergeAndDiff identifies
                // added/modified/removed vs existing `files` rows and
                // returns the diff for SQLite/cache propagation.
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

                // First scan of a brand-new DuckDB produces a diff where
                // every row is in `added` and the cache is empty. loadCache
                // is still the cheapest path in that case.
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

                // If the cache was never loaded (cold start, first scan),
                // applyDiff is a no-op. Do a one-time full load.
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

    func clearVolumeFiles(_ volumeUUID: String) throws {
        try duckDBStore?.deleteFilesByVolume(volumeUUID)
        if let store = duckDBStore {
            // Clear Index is a full-rebuild operation by design.
            try dbManager.rebuildSearchIndex(from: store)
        }
        results = []
        resultsUpdateID = UUID()
    }

    func getVolumeFileCount(_ volumeUUID: String) throws -> Int {
        try duckDBStore?.getFileCountByVolume(volumeUUID) ?? 0
    }

    func getStoredFileCount() throws -> Int {
        try duckDBStore?.getFileCount() ?? 0
    }

    func rebuildIndex() async {
        do {
            try duckDBStore?.clearAll()
            try dbManager.rebuildDatabase()
            totalFilesIndexed = 0
            results = []
            resultsUpdateID = UUID()
        } catch {
            Log.error("Failed to rebuild: \(error)")
        }
    }
}
