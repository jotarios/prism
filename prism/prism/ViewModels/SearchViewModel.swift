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
    @Published var resultsUpdateID = UUID() // Changes whenever results update
    @Published var volumes: [VolumeInfo] = []
    @Published var isScanning: Bool = false
    @Published var scanProgress: String = ""
    @Published var totalFilesIndexed: Int = 0

    private let dbManager = DatabaseManager.shared
    private let volumeManager = VolumeManager.shared
    private let scanner = FileScanner()

    private var searchDebounce: AnyCancellable?
    private var searchTask: Task<Void, Never>?  // Track the current search task

    private init() {
        // Load volumes (fast, no disk access)
        loadVolumes()

        // Initialize database on background thread - NEVER block main thread!
        Task.detached {
            do {
                try self.dbManager.open()

                // Update file count
                let count = try await self.dbManager.getFileCount()
                await MainActor.run {
                    self.totalFilesIndexed = count
                }

                // Load initial files
                await self.performSearch("")
            } catch {
                print("Failed to open database: \(error)")
            }
        }

        // Setup search debouncing with task cancellation
        searchDebounce = $searchQuery
            .debounce(for: .milliseconds(150), scheduler: RunLoop.main)
            .sink { [weak self] query in
                // Cancel any in-flight search
                self?.searchTask?.cancel()

                // Start new search task on background thread
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
            // Check if task was cancelled before starting expensive work
            try Task.checkCancellation()

            // Run FTS5 search on READER THREAD (not main thread!)
            let fetchedResults = try await dbManager.searchFiles(query: query, limit: 1000)

            // Check cancellation before updating UI
            try Task.checkCancellation()

            // Update UI on main thread
            await MainActor.run {
                self.results = fetchedResults
                self.resultsUpdateID = UUID()
            }
        } catch is CancellationError {
            // Task was cancelled - ignore silently
            return
        } catch {
            print("Search error: \(error)")
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

        // Run scan on background thread (fire and forget)
        Task.detached {
            do {
                try await self.scanner.scanVolume(path: volume.path) { count, currentPath in
                    Task { @MainActor in
                        // Extract just the last path component for cleaner display
                        let lastComponent = URL(fileURLWithPath: currentPath).lastPathComponent
                        self.scanProgress = "Found \(count) files... (\(lastComponent))"
                    }
                }

                await MainActor.run {
                    self.scanProgress = "Scan complete! Indexing..."
                    // Reload files to show new results
                    self.loadAllFiles()
                }

                // Update file count asynchronously
                do {
                    let count = try await self.dbManager.getFileCount()
                    await MainActor.run {
                        self.totalFilesIndexed = count
                    }
                } catch {
                    await MainActor.run {
                        self.scanProgress = "Error: \(error.localizedDescription)"
                    }
                }

                // Wait a moment then clear
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

    func rebuildIndex() async {
        do {
            try dbManager.rebuildDatabase()
            totalFilesIndexed = 0
            results = []
        } catch {
            print("Failed to rebuild: \(error)")
        }
    }
}
