//
//  ParallelScanCoordinator.swift
//  prism
//

import Foundation

actor ParallelScanCoordinator {
    private let rootPath: String
    private let volumeUUID: String
    private let maxConcurrency: Int
    private var isCancelled = false

    init(rootPath: String, volumeUUID: String, maxConcurrency: Int = 8) {
        self.rootPath = rootPath
        self.volumeUUID = volumeUUID
        self.maxConcurrency = maxConcurrency
    }

    /// A chunk emitted by the scan stream. `files` may be empty (when a
    /// directory had no audio files); `dirsScannedSoFar` is the total
    /// directory count across the whole walk, monotonically increasing. The
    /// caller uses `dirsScannedSoFar` for progress UX on trees where most
    /// directories hold no audio (dev drives, backups, etc.).
    struct StreamChunk: Sendable {
        let files: [ScannedFile]
        let dirsScannedSoFar: Int
        let lastDirPath: String
    }

    private func makeProducerStream() -> (AsyncStream<StreamChunk>, Task<Void, Error>) {
        let (stream, continuation) = AsyncStream<StreamChunk>.makeStream(bufferingPolicy: .unbounded)
        let cancelCheck = { self.isCancelled }
        let producerTask = Task { [rootPath, maxConcurrency] in
            defer { continuation.finish() }

            var directoryQueue: [(path: String, order: Int)] = [(rootPath, 0)]
            var inFlight = 0
            var dirsScanned = 0
            var dirsDispatched = 0

            try await withThrowingTaskGroup(of: (DirectoryScanResult, String).self) { group in
                while !directoryQueue.isEmpty && inFlight < maxConcurrency {
                    let entry = directoryQueue.removeFirst()
                    inFlight += 1
                    dirsDispatched += 1
                    group.addTask { (BulkScanner.scanDirectory(atPath: entry.path), entry.path) }
                }

                for try await (result, dirPath) in group {
                    inFlight -= 1
                    dirsScanned += 1
                    for sub in result.subdirectories {
                        directoryQueue.append((sub, dirsDispatched))
                    }

                    // Yield every directory (even empty) so the progress
                    // callback fires on trees where most dirs have no audio.
                    // Empty-files chunks are cheap to the consumer.
                    continuation.yield(StreamChunk(
                        files: result.audioFiles,
                        dirsScannedSoFar: dirsScanned,
                        lastDirPath: dirPath
                    ))

                    let cancelled = await cancelCheck()
                    while !directoryQueue.isEmpty && inFlight < maxConcurrency && !cancelled {
                        let entry = directoryQueue.removeFirst()
                        inFlight += 1
                        dirsDispatched += 1
                        group.addTask { (BulkScanner.scanDirectory(atPath: entry.path), entry.path) }
                    }
                    if cancelled { break }
                }
            }
        }
        return (stream, producerTask)
    }

    /// Progress callback. `filesFound` is cumulative audio-file count,
    /// `dirsScanned` is cumulative directory count, `currentDir` is the most
    /// recent directory walked (for display).
    typealias ProgressHandler = @Sendable (_ filesFound: Int, _ dirsScanned: Int, _ currentDir: String) async -> Void

    /// Collect-only scan: returns all discovered files in one array. Intended
    /// for tests and benchmarks that don't want to round-trip through DuckDB.
    func scan(progress: @escaping ProgressHandler) async throws -> [ScannedFile] {
        let (stream, producerTask) = makeProducerStream()
        var all: [ScannedFile] = []
        for await chunk in stream {
            all.append(contentsOf: chunk.files)
            await progress(all.count, chunk.dirsScannedSoFar, chunk.lastDirPath)
        }
        try await producerTask.value
        return all
    }

    func scanStreaming(into store: DuckDBStore, progress: @escaping ProgressHandler) async throws -> Int {
        let writerBatchSize = 5000
        // Throttle UI updates so we don't spam MainActor.run on a dir-rich
        // drive that produces thousands of chunks per second.
        let progressIntervalSec: Double = 0.1

        let (stream, producerTask) = makeProducerStream()

        var totalFiles = 0
        var writeBatch: [ScannedFile] = []
        writeBatch.reserveCapacity(writerBatchSize)
        var writeTime: Double = 0
        var lastProgressTime = CFAbsoluteTimeGetCurrent()
        var dirsScanned = 0
        var lastDirPath = ""

        for await chunk in stream {
            dirsScanned = chunk.dirsScannedSoFar
            lastDirPath = chunk.lastDirPath
            writeBatch.append(contentsOf: chunk.files)

            if writeBatch.count >= writerBatchSize {
                let ws = CFAbsoluteTimeGetCurrent()
                try store.ingestBatch(writeBatch, volumeUUID: volumeUUID)
                writeTime += CFAbsoluteTimeGetCurrent() - ws
                totalFiles += writeBatch.count
                writeBatch.removeAll(keepingCapacity: true)
                await progress(totalFiles, dirsScanned, lastDirPath)
                lastProgressTime = CFAbsoluteTimeGetCurrent()
            } else {
                // Emit a heartbeat progress even without a flush, so dir-heavy
                // trees with few audio files still show movement.
                let now = CFAbsoluteTimeGetCurrent()
                if now - lastProgressTime >= progressIntervalSec {
                    await progress(totalFiles + writeBatch.count, dirsScanned, lastDirPath)
                    lastProgressTime = now
                }
            }
        }

        if !writeBatch.isEmpty {
            let ws = CFAbsoluteTimeGetCurrent()
            try store.ingestBatch(writeBatch, volumeUUID: volumeUUID)
            writeTime += CFAbsoluteTimeGetCurrent() - ws
            totalFiles += writeBatch.count
            await progress(totalFiles, dirsScanned, lastDirPath)
        }

        try await producerTask.value
        Log.debug("Pipeline detail: \(dirsScanned) dirs scanned, DuckDB write=\(String(format: "%.2f", writeTime))s, rest=I/O+scan")
        return totalFiles
    }

    func cancel() {
        isCancelled = true
    }
}
