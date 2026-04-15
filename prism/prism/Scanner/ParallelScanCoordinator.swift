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

    private func makeProducerStream() -> (AsyncStream<[ScannedFile]>, Task<Void, Error>) {
        let (stream, continuation) = AsyncStream<[ScannedFile]>.makeStream(bufferingPolicy: .unbounded)
        let cancelCheck = { self.isCancelled }
        let producerTask = Task { [rootPath, maxConcurrency] in
            defer { continuation.finish() }

            var directoryQueue: [String] = [rootPath]
            var inFlight = 0

            try await withThrowingTaskGroup(of: DirectoryScanResult.self) { group in
                while !directoryQueue.isEmpty && inFlight < maxConcurrency {
                    let dir = directoryQueue.removeFirst()
                    inFlight += 1
                    group.addTask { BulkScanner.scanDirectory(atPath: dir) }
                }

                for try await result in group {
                    inFlight -= 1
                    directoryQueue.append(contentsOf: result.subdirectories)

                    if !result.audioFiles.isEmpty {
                        continuation.yield(result.audioFiles)
                    }

                    let cancelled = await cancelCheck()
                    while !directoryQueue.isEmpty && inFlight < maxConcurrency && !cancelled {
                        let dir = directoryQueue.removeFirst()
                        inFlight += 1
                        group.addTask { BulkScanner.scanDirectory(atPath: dir) }
                    }
                    if cancelled { break }
                }
            }
        }
        return (stream, producerTask)
    }

    /// Collect-only scan: returns all discovered files in one array. Intended
    /// for tests and benchmarks that don't want to round-trip through DuckDB.
    func scan(progress: @escaping (Int, String) async -> Void) async throws -> [ScannedFile] {
        let (stream, producerTask) = makeProducerStream()
        var all: [ScannedFile] = []
        for await files in stream {
            all.append(contentsOf: files)
            await progress(all.count, files.first?.parentPath ?? "")
        }
        try await producerTask.value
        return all
    }

    func scanStreaming(into store: DuckDBStore, progress: @escaping (Int, String) async -> Void) async throws -> Int {
        let writerBatchSize = 5000
        let (stream, producerTask) = makeProducerStream()

        var totalFiles = 0
        var writeBatch: [ScannedFile] = []
        writeBatch.reserveCapacity(writerBatchSize)
        var writeTime: Double = 0
        var dirsScanned = 0

        for await files in stream {
            dirsScanned += 1
            writeBatch.append(contentsOf: files)

            if writeBatch.count >= writerBatchSize {
                let ws = CFAbsoluteTimeGetCurrent()
                try store.ingestBatch(writeBatch, volumeUUID: volumeUUID)
                writeTime += CFAbsoluteTimeGetCurrent() - ws
                totalFiles += writeBatch.count
                writeBatch.removeAll(keepingCapacity: true)
                await progress(totalFiles, files.first?.parentPath ?? "")
            }
        }

        if !writeBatch.isEmpty {
            let ws = CFAbsoluteTimeGetCurrent()
            try store.ingestBatch(writeBatch, volumeUUID: volumeUUID)
            writeTime += CFAbsoluteTimeGetCurrent() - ws
            totalFiles += writeBatch.count
        }

        try await producerTask.value
        Log.debug("Pipeline detail: \(dirsScanned) dir batches, DuckDB write=\(String(format: "%.2f", writeTime))s, rest=I/O+scan")
        return totalFiles
    }

    func cancel() {
        isCancelled = true
    }
}
