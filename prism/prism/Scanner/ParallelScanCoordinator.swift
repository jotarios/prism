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

    func scan(progress: @escaping (Int, String) async -> Void) async throws -> [ScannedFile] {
        var allFiles: [ScannedFile] = []
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
                allFiles.append(contentsOf: result.audioFiles)
                directoryQueue.append(contentsOf: result.subdirectories)

                while !directoryQueue.isEmpty && inFlight < maxConcurrency && !isCancelled {
                    let dir = directoryQueue.removeFirst()
                    inFlight += 1
                    group.addTask { BulkScanner.scanDirectory(atPath: dir) }
                }

                if allFiles.count % 500 < result.audioFiles.count {
                    await progress(allFiles.count, result.audioFiles.first?.parentPath ?? "")
                }
            }
        }

        return allFiles
    }

    func scanStreaming(into store: DuckDBStore, progress: @escaping (Int, String) async -> Void) async throws -> Int {
        let writerBatchSize = 5000
        let (stream, continuation) = AsyncStream<[ScannedFile]>.makeStream(bufferingPolicy: .bufferingNewest(64))

        let cancelCheck = { self.isCancelled }
        let producerTask = Task { [rootPath, maxConcurrency] in
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
            continuation.finish()
        }

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
