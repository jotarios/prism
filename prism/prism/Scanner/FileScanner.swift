//
//  FileScanner.swift
//  prism
//

import Foundation

enum ScannerError: Error {
    case invalidPath
    case permissionDenied
    case volumeNotFound
}

/// Progress callback for scanning
typealias ScanProgressCallback = (Int, String) async -> Void

final class FileScanner {
    private let volumeManager = VolumeManager.shared
    private let dbManager = DatabaseManager.shared

    private var isCancelled = false
    private let progressInterval = 50 // Report every N files for better feedback
    private let batchSize = 500 // Smaller batches to reduce write lock time and keep UI responsive

    // Audio file extensions to index
    private let audioExtensions: Set<String> = [
        "mp3", "wav", "flac", "aac", "m4a", "ogg", "wma",
        "aiff", "aif", "ape", "opus", "alac", "dsd", "dsf",
        "mp2", "mpc", "wv", "tta", "ac3", "dts"
    ]

    private func isAudioFile(_ ext: String) -> Bool {
        return audioExtensions.contains(ext.lowercased())
    }

    /// Scan a volume and index all files
    func scanVolume(path: String, progressCallback: ScanProgressCallback? = nil) async throws {
        guard FileManager.default.fileExists(atPath: path) else {
            throw ScannerError.invalidPath
        }

        guard let volumeUUID = volumeManager.getVolumeUUID(for: path) else {
            throw ScannerError.volumeNotFound
        }

        isCancelled = false
        var scannedCount = 0
        var totalFilesChecked = 0
        var batch: [FileRecordInsert] = []
        batch.reserveCapacity(batchSize)

        // Use breadth-first traversal
        var queue: [URL] = [URL(fileURLWithPath: path)]

        while !queue.isEmpty && !isCancelled {
            let currentURL = queue.removeFirst()

            // Yield at the start of each directory to allow UI updates
            await Task.yield()

            // Report directory being scanned
            await progressCallback?(scannedCount, currentURL.lastPathComponent)

            // Scan current directory
            do {
                let contents = try FileManager.default.contentsOfDirectory(
                    at: currentURL,
                    includingPropertiesForKeys: [
                        .nameKey,
                        .pathKey,
                        .fileSizeKey,
                        .contentModificationDateKey,
                        .creationDateKey,
                        .isDirectoryKey
                    ],
                    options: [.skipsHiddenFiles]
                )

                for fileURL in contents {
                    do {
                        let resourceValues = try fileURL.resourceValues(forKeys: [
                            .nameKey,
                            .pathKey,
                            .fileSizeKey,
                            .contentModificationDateKey,
                            .creationDateKey,
                            .isDirectoryKey
                        ])

                        guard let name = resourceValues.name,
                              let path = resourceValues.path,
                              let modDate = resourceValues.contentModificationDate,
                              let createDate = resourceValues.creationDate,
                              let isDirectory = resourceValues.isDirectory else {
                            continue
                        }

                        // Add directories to queue for breadth-first traversal
                        if isDirectory {
                            queue.append(fileURL)
                        } else {
                            // Files only - check for size
                            guard let size = resourceValues.fileSize else {
                                continue
                            }

                            // Extract extension
                            let ext = fileURL.pathExtension.lowercased()

                            // Only index audio files
                            guard isAudioFile(ext) else {
                                continue
                            }

                            let record = FileRecordInsert(
                                filename: name,
                                path: path,
                                volumeUUID: volumeUUID,
                                ext: ext,
                                sizeBytes: Int64(size),
                                dateModified: modDate,
                                dateCreated: createDate,
                                isOnline: true
                            )

                            batch.append(record)
                            scannedCount += 1

                            // Insert batch when it reaches size
                            if batch.count >= batchSize {
                                // Insert the batch synchronously to maintain data integrity
                                try dbManager.insertFiles(batch)
                                batch.removeAll(keepingCapacity: true)

                                // Yield to allow UI updates and other tasks to run
                                // This is critical for keeping the UI responsive
                                await Task.yield()

                                // Report progress after each batch insert
                                await progressCallback?(scannedCount, currentURL.path)
                            }

                            // Report progress more frequently
                            if scannedCount % progressInterval == 0 {
                                // Yield periodically even between batches
                                await Task.yield()
                                await progressCallback?(scannedCount, currentURL.path)
                            }
                        }
                    } catch {
                        // Skip files we can't read
                        continue
                    }
                }
            } catch {
                // Skip directories we can't read (permission issues, etc.)
                continue
            }
        }

        // Insert remaining batch
        if !batch.isEmpty {
            try dbManager.insertFiles(batch)
        }

        // Final progress report
        await progressCallback?(scannedCount, "Completed")
    }

    /// Cancel ongoing scan
    func cancel() {
        isCancelled = true
    }
}
