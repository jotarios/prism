//
//  ScannerBenchmark.swift
//  prismTests
//
//  Benchmark: FileManager vs getattrlistbulk scanning
//  Creates a test directory tree and measures both approaches.
//
//  Run with: xcodebuild test -scheme prism -only-testing:prismTests/ScannerBenchmark
//

import XCTest
import Darwin
@testable import prism

// MARK: - Standalone getattrlistbulk scanner (no project dependencies)

/// Minimal scanned entry — just what we need for benchmarking
private struct BulkEntry {
    let filename: String
    let isDirectory: Bool
    let sizeBytes: Int64
    let modTimeSec: Int
    let createTimeSec: Int
}

/// getattrlistbulk-based directory scanner (single directory, non-recursive)
private func scanDirectoryBulk(atPath path: String) -> (files: [BulkEntry], subdirs: [String]) {
    let fd = open(path, O_RDONLY | O_DIRECTORY)
    guard fd >= 0 else { return ([], []) }
    defer { close(fd) }

    var attrList = attrlist()
    attrList.bitmapcount = UInt16(ATTR_BIT_MAP_COUNT)
    attrList.commonattr =
        attrgroup_t(ATTR_CMN_RETURNED_ATTRS) |
        attrgroup_t(bitPattern: ATTR_CMN_NAME) |
        attrgroup_t(bitPattern: ATTR_CMN_ERROR) |
        attrgroup_t(bitPattern: ATTR_CMN_OBJTYPE) |
        attrgroup_t(bitPattern: ATTR_CMN_CRTIME) |
        attrgroup_t(bitPattern: ATTR_CMN_MODTIME)
    attrList.fileattr = attrgroup_t(bitPattern: ATTR_FILE_DATALENGTH)

    let bufferSize = 256 * 1024
    let buffer = UnsafeMutableRawPointer.allocate(byteCount: bufferSize, alignment: 8)
    defer { buffer.deallocate() }

    var files: [BulkEntry] = []
    var subdirs: [String] = []

    while true {
        let count = getattrlistbulk(fd, &attrList, buffer, bufferSize, 0)
        if count <= 0 { break }

        var ptr = buffer
        for _ in 0..<count {
            let entryStart = ptr
            let entryLength = Int(ptr.loadUnaligned(as: UInt32.self))
            ptr = ptr.advanced(by: 4)

            // attribute_set_t (5 × UInt32 = 20 bytes)
            let returnedCommon = ptr.loadUnaligned(as: UInt32.self)
            let returnedFile = ptr.advanced(by: 12).loadUnaligned(as: UInt32.self)
            ptr = ptr.advanced(by: 20)

            // Error
            var entryError: UInt32 = 0
            if returnedCommon & UInt32(bitPattern: ATTR_CMN_ERROR) != 0 {
                entryError = ptr.loadUnaligned(as: UInt32.self)
                ptr = ptr.advanced(by: 4)
            }
            if entryError != 0 {
                ptr = entryStart.advanced(by: entryLength)
                continue
            }

            // Name (attrreference_t)
            var filename = ""
            if returnedCommon & UInt32(bitPattern: ATTR_CMN_NAME) != 0 {
                let nameRefPtr = ptr
                let nameOffset = nameRefPtr.loadUnaligned(as: Int32.self)
                let namePtr = nameRefPtr.advanced(by: Int(nameOffset))
                filename = String(cString: namePtr.assumingMemoryBound(to: CChar.self))
                ptr = ptr.advanced(by: 8)
            }

            // Object type
            var objType: UInt32 = 0
            if returnedCommon & UInt32(bitPattern: ATTR_CMN_OBJTYPE) != 0 {
                objType = ptr.loadUnaligned(as: UInt32.self)
                ptr = ptr.advanced(by: 4)
            }

            // Creation time — use manual field reads to avoid alignment issues
            var crtimeSec: Int = 0
            if returnedCommon & UInt32(bitPattern: ATTR_CMN_CRTIME) != 0 {
                crtimeSec = ptr.loadUnaligned(as: Int.self)
                ptr = ptr.advanced(by: MemoryLayout<timespec>.size)
            }

            // Modification time
            var modtimeSec: Int = 0
            if returnedCommon & UInt32(bitPattern: ATTR_CMN_MODTIME) != 0 {
                modtimeSec = ptr.loadUnaligned(as: Int.self)
                ptr = ptr.advanced(by: MemoryLayout<timespec>.size)
            }

            // File data length
            var dataLength: Int64 = 0
            if returnedFile & UInt32(bitPattern: ATTR_FILE_DATALENGTH) != 0 {
                dataLength = ptr.loadUnaligned(as: off_t.self)
            }

            // Advance to next entry (always use entryLength)
            ptr = entryStart.advanced(by: entryLength)

            // Skip hidden
            guard !filename.isEmpty, !filename.hasPrefix(".") else { continue }

            if objType == 2 { // VDIR
                subdirs.append(path + "/" + filename)
            } else if objType == 1 { // VREG
                files.append(BulkEntry(
                    filename: filename,
                    isDirectory: false,
                    sizeBytes: dataLength,
                    modTimeSec: modtimeSec,
                    createTimeSec: crtimeSec
                ))
            }
        }
    }

    return (files, subdirs)
}

/// BFS scan using getattrlistbulk — scans full tree
private func scanTreeBulk(rootPath: String) -> (audioFiles: [BulkEntry], totalDirs: Int) {
    let audioExtensions: Set<String> = [
        "mp3", "wav", "flac", "aac", "m4a", "ogg", "wma",
        "aiff", "aif", "ape", "opus", "alac", "dsd", "dsf",
        "mp2", "mpc", "wv", "tta", "ac3", "dts"
    ]

    var queue = [rootPath]
    var audioFiles: [BulkEntry] = []
    var dirsScanned = 0

    while !queue.isEmpty {
        let dir = queue.removeFirst()
        dirsScanned += 1

        let (files, subdirs) = scanDirectoryBulk(atPath: dir)
        queue.append(contentsOf: subdirs)

        for file in files {
            // Extract extension without URL.pathExtension
            if let dotIdx = file.filename.lastIndex(of: ".") {
                let afterDot = file.filename.index(after: dotIdx)
                if afterDot < file.filename.endIndex {
                    let ext = String(file.filename[afterDot...]).lowercased()
                    if audioExtensions.contains(ext) {
                        audioFiles.append(file)
                    }
                }
            }
        }
    }

    return (audioFiles, dirsScanned)
}

/// BFS scan using FileManager (current approach)
private func scanTreeFileManager(rootPath: String) -> (audioCount: Int, totalDirs: Int) {
    let audioExtensions: Set<String> = [
        "mp3", "wav", "flac", "aac", "m4a", "ogg", "wma",
        "aiff", "aif", "ape", "opus", "alac", "dsd", "dsf",
        "mp2", "mpc", "wv", "tta", "ac3", "dts"
    ]

    var queue = [URL(fileURLWithPath: rootPath)]
    var audioCount = 0
    var dirsScanned = 0

    while !queue.isEmpty {
        let currentURL = queue.removeFirst()
        dirsScanned += 1

        guard let contents = try? FileManager.default.contentsOfDirectory(
            at: currentURL,
            includingPropertiesForKeys: [
                .nameKey, .fileSizeKey,
                .contentModificationDateKey, .creationDateKey,
                .isDirectoryKey
            ],
            options: [.skipsHiddenFiles]
        ) else { continue }

        for fileURL in contents {
            guard let resourceValues = try? fileURL.resourceValues(forKeys: [
                .nameKey, .fileSizeKey,
                .contentModificationDateKey, .creationDateKey,
                .isDirectoryKey
            ]) else { continue }

            if resourceValues.isDirectory == true {
                queue.append(fileURL)
            } else {
                let ext = fileURL.pathExtension.lowercased()
                if audioExtensions.contains(ext) {
                    // Access the same attributes we'd store
                    _ = resourceValues.name
                    _ = resourceValues.fileSize
                    _ = resourceValues.contentModificationDate
                    _ = resourceValues.creationDate
                    audioCount += 1
                }
            }
        }
    }

    return (audioCount, dirsScanned)
}


/// Parallel BFS scan using getattrlistbulk with TaskGroup
private func scanTreeBulkParallel(rootPath: String, maxConcurrency: Int) async -> (audioCount: Int, totalDirs: Int) {
    let audioExtensions: Set<String> = [
        "mp3", "wav", "flac", "aac", "m4a", "ogg", "wma",
        "aiff", "aif", "ape", "opus", "alac", "dsd", "dsf",
        "mp2", "mpc", "wv", "tta", "ac3", "dts"
    ]

    var directoryQueue = [rootPath]
    var audioCount = 0
    var dirsScanned = 0
    var inFlight = 0

    await withTaskGroup(of: (files: [BulkEntry], subdirs: [String]).self) { group in
        // Seed initial tasks
        while !directoryQueue.isEmpty && inFlight < maxConcurrency {
            let dir = directoryQueue.removeFirst()
            inFlight += 1
            group.addTask { scanDirectoryBulk(atPath: dir) }
        }

        for await result in group {
            inFlight -= 1
            dirsScanned += 1

            // Count audio files
            for file in result.files {
                if let dotIdx = file.filename.lastIndex(of: ".") {
                    let afterDot = file.filename.index(after: dotIdx)
                    if afterDot < file.filename.endIndex {
                        let ext = String(file.filename[afterDot...]).lowercased()
                        if audioExtensions.contains(ext) {
                            audioCount += 1
                        }
                    }
                }
            }

            // Enqueue subdirectories
            directoryQueue.append(contentsOf: result.subdirs)

            // Launch more work
            while !directoryQueue.isEmpty && inFlight < maxConcurrency {
                let dir = directoryQueue.removeFirst()
                inFlight += 1
                group.addTask { scanDirectoryBulk(atPath: dir) }
            }
        }
    }

    return (audioCount, dirsScanned)
}

// MARK: - Test Harness

final class ScannerBenchmark: XCTestCase {

    private var testDirectory: URL!

    // MARK: - Test directory configuration
    // Adjust these to control benchmark size:
    private let dirDepth = 4           // directory nesting depth
    private let dirsPerLevel = 5       // subdirectories per directory
    private let audioFilesPerDir = 50  // audio files per leaf directory
    private let nonAudioFilesPerDir = 20 // non-audio files to skip

    override func setUp() async throws {
        testDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent("PrismBenchmark_\(UUID().uuidString)")

        print("\n========================================")
        print("  PRISM SCANNER BENCHMARK")
        print("========================================")
        print("Creating test directory tree...")
        let (totalFiles, totalDirs) = try createTestTree()
        print("  Created \(totalFiles) files in \(totalDirs) directories")
        print("  Path: \(testDirectory.path)")
        print("========================================\n")
    }

    override func tearDown() async throws {
        if let dir = testDirectory {
            try? FileManager.default.removeItem(at: dir)
        }
    }

    // MARK: - Create test file tree

    /// Creates a nested directory tree with audio and non-audio files.
    /// Returns (totalFiles, totalDirs).
    private func createTestTree() throws -> (Int, Int) {
        var totalFiles = 0
        var totalDirs = 0

        let audioExts = ["mp3", "wav", "flac", "aac", "m4a", "ogg", "aiff"]
        let nonAudioExts = ["jpg", "png", "txt", "pdf", "doc"]

        func createLevel(at url: URL, depth: Int) throws {
            try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
            totalDirs += 1

            // Create audio files
            for i in 0..<audioFilesPerDir {
                let ext = audioExts[i % audioExts.count]
                let file = url.appendingPathComponent("audio_\(i).\(ext)")
                try Data(count: 1024 + (i * 100)).write(to: file) // varied sizes
                totalFiles += 1
            }

            // Create non-audio files (should be skipped by both scanners)
            for i in 0..<nonAudioFilesPerDir {
                let ext = nonAudioExts[i % nonAudioExts.count]
                let file = url.appendingPathComponent("other_\(i).\(ext)")
                try Data(count: 512).write(to: file)
                totalFiles += 1
            }

            // Create subdirectories
            if depth < dirDepth {
                for i in 0..<dirsPerLevel {
                    let subdir = url.appendingPathComponent("dir_\(i)")
                    try createLevel(at: subdir, depth: depth + 1)
                }
            }
        }

        try createLevel(at: testDirectory, depth: 1)
        return (totalFiles, totalDirs)
    }

    // MARK: - Benchmarks

    func testBenchmarkFileManager() throws {
        // Warmup run (primes filesystem cache)
        _ = scanTreeFileManager(rootPath: testDirectory.path)

        // Timed runs
        let iterations = 3
        var times: [Double] = []
        var audioCount = 0
        var dirCount = 0

        for i in 0..<iterations {
            let start = CFAbsoluteTimeGetCurrent()
            let result = scanTreeFileManager(rootPath: testDirectory.path)
            let elapsed = CFAbsoluteTimeGetCurrent() - start

            times.append(elapsed)
            audioCount = result.audioCount
            dirCount = result.totalDirs

            print("  FileManager  run \(i+1): \(String(format: "%.4f", elapsed))s")
        }

        let avgTime = times.reduce(0, +) / Double(iterations)
        let filesPerSec = audioCount > 0 ? Double(audioCount) / avgTime : 0

        print("\n  ── FileManager Results ──")
        print("  Audio files found: \(audioCount)")
        print("  Directories scanned: \(dirCount)")
        print("  Avg time: \(String(format: "%.4f", avgTime))s")
        print("  Best time: \(String(format: "%.4f", times.min()!))s")
        print("  Throughput: \(String(format: "%.0f", filesPerSec)) audio files/sec\n")
    }

    func testBenchmarkGetattrlistbulk() throws {
        // Warmup
        _ = scanTreeBulk(rootPath: testDirectory.path)

        let iterations = 3
        var times: [Double] = []
        var audioCount = 0
        var dirCount = 0

        for i in 0..<iterations {
            let start = CFAbsoluteTimeGetCurrent()
            let result = scanTreeBulk(rootPath: testDirectory.path)
            let elapsed = CFAbsoluteTimeGetCurrent() - start

            times.append(elapsed)
            audioCount = result.audioFiles.count
            dirCount = result.totalDirs

            print("  getattrlistbulk  run \(i+1): \(String(format: "%.4f", elapsed))s")
        }

        let avgTime = times.reduce(0, +) / Double(iterations)
        let filesPerSec = audioCount > 0 ? Double(audioCount) / avgTime : 0

        print("\n  ── getattrlistbulk Results ──")
        print("  Audio files found: \(audioCount)")
        print("  Directories scanned: \(dirCount)")
        print("  Avg time: \(String(format: "%.4f", avgTime))s")
        print("  Best time: \(String(format: "%.4f", times.min()!))s")
        print("  Throughput: \(String(format: "%.0f", filesPerSec)) audio files/sec\n")
    }

    /// Comparison test — runs both and prints side-by-side summary
    func testBenchmarkComparison() throws {
        let outputFile = "/tmp/prism_benchmark_results.txt"
        var output = ""
        func log(_ msg: String) { output += msg + "\n"; print(msg) }

        log("")
        log("════════════════════════════════════════")
        log("  SIDE-BY-SIDE COMPARISON (scan only, no DB)")
        log("════════════════════════════════════════")
        log("")

        // Warmup both (primes filesystem cache)
        _ = scanTreeFileManager(rootPath: testDirectory.path)
        _ = scanTreeBulk(rootPath: testDirectory.path)

        // Run multiple iterations for reliability
        let iterations = 5
        var fmTimes: [Double] = []
        var bulkTimes: [Double] = []
        var fmAudioCount = 0
        var fmDirCount = 0
        var bulkAudioCount = 0
        var bulkDirCount = 0

        for i in 0..<iterations {
            let fmStart = CFAbsoluteTimeGetCurrent()
            let fmResult = scanTreeFileManager(rootPath: testDirectory.path)
            let fmTime = CFAbsoluteTimeGetCurrent() - fmStart
            fmTimes.append(fmTime)
            fmAudioCount = fmResult.audioCount
            fmDirCount = fmResult.totalDirs

            let bulkStart = CFAbsoluteTimeGetCurrent()
            let bulkResult = scanTreeBulk(rootPath: testDirectory.path)
            let bulkTime = CFAbsoluteTimeGetCurrent() - bulkStart
            bulkTimes.append(bulkTime)
            bulkAudioCount = bulkResult.audioFiles.count
            bulkDirCount = bulkResult.totalDirs

            log("  Run \(i+1):  FileManager=\(String(format: "%.4f", fmTime))s  getattrlistbulk=\(String(format: "%.4f", bulkTime))s")
        }

        // Verify both found the same count
        XCTAssertEqual(fmAudioCount, bulkAudioCount,
                       "Both scanners should find the same number of audio files")
        XCTAssertEqual(fmDirCount, bulkDirCount,
                       "Both scanners should traverse the same number of directories")

        let fmAvg = fmTimes.reduce(0, +) / Double(iterations)
        let bulkAvg = bulkTimes.reduce(0, +) / Double(iterations)
        let fmBest = fmTimes.min()!
        let bulkBest = bulkTimes.min()!
        let speedupAvg = fmAvg / bulkAvg
        let speedupBest = fmBest / bulkBest

        log("")
        log("──── RESULTS ────")
        log("  Test tree: \(fmAudioCount) audio files, \(fmDirCount) directories")
        log("")
        log("  FileManager:")
        log("    Avg: \(String(format: "%.4f", fmAvg))s  Best: \(String(format: "%.4f", fmBest))s")
        log("    Throughput: \(String(format: "%.0f", Double(fmAudioCount) / fmAvg)) files/sec")
        log("")
        log("  getattrlistbulk:")
        log("    Avg: \(String(format: "%.4f", bulkAvg))s  Best: \(String(format: "%.4f", bulkBest))s")
        log("    Throughput: \(String(format: "%.0f", Double(bulkAudioCount) / bulkAvg)) files/sec")
        log("")
        log("  Speedup (avg): \(String(format: "%.1f", speedupAvg))×")
        log("  Speedup (best): \(String(format: "%.1f", speedupBest))×")
        log("════════════════════════════════════════")

        // Write to file so we can read it
        try? output.write(toFile: outputFile, atomically: true, encoding: .utf8)
    }

    /// Serial vs Parallel: isolate whether parallelism adds value beyond getattrlistbulk itself
    func testBenchmarkSerialVsParallel() async throws {
        let outputFile = "/tmp/prism_benchmark_parallel.txt"
        var output = ""
        func log(_ msg: String) { output += msg + "\n"; print(msg) }

        log("")
        log("════════════════════════════════════════")
        log("  SERIAL vs PARALLEL (getattrlistbulk only)")
        log("════════════════════════════════════════")
        log("")

        // Warmup
        _ = scanTreeBulk(rootPath: testDirectory.path)
        _ = await scanTreeBulkParallel(rootPath: testDirectory.path, maxConcurrency: 8)

        let iterations = 5
        var serialTimes: [Double] = []
        var parallelTimes: [Double] = []
        var serialCount = 0
        var parallelCount = 0

        for i in 0..<iterations {
            // Serial (1 worker)
            let serialStart = CFAbsoluteTimeGetCurrent()
            let serialResult = scanTreeBulk(rootPath: testDirectory.path)
            let serialTime = CFAbsoluteTimeGetCurrent() - serialStart
            serialTimes.append(serialTime)
            serialCount = serialResult.audioFiles.count

            // Parallel (8 workers)
            let parallelStart = CFAbsoluteTimeGetCurrent()
            let parallelResult = await scanTreeBulkParallel(rootPath: testDirectory.path, maxConcurrency: 8)
            let parallelTime = CFAbsoluteTimeGetCurrent() - parallelStart
            parallelTimes.append(parallelTime)
            parallelCount = parallelResult.audioCount

            log("  Run \(i+1):  Serial=\(String(format: "%.4f", serialTime))s  Parallel(8)=\(String(format: "%.4f", parallelTime))s")
        }

        XCTAssertEqual(serialCount, parallelCount,
                       "Serial and parallel should find the same number of audio files")

        let serialAvg = serialTimes.reduce(0, +) / Double(iterations)
        let parallelAvg = parallelTimes.reduce(0, +) / Double(iterations)
        let serialBest = serialTimes.min()!
        let parallelBest = parallelTimes.min()!
        let speedupAvg = serialAvg / parallelAvg
        let speedupBest = serialBest / parallelBest

        log("")
        log("──── RESULTS ────")
        log("  Test tree: \(serialCount) audio files")
        log("")
        log("  Serial (1 worker):")
        log("    Avg: \(String(format: "%.4f", serialAvg))s  Best: \(String(format: "%.4f", serialBest))s")
        log("    Throughput: \(String(format: "%.0f", Double(serialCount) / serialAvg)) files/sec")
        log("")
        log("  Parallel (8 workers):")
        log("    Avg: \(String(format: "%.4f", parallelAvg))s  Best: \(String(format: "%.4f", parallelBest))s")
        log("    Throughput: \(String(format: "%.0f", Double(parallelCount) / parallelAvg)) files/sec")
        log("")
        log("  Parallelism speedup (avg): \(String(format: "%.2f", speedupAvg))×")
        log("  Parallelism speedup (best): \(String(format: "%.2f", speedupBest))×")
        if speedupAvg < 1.1 {
            log("  ⚠ Parallelism shows <10% gain — consider serial-only for simplicity")
        } else if speedupAvg > 2.0 {
            log("  ✓ Parallelism shows significant gain — worth the complexity")
        } else {
            log("  ~ Parallelism shows modest gain — marginal tradeoff")
        }
        log("════════════════════════════════════════")

        try? output.write(toFile: outputFile, atomically: true, encoding: .utf8)
    }

}
