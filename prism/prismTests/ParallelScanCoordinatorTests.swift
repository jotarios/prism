//
//  ParallelScanCoordinatorTests.swift
//  prismTests
//

import XCTest
@testable import prism

final class ParallelScanCoordinatorTests: XCTestCase {

    var testDirectory: URL!

    override func setUp() async throws {
        testDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent("PrismParallelTest_\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: testDirectory, withIntermediateDirectories: true)
    }

    override func tearDown() async throws {
        try? FileManager.default.removeItem(at: testDirectory)
    }

    private func createTree(depth: Int, dirsPerLevel: Int, filesPerDir: Int) throws {
        func create(at url: URL, currentDepth: Int) throws {
            try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
            for i in 0..<filesPerDir {
                let file = url.appendingPathComponent("audio_\(i).mp3")
                try "data".write(to: file, atomically: true, encoding: .utf8)
            }
            if currentDepth < depth {
                for i in 0..<dirsPerLevel {
                    try create(at: url.appendingPathComponent("dir_\(i)"), currentDepth: currentDepth + 1)
                }
            }
        }
        try create(at: testDirectory, currentDepth: 1)
    }

    func testScansAllFiles() async throws {
        try createTree(depth: 3, dirsPerLevel: 3, filesPerDir: 5)
        // depth=3, dirsPerLevel=3: root + 3 + 9 = 13 dirs, 13 * 5 = 65

        let coordinator = ParallelScanCoordinator(rootPath: testDirectory.path, volumeUUID: "TEST")
        let files = try await coordinator.scan { _, _ in }

        XCTAssertEqual(files.count, 65)
    }

    func testMatchesSerialCount() async throws {
        try createTree(depth: 3, dirsPerLevel: 4, filesPerDir: 10)

        let coordinator = ParallelScanCoordinator(rootPath: testDirectory.path, volumeUUID: "TEST", maxConcurrency: 8)
        let parallelFiles = try await coordinator.scan { _, _ in }

        var serialCount = 0
        var queue = [testDirectory.path]
        while !queue.isEmpty {
            let dir = queue.removeFirst()
            let result = BulkScanner.scanDirectory(atPath: dir)
            serialCount += result.audioFiles.count
            queue.append(contentsOf: result.subdirectories)
        }

        XCTAssertEqual(parallelFiles.count, serialCount)
    }

    func testNoDuplicates() async throws {
        try createTree(depth: 3, dirsPerLevel: 3, filesPerDir: 5)

        let coordinator = ParallelScanCoordinator(rootPath: testDirectory.path, volumeUUID: "TEST")
        let files = try await coordinator.scan { _, _ in }

        let paths = files.map { $0.parentPath + "/" + $0.filename }
        let uniquePaths = Set(paths)
        XCTAssertEqual(paths.count, uniquePaths.count, "Found duplicate file paths")
    }

    func testEmptyDirectory() async throws {
        let coordinator = ParallelScanCoordinator(rootPath: testDirectory.path, volumeUUID: "TEST")
        let files = try await coordinator.scan { _, _ in }
        XCTAssertEqual(files.count, 0)
    }

    func testSingleFlatDirectory() async throws {
        for i in 0..<100 {
            try "data".write(
                to: testDirectory.appendingPathComponent("file_\(i).wav"),
                atomically: true, encoding: .utf8
            )
        }

        let coordinator = ParallelScanCoordinator(rootPath: testDirectory.path, volumeUUID: "TEST")
        let files = try await coordinator.scan { _, _ in }
        XCTAssertEqual(files.count, 100)
    }

    func testProgressReporting() async throws {
        try createTree(depth: 3, dirsPerLevel: 5, filesPerDir: 50)

        var progressCalls = 0
        let coordinator = ParallelScanCoordinator(rootPath: testDirectory.path, volumeUUID: "TEST")
        _ = try await coordinator.scan { count, _ in
            progressCalls += 1
        }

        XCTAssertGreaterThan(progressCalls, 0)
    }
}
