//
//  ScannerTests.swift
//  prismTests
//

import XCTest
@testable import prism

final class ScannerTests: XCTestCase {

    var dbManager: DatabaseManager!
    var scanner: FileScanner!
    var testDirectory: URL!

    override func setUp() async throws {
        dbManager = DatabaseManager.shared
        try dbManager.open()
        try dbManager.rebuildDatabase()

        scanner = FileScanner()

        // Create a temporary test directory
        testDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent("PrismScanTest_\(UUID().uuidString)")

        try FileManager.default.createDirectory(at: testDirectory, withIntermediateDirectories: true)

        // Create test file structure
        try createTestFiles()
    }

    override func tearDown() async throws {
        // Clean up test directory
        try? FileManager.default.removeItem(at: testDirectory)
        dbManager.close()
    }

    private func createTestFiles() throws {
        // Create some test files and directories
        let musicDir = testDirectory.appendingPathComponent("Music")
        let photosDir = testDirectory.appendingPathComponent("Photos")

        try FileManager.default.createDirectory(at: musicDir, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: photosDir, withIntermediateDirectories: true)

        // Create test files
        for i in 1...10 {
            let mp3File = musicDir.appendingPathComponent("song_\(i).mp3")
            try "Test MP3 Content".write(to: mp3File, atomically: true, encoding: .utf8)

            let jpgFile = photosDir.appendingPathComponent("photo_\(i).jpg")
            try "Test JPG Content".write(to: jpgFile, atomically: true, encoding: .utf8)
        }

        // Create a subdirectory
        let albumDir = musicDir.appendingPathComponent("Album1")
        try FileManager.default.createDirectory(at: albumDir, withIntermediateDirectories: true)

        for i in 1...5 {
            let file = albumDir.appendingPathComponent("track_\(i).mp3")
            try "Test Track Content".write(to: file, atomically: true, encoding: .utf8)
        }
    }

    func testVolumeManager() throws {
        let volumes = VolumeManager.shared.getMountedVolumes()
        XCTAssertGreaterThan(volumes.count, 0, "Should have at least one mounted volume")

        // Check that we can get volume UUID for test directory
        let uuid = VolumeManager.shared.getVolumeUUID(for: testDirectory.path)
        XCTAssertNotNil(uuid, "Should get volume UUID for test directory")
    }

    func testScanDirectory() async throws {
        var progressReports: [(count: Int, path: String)] = []

        // First, verify test files exist
        let files = try FileManager.default.contentsOfDirectory(atPath: testDirectory.path)
        print("Test directory contains: \(files)")

        try await scanner.scanVolume(path: testDirectory.path) { count, path in
            progressReports.append((count, path))
        }

        // Check that files were indexed
        let fileCount = try dbManager.getFileCount()
        XCTAssertGreaterThan(fileCount, 0, "Should have indexed at least some files, got \(fileCount)")
    }

    func testScanLargeDirectory() async throws {
        // Create more files to test batching
        let largeDir = testDirectory.appendingPathComponent("Large")
        try FileManager.default.createDirectory(at: largeDir, withIntermediateDirectories: true)

        for i in 1...100 {
            let file = largeDir.appendingPathComponent("file_\(i).txt")
            try "Test content".write(to: file, atomically: true, encoding: .utf8)
        }

        let startTime = Date()
        try await scanner.scanVolume(path: testDirectory.path)
        let elapsed = Date().timeIntervalSince(startTime)

        let fileCount = try dbManager.getFileCount()
        XCTAssertEqual(fileCount, 125, "Should have indexed 125 files")

        print("Scanned and indexed 125 files in \(elapsed) seconds")

        // Should be fast
        XCTAssertLessThan(elapsed, 2.0, "Should scan 125 files in under 2 seconds")
    }
}
