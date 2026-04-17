//
//  ScannerTests.swift
//  prismTests
//

import XCTest
@testable import prism

final class ScannerTests: XCTestCase {

    var testDirectory: URL!

    override func setUp() async throws {
        testDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent("PrismScanTest_\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: testDirectory, withIntermediateDirectories: true)
        try createTestFiles()
    }

    override func tearDown() async throws {
        try? FileManager.default.removeItem(at: testDirectory)
    }

    private func createTestFiles() throws {
        let musicDir = testDirectory.appendingPathComponent("Music")
        let photosDir = testDirectory.appendingPathComponent("Photos")
        try FileManager.default.createDirectory(at: musicDir, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: photosDir, withIntermediateDirectories: true)

        for i in 1...10 {
            try "data".write(to: musicDir.appendingPathComponent("song_\(i).mp3"), atomically: true, encoding: .utf8)
            try "data".write(to: photosDir.appendingPathComponent("photo_\(i).jpg"), atomically: true, encoding: .utf8)
        }

        let albumDir = musicDir.appendingPathComponent("Album1")
        try FileManager.default.createDirectory(at: albumDir, withIntermediateDirectories: true)
        for i in 1...5 {
            try "data".write(to: albumDir.appendingPathComponent("track_\(i).mp3"), atomically: true, encoding: .utf8)
        }
    }

    func testVolumeManager() throws {
        let volumes = VolumeManager.shared.getMountedVolumes()
        XCTAssertGreaterThan(volumes.count, 0)

        let uuid = VolumeManager.shared.getVolumeUUID(for: testDirectory.path)
        XCTAssertNotNil(uuid)
    }

    func testBulkScanDirectory() throws {
        let musicDir = testDirectory.appendingPathComponent("Music")
        let result = BulkScanner.scanDirectory(atPath: musicDir.path)

        XCTAssertEqual(result.audioFiles.count, 10)
        XCTAssertEqual(result.subdirectories.count, 1)
    }

    func testFullTreeScan() async throws {
        let coordinator = ParallelScanCoordinator(
            rootPath: testDirectory.path,
            volumeUUID: "TEST-UUID"
        )
        let files = try await coordinator.scan { _, _, _ in }

        // 10 mp3 in Music + 5 mp3 in Album1 = 15
        XCTAssertEqual(files.count, 15)
    }

    func testAudioOnlyFiltering() async throws {
        let coordinator = ParallelScanCoordinator(
            rootPath: testDirectory.path,
            volumeUUID: "TEST-UUID"
        )
        let files = try await coordinator.scan { _, _, _ in }

        for file in files {
            XCTAssertTrue(BulkScanner.audioExtensions.contains(file.ext),
                         "Non-audio file found: \(file.filename)")
        }
    }
}
