//
//  BulkScannerTests.swift
//  prismTests
//

import XCTest
@testable import prism

final class BulkScannerTests: XCTestCase {

    var testDirectory: URL!

    override func setUp() async throws {
        testDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent("PrismBulkScanTest_\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: testDirectory, withIntermediateDirectories: true)
    }

    override func tearDown() async throws {
        try? FileManager.default.removeItem(at: testDirectory)
    }

    private func createFile(_ name: String, in dir: URL? = nil) throws {
        let target = dir ?? testDirectory!
        try "data".write(to: target.appendingPathComponent(name), atomically: true, encoding: .utf8)
    }

    func testScansAudioFiles() throws {
        try createFile("song.mp3")
        try createFile("track.wav")
        try createFile("beat.flac")

        let result = BulkScanner.scanDirectory(atPath: testDirectory.path)
        XCTAssertEqual(result.audioFiles.count, 3)
        let names = Set(result.audioFiles.map(\.filename))
        XCTAssertTrue(names.contains("song.mp3"))
        XCTAssertTrue(names.contains("track.wav"))
        XCTAssertTrue(names.contains("beat.flac"))
    }

    func testSkipsNonAudioFiles() throws {
        try createFile("photo.jpg")
        try createFile("doc.pdf")
        try createFile("notes.txt")
        try createFile("song.mp3")

        let result = BulkScanner.scanDirectory(atPath: testDirectory.path)
        XCTAssertEqual(result.audioFiles.count, 1)
        XCTAssertEqual(result.audioFiles[0].filename, "song.mp3")
    }

    func testSkipsHiddenFiles() throws {
        try createFile(".hidden.mp3")
        try createFile(".DS_Store")
        try createFile("visible.mp3")

        let result = BulkScanner.scanDirectory(atPath: testDirectory.path)
        XCTAssertEqual(result.audioFiles.count, 1)
        XCTAssertEqual(result.audioFiles[0].filename, "visible.mp3")
    }

    func testFindsSubdirectories() throws {
        let subdir = testDirectory.appendingPathComponent("subdir")
        try FileManager.default.createDirectory(at: subdir, withIntermediateDirectories: true)
        try createFile("song.mp3")

        let result = BulkScanner.scanDirectory(atPath: testDirectory.path)
        XCTAssertEqual(result.subdirectories.count, 1)
        XCTAssertTrue(result.subdirectories[0].hasSuffix("/subdir"))
    }

    func testDoesNotRecurse() throws {
        let subdir = testDirectory.appendingPathComponent("subdir")
        try FileManager.default.createDirectory(at: subdir, withIntermediateDirectories: true)
        try createFile("nested.mp3", in: subdir)

        let result = BulkScanner.scanDirectory(atPath: testDirectory.path)
        XCTAssertEqual(result.audioFiles.count, 0)
        XCTAssertEqual(result.subdirectories.count, 1)
    }

    func testEmptyDirectory() throws {
        let result = BulkScanner.scanDirectory(atPath: testDirectory.path)
        XCTAssertEqual(result.audioFiles.count, 0)
        XCTAssertEqual(result.subdirectories.count, 0)
    }

    func testInvalidPath() throws {
        let result = BulkScanner.scanDirectory(atPath: "/nonexistent/path/here")
        XCTAssertEqual(result.audioFiles.count, 0)
        XCTAssertEqual(result.subdirectories.count, 0)
    }

    func testExtractExtension() throws {
        XCTAssertEqual(BulkScanner.extractExtension(from: "song.mp3"), "mp3")
        XCTAssertEqual(BulkScanner.extractExtension(from: "artist.name.flac"), "flac")
        XCTAssertEqual(BulkScanner.extractExtension(from: "noext"), "")
        XCTAssertEqual(BulkScanner.extractExtension(from: "trailing."), "")
        XCTAssertEqual(BulkScanner.extractExtension(from: ""), "")
        XCTAssertEqual(BulkScanner.extractExtension(from: ".hidden"), "hidden")
        XCTAssertEqual(BulkScanner.extractExtension(from: "SONG.MP3"), "mp3")
    }

    func testFileMetadata() throws {
        let filePath = testDirectory.appendingPathComponent("test.mp3")
        try Data(count: 4096).write(to: filePath)

        let result = BulkScanner.scanDirectory(atPath: testDirectory.path)
        XCTAssertEqual(result.audioFiles.count, 1)

        let file = result.audioFiles[0]
        XCTAssertEqual(file.filename, "test.mp3")
        XCTAssertEqual(file.ext, "mp3")
        XCTAssertEqual(file.sizeBytes, 4096)
        XCTAssertEqual(file.parentPath, testDirectory.path)
        XCTAssertFalse(file.isDirectory)
        XCTAssertGreaterThan(file.modTimeSec, 0)
        XCTAssertGreaterThan(file.createTimeSec, 0)
    }

    func testMatchesFileManagerCount() throws {
        let audioExts = ["mp3", "wav", "flac", "aac", "m4a", "ogg", "aiff"]
        let nonAudioExts = ["jpg", "png", "txt", "pdf"]

        for i in 0..<30 {
            try createFile("audio_\(i).\(audioExts[i % audioExts.count])")
        }
        for i in 0..<20 {
            try createFile("other_\(i).\(nonAudioExts[i % nonAudioExts.count])")
        }

        let bulkResult = BulkScanner.scanDirectory(atPath: testDirectory.path)

        let fmContents = try FileManager.default.contentsOfDirectory(
            at: testDirectory,
            includingPropertiesForKeys: nil,
            options: [.skipsHiddenFiles]
        )
        let fmAudioCount = fmContents.filter { url in
            BulkScanner.audioExtensions.contains(url.pathExtension.lowercased())
        }.count

        XCTAssertEqual(bulkResult.audioFiles.count, fmAudioCount)
    }
}
