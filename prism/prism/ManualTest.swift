//
//  ManualTest.swift
//  prism
//

import Foundation

class ManualTest {

    static func runScannerTest() async {
        print("=== Prism Scanner Manual Test ===\n")

        // 1. Initialize database
        print("1. Initializing database...")
        let dbManager = DatabaseManager.shared
        do {
            try dbManager.open()
            print("   ✓ Database opened")

            let initialCount = try await dbManager.getFileCount()
            print("   Current file count: \(initialCount)")
        } catch {
            print("   ✗ Database error: \(error)")
            return
        }

        // 2. Check volumes
        print("\n2. Checking mounted volumes...")
        let volumes = VolumeManager.shared.getMountedVolumes()
        print("   Found \(volumes.count) volumes:")
        for volume in volumes {
            print("   - \(volume.name) (\(volume.uuid))")
            print("     Path: \(volume.path)")
            print("     Internal: \(volume.isInternal)")
        }

        // 3. Create test directory with sample files
        print("\n3. Creating test directory with sample files...")
        let testDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("PrismManualTest")

        // Clean up if exists
        try? FileManager.default.removeItem(at: testDir)

        do {
            // Create test structure
            try FileManager.default.createDirectory(at: testDir, withIntermediateDirectories: true)

            let musicDir = testDir.appendingPathComponent("Music")
            let photosDir = testDir.appendingPathComponent("Photos")
            try FileManager.default.createDirectory(at: musicDir, withIntermediateDirectories: true)
            try FileManager.default.createDirectory(at: photosDir, withIntermediateDirectories: true)

            // Create sample audio files
            for i in 1...20 {
                let mp3 = musicDir.appendingPathComponent("song_\(i).mp3")
                try "Sample MP3 data".write(to: mp3, atomically: true, encoding: .utf8)
            }

            // Create non-audio files (should be skipped)
            for i in 1...15 {
                let jpg = photosDir.appendingPathComponent("photo_\(i).jpg")
                try "Sample JPG data".write(to: jpg, atomically: true, encoding: .utf8)
            }

            // Add some text files that should be skipped
            for i in 1...5 {
                let txt = musicDir.appendingPathComponent("notes_\(i).txt")
                try "Sample text".write(to: txt, atomically: true, encoding: .utf8)
            }

            let albumDir = musicDir.appendingPathComponent("Album")
            try FileManager.default.createDirectory(at: albumDir, withIntermediateDirectories: true)
            for i in 1...10 {
                let track = albumDir.appendingPathComponent("track_\(i).flac")
                try "Sample FLAC data".write(to: track, atomically: true, encoding: .utf8)
            }

            // Add some WAV files
            for i in 1...5 {
                let wav = albumDir.appendingPathComponent("master_\(i).wav")
                try "Sample WAV data".write(to: wav, atomically: true, encoding: .utf8)
            }

            print("   ✓ Created test directory with 55 total files")
            print("     - 35 audio files (mp3, flac, wav)")
            print("     - 20 non-audio files (jpg, txt) - should be skipped")
            print("   Path: \(testDir.path)")
        } catch {
            print("   ✗ Failed to create test files: \(error)")
            return
        }

        let scanPath = testDir.path

        // 4. Run scanner
        print("\n4. Starting scan...")
        let scanner = FileScanner()
        var lastCount = 0

        do {
            try await scanner.scanVolume(path: scanPath) { count, path in
                if count > lastCount {
                    print("   Progress: \(count) files indexed...")
                    lastCount = count
                }
            }
            print("   ✓ Scan complete!")
        } catch {
            print("   ✗ Scan error: \(error)")
            return
        }

        // 5. Check results
        print("\n5. Results:")
        do {
            let finalCount = try await dbManager.getFileCount()
            print("   Total files in database: \(finalCount)")

            // Show breakdown by volume
            for volume in volumes {
                let volumeCount = try dbManager.getFileCountByVolume(volume.uuid)
                if volumeCount > 0 {
                    print("   - \(volume.name): \(volumeCount) files")
                }
            }
        } catch {
            print("   ✗ Error getting results: \(error)")
        }

        // 6. Cleanup
        print("\n6. Cleaning up test directory...")
        try? FileManager.default.removeItem(at: testDir)
        print("   ✓ Cleanup complete")

        print("\n=== Test Complete ===")
    }
}
