//
//  BulkScanner.swift
//  prism
//

import Foundation
import Darwin

struct ScannedFile: Sendable {
    let filename: String
    let parentPath: String
    let ext: String
    let sizeBytes: Int64
    let modTimeSec: Int
    let createTimeSec: Int
    let isDirectory: Bool
}

struct DirectoryScanResult: Sendable {
    let audioFiles: [ScannedFile]
    let subdirectories: [String]
}

final class BulkScanner {
    static let audioExtensions: Set<String> = [
        "mp3", "wav", "flac", "aac", "m4a", "ogg", "wma",
        "aiff", "aif", "ape", "opus", "alac", "dsd", "dsf",
        "mp2", "mpc", "wv", "tta", "ac3", "dts"
    ]

    /// Directories whose contents are (almost) never audio. Basename-matched.
    /// Dev-artifact names kept broad but conservative — nothing a user might
    /// plausibly store music under. A future Phase 5 settings pane can make
    /// this configurable per-volume.
    ///
    /// Note: any directory whose basename starts with `.` or `$` is already
    /// skipped by the per-entry filter below, so `.git`, `.cache`, `.npm`,
    /// `$RECYCLE.BIN` etc. don't need entries here.
    static let skippedDirectories: Set<String> = [
        // Windows / Samba volume metadata
        "$RECYCLE.BIN", "System Volume Information",
        // macOS volume metadata
        ".Trashes", ".Spotlight-V100", ".fseventsd", ".TemporaryItems",
        // DJ tool metadata
        "_Serato_",
        // Developer artifacts — large, nested, no audio
        "node_modules", "Pods", "DerivedData", "build",
        "target", "vendor", "venv", ".venv", "__pycache__"
    ]

    private static let initialBufferSize = 256 * 1024
    private static let maxBufferSize = 8 * 1024 * 1024

    static func scanDirectory(atPath path: String) -> DirectoryScanResult {
        let fd = open(path, O_RDONLY | O_DIRECTORY)
        guard fd >= 0 else {
            return DirectoryScanResult(audioFiles: [], subdirectories: [])
        }
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

        var bufferSize = initialBufferSize
        var buffer = UnsafeMutableRawPointer.allocate(byteCount: bufferSize, alignment: 8)
        defer { buffer.deallocate() }

        var audioFiles: [ScannedFile] = []
        var subdirectories: [String] = []

        while true {
            let count = getattrlistbulk(fd, &attrList, buffer, bufferSize, 0)
            if count == 0 { break }
            if count < 0 {
                // ERANGE means a single entry didn't fit the buffer. Grow it
                // (up to maxBufferSize) and retry. Same-size retry would just
                // loop forever and silently truncate the directory.
                if errno == ERANGE && bufferSize < maxBufferSize {
                    buffer.deallocate()
                    bufferSize = min(bufferSize * 2, maxBufferSize)
                    buffer = UnsafeMutableRawPointer.allocate(byteCount: bufferSize, alignment: 8)
                    continue
                }
                Log.error("getattrlistbulk failed at \(path): errno=\(errno)")
                break
            }

            var ptr = buffer
            for _ in 0..<count {
                let entryStart = ptr
                let entryLength = Int(ptr.loadUnaligned(as: UInt32.self))
                ptr = ptr.advanced(by: 4)

                let returnedCommon = ptr.loadUnaligned(as: UInt32.self)
                let returnedFile = ptr.advanced(by: 12).loadUnaligned(as: UInt32.self)
                ptr = ptr.advanced(by: 20)

                var entryError: UInt32 = 0
                if returnedCommon & UInt32(bitPattern: ATTR_CMN_ERROR) != 0 {
                    entryError = ptr.loadUnaligned(as: UInt32.self)
                    ptr = ptr.advanced(by: 4)
                }
                if entryError != 0 {
                    ptr = entryStart.advanced(by: entryLength)
                    continue
                }

                var filename = ""
                if returnedCommon & UInt32(bitPattern: ATTR_CMN_NAME) != 0 {
                    let nameRefPtr = ptr
                    let nameOffset = nameRefPtr.loadUnaligned(as: Int32.self)
                    let nameLength = nameRefPtr.advanced(by: 4).loadUnaligned(as: UInt32.self)
                    let namePtr = nameRefPtr.advanced(by: Int(nameOffset))
                    let nameEnd = nameRefPtr.advanced(by: Int(nameOffset) + Int(nameLength))
                    let entryEnd = entryStart.advanced(by: entryLength)
                    if nameLength > 1 && nameEnd <= entryEnd {
                        filename = String(
                            decoding: UnsafeBufferPointer(start: namePtr.assumingMemoryBound(to: UInt8.self),
                                                          count: Int(nameLength) - 1),
                            as: UTF8.self
                        )
                    }
                    ptr = ptr.advanced(by: 8)
                }

                var objType: UInt32 = 0
                if returnedCommon & UInt32(bitPattern: ATTR_CMN_OBJTYPE) != 0 {
                    objType = ptr.loadUnaligned(as: UInt32.self)
                    ptr = ptr.advanced(by: 4)
                }

                var crtimeSec: Int = 0
                if returnedCommon & UInt32(bitPattern: ATTR_CMN_CRTIME) != 0 {
                    crtimeSec = ptr.loadUnaligned(as: Int.self)
                    ptr = ptr.advanced(by: MemoryLayout<timespec>.size)
                }

                var modtimeSec: Int = 0
                if returnedCommon & UInt32(bitPattern: ATTR_CMN_MODTIME) != 0 {
                    modtimeSec = ptr.loadUnaligned(as: Int.self)
                    ptr = ptr.advanced(by: MemoryLayout<timespec>.size)
                }

                var dataLength: Int64 = 0
                if returnedFile & UInt32(bitPattern: ATTR_FILE_DATALENGTH) != 0 {
                    dataLength = ptr.loadUnaligned(as: off_t.self)
                }

                ptr = entryStart.advanced(by: entryLength)

                guard !filename.isEmpty, !filename.hasPrefix("."), !filename.hasPrefix("$") else { continue }

                if objType == 2 { // VDIR
                    guard !skippedDirectories.contains(filename) else { continue }
                    subdirectories.append(path + "/" + filename)
                } else if objType == 1 { // VREG
                    let ext = extractExtension(from: filename)
                    guard audioExtensions.contains(ext) else { continue }

                    audioFiles.append(ScannedFile(
                        filename: filename,
                        parentPath: path,
                        ext: ext,
                        sizeBytes: dataLength,
                        modTimeSec: modtimeSec,
                        createTimeSec: crtimeSec,
                        isDirectory: false
                    ))
                }
            }
        }

        return DirectoryScanResult(audioFiles: audioFiles, subdirectories: subdirectories)
    }

    static func extractExtension(from filename: String) -> String {
        guard let dotIndex = filename.lastIndex(of: ".") else { return "" }
        let afterDot = filename.index(after: dotIndex)
        guard afterDot < filename.endIndex else { return "" }
        return String(filename[afterDot...]).lowercased()
    }
}
