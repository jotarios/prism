//
//  FSEventsStreamHandle.swift
//  prism
//
//  Wrapper around FSEventStream's C API. The callback lives with
//  LiveIndexCoordinator (needs Unmanaged<LiveIndexCoordinator>); this type
//  just owns create/start/teardown.
//

import Foundation
import CoreServices

/// FSEvents context retain/release for an Unmanaged<AnyObject> pointer.
/// Retains the target while the stream is alive so the C callback can
/// safely deref it; release happens automatically when the stream itself
/// is released via FSEventStreamRelease.
private let fsEventsRetain: CFAllocatorRetainCallBack = { ptr in
    guard let ptr else { return nil }
    _ = Unmanaged<AnyObject>.fromOpaque(ptr).retain()
    return ptr
}

private let fsEventsRelease: CFAllocatorReleaseCallBack = { ptr in
    guard let ptr else { return }
    Unmanaged<AnyObject>.fromOpaque(ptr).release()
}

struct FSEventsStreamHandle {
    private let stream: FSEventStreamRef

    // File-level events + fire-on-first-event + RootChanged on unmount.
    // UseCFTypes is required so the callback receives eventPaths as a
    // CFArray<CFString> instead of the default `char**` C-string array.
    // Without this, treating eventPaths as a CFArray crashes immediately.
    static let flags: UInt32 =
        UInt32(kFSEventStreamCreateFlagUseCFTypes) |
        UInt32(kFSEventStreamCreateFlagFileEvents) |
        UInt32(kFSEventStreamCreateFlagNoDefer) |
        UInt32(kFSEventStreamCreateFlagWatchRoot)

    static let latency: CFTimeInterval = 0.1

    init?(
        path: String,
        sinceWhen: FSEventStreamEventId,
        callback: FSEventStreamCallback,
        callbackInfo: UnsafeMutableRawPointer
    ) {
        // Pass retain/release so FSEvents holds a strong ref to the target
        // for the stream's lifetime. Without this, an in-flight callback can
        // deref a freed actor and segfault.
        var context = FSEventStreamContext(
            version: 0,
            info: callbackInfo,
            retain: fsEventsRetain,
            release: fsEventsRelease,
            copyDescription: nil
        )
        let paths = [path] as CFArray

        guard let stream = FSEventStreamCreate(
            kCFAllocatorDefault,
            callback,
            &context,
            paths,
            sinceWhen,
            Self.latency,
            Self.flags
        ) else {
            return nil
        }
        self.stream = stream
    }

    /// Returns false if start failed; caller should call tearDown().
    func start(on queue: DispatchQueue) -> Bool {
        FSEventStreamSetDispatchQueue(stream, queue)
        return FSEventStreamStart(stream)
    }

    func tearDown() {
        FSEventStreamStop(stream)
        FSEventStreamInvalidate(stream)
        FSEventStreamRelease(stream)
    }
}
