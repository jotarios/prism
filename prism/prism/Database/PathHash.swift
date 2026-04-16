//
//  PathHash.swift
//  prism
//

import Foundation

/// Stable 64-bit hash of a (volume_uuid, path) pair, used as the primary key
/// for files across DuckDB, SQLite, and the FTS5 index.
///
/// Requirements:
///   * Deterministic across runs, processes, and machines. Swift's built-in
///     `Hasher` is explicitly randomized per process, so we cannot use it.
///   * Same input → same output, forever. Changing the algorithm later would
///     invalidate every existing row's id and force a full re-index.
///   * Distribution good enough to keep 64-bit collisions astronomically rare
///     at our scale (5M files → ~7e-9 expected collisions per scan).
///
/// Algorithm: FNV-1a 64-bit. Simple, stateless, no external key material,
/// well-understood distribution on strings. The final cast to `Int64` uses
/// `bitPattern` because DuckDB `BIGINT` is signed; the two-way cast is lossless.
enum PathHash {
    private static let fnvOffset: UInt64 = 0xcbf2_9ce4_8422_2325
    private static let fnvPrime: UInt64 = 0x0000_0100_0000_01b3

    /// Hash a (volumeUUID, path) pair. Uses NUL as a separator to guarantee
    /// that ("ab", "c") and ("a", "bc") hash differently.
    static func id(volumeUUID: String, path: String) -> Int64 {
        var hash = fnvOffset
        for byte in volumeUUID.utf8 {
            hash ^= UInt64(byte)
            hash = hash &* fnvPrime
        }
        hash ^= 0                    // NUL separator
        hash = hash &* fnvPrime
        for byte in path.utf8 {
            hash ^= UInt64(byte)
            hash = hash &* fnvPrime
        }
        return Int64(bitPattern: hash)
    }
}
