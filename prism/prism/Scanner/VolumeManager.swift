//
//  VolumeManager.swift
//  prism
//

import Foundation

final class VolumeManager {
    static let shared = VolumeManager()

    private init() {}

    /// Get all mounted volumes
    func getMountedVolumes() -> [VolumeInfo] {
        var volumes: [VolumeInfo] = []

        guard let volumeURLs = FileManager.default.mountedVolumeURLs(
            includingResourceValuesForKeys: [
                .volumeNameKey,
                .volumeUUIDStringKey,
                .volumeIsInternalKey,
                .volumeIsRemovableKey
            ],
            options: [.skipHiddenVolumes]
        ) else {
            return []
        }

        for url in volumeURLs {
            do {
                let resourceValues = try url.resourceValues(forKeys: [
                    .volumeNameKey,
                    .volumeUUIDStringKey,
                    .volumeIsInternalKey
                ])

                guard let volumeName = resourceValues.volumeName,
                      let volumeUUID = resourceValues.volumeUUIDString else {
                    continue
                }

                let isInternal = resourceValues.volumeIsInternal ?? false

                let volumeInfo = VolumeInfo(
                    uuid: volumeUUID,
                    name: volumeName,
                    path: url.path,
                    isInternal: isInternal,
                    isOnline: true
                )

                volumes.append(volumeInfo)
            } catch {
                print("Error reading volume info for \(url.path): \(error)")
                continue
            }
        }

        return volumes
    }

    /// Get volume UUID for a given path
    func getVolumeUUID(for path: String) -> String? {
        let url = URL(fileURLWithPath: path)

        do {
            let resourceValues = try url.resourceValues(forKeys: [.volumeUUIDStringKey])
            return resourceValues.volumeUUIDString
        } catch {
            print("Error getting volume UUID for \(path): \(error)")
            return nil
        }
    }

    /// Check if a volume with given UUID is currently mounted
    func isVolumeMounted(uuid: String) -> Bool {
        let mounted = getMountedVolumes()
        return mounted.contains { $0.uuid == uuid }
    }

    /// Get mount path for a volume UUID
    func getMountPath(for uuid: String) -> String? {
        let mounted = getMountedVolumes()
        return mounted.first { $0.uuid == uuid }?.path
    }
}
