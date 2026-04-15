//
//  SettingsView.swift
//  prism
//

import SwiftUI

struct SettingsView: View {
    @ObservedObject var viewModel: SearchViewModel

    // Filter to show only external volumes
    private var externalVolumes: [VolumeInfo] {
        viewModel.volumes.filter { !$0.isInternal }
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 20) {
            Text("Indexed Volumes")
                .font(.headline)

            if externalVolumes.isEmpty {
                Text("No external volumes detected")
                    .foregroundColor(.secondary)
                    .padding()
            } else {
                List {
                    ForEach(externalVolumes) { volume in
                        VolumeSettingsRow(
                            volume: volume,
                            viewModel: viewModel
                        )
                    }
                }
            }

            Spacer()

            HStack {
                Spacer()
                Button("Refresh Volumes") {
                    viewModel.loadVolumes()
                }
                .buttonStyle(.bordered)
            }
        }
        .padding()
        .frame(minWidth: 500, minHeight: 400)
    }
}

struct VolumeSettingsRow: View {
    let volume: VolumeInfo
    @ObservedObject var viewModel: SearchViewModel

    @State private var isIndexing = false
    @State private var fileCount: Int = 0

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                // Volume icon and name
                Image(systemName: volume.isInternal ? "internaldrive" : "externaldrive")
                    .font(.title2)

                VStack(alignment: .leading, spacing: 4) {
                    Text(volume.name)
                        .font(.headline)

                    Text(volume.path)
                        .font(.caption)
                        .foregroundColor(.secondary)

                    Text("UUID: \(volume.uuid)")
                        .font(.caption2)
                        .foregroundColor(.secondary)
                }

                Spacer()

                // File count
                VStack(alignment: .trailing, spacing: 4) {
                    Text("\(fileCount) files")
                        .font(.subheadline)
                        .foregroundColor(.secondary)

                    if isIndexing {
                        ProgressView()
                            .scaleEffect(0.7)
                    }
                }
            }

            // Action buttons
            HStack {
                Button("Scan Volume") {
                    scanVolume()
                }
                .buttonStyle(.borderedProminent)
                .disabled(isIndexing || viewModel.isScanning)

                Button("Rebuild Index") {
                    rebuildVolume()
                }
                .buttonStyle(.bordered)
                .disabled(isIndexing || viewModel.isScanning)

                Button("Clear Index") {
                    clearVolume()
                }
                .buttonStyle(.bordered)
                .tint(.red)
                .disabled(isIndexing || viewModel.isScanning)
            }
        }
        .padding(.vertical, 8)
        .task {
            await loadFileCount()
        }
    }

    private func scanVolume() {
        isIndexing = true
        viewModel.scanVolume(volume)

        // Update file count after scan completes
        Task {
            try? await Task.sleep(nanoseconds: 1_000_000_000)
            await loadFileCount()
            isIndexing = false
        }
    }

    private func rebuildVolume() {
        Task {
            isIndexing = true

            // Clear this volume's files
            await clearVolume()

            // Rescan
            viewModel.scanVolume(volume)

            try? await Task.sleep(nanoseconds: 1_000_000_000)
            await loadFileCount()
            isIndexing = false
        }
    }

    private func clearVolume() {
        Task {
            do {
                try viewModel.clearVolumeFiles(volume.uuid)
                await loadFileCount()
                await viewModel.performSearch(viewModel.searchQuery)
                let totalCount = try viewModel.getStoredFileCount()
                await MainActor.run {
                    viewModel.totalFilesIndexed = totalCount
                }
            } catch {
                Log.error("Failed to clear volume \(volume.name): \(error)")
            }
        }
    }

    private func loadFileCount() async {
        do {
            let count = try viewModel.getVolumeFileCount(volume.uuid)
            await MainActor.run {
                self.fileCount = count
            }
        } catch {
            Log.error("Failed to get file count for volume \(volume.name): \(error)")
        }
    }
}
