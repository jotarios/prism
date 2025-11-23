//
//  SidebarView.swift
//  prism
//

import SwiftUI

struct SidebarView: View {
    @ObservedObject var viewModel: SearchViewModel

    // Filter to show only external volumes
    private var externalVolumes: [VolumeInfo] {
        viewModel.volumes.filter { !$0.isInternal }
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            // Header
            HStack {
                Text("Volumes")
                    .font(.headline)
                Spacer()
                Button(action: {
                    viewModel.loadVolumes()
                }) {
                    Image(systemName: "arrow.clockwise")
                }
                .buttonStyle(.borderless)
                .help("Refresh volumes")
            }
            .padding()

            Divider()

            // Volume list (external only)
            if externalVolumes.isEmpty {
                VStack {
                    Spacer()
                    Text("No external volumes")
                        .foregroundColor(.secondary)
                        .font(.callout)
                    Text("Connect an external drive to index files")
                        .foregroundColor(.secondary)
                        .font(.caption)
                    Spacer()
                }
                .frame(maxWidth: .infinity)
            } else {
                List {
                    ForEach(externalVolumes) { volume in
                        VolumeRow(volume: volume, viewModel: viewModel)
                    }
                }
                .listStyle(.sidebar)
            }

            Divider()

            // Stats footer
            VStack(alignment: .leading, spacing: 4) {
                Text("\(viewModel.totalFilesIndexed) files indexed")
                    .font(.caption)
                    .foregroundStyle(.secondary)

                if viewModel.isScanning {
                    HStack(spacing: 4) {
                        ProgressView()
                            .scaleEffect(0.6)
                        Text(viewModel.scanProgress)
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                }
            }
            .padding()
        }
    }
}

struct VolumeRow: View {
    let volume: VolumeInfo
    @ObservedObject var viewModel: SearchViewModel

    var body: some View {
        HStack {
            Image(systemName: volume.isInternal ? "internaldrive" : "externaldrive.fill")
                .foregroundStyle(volume.isOnline ? .blue : .secondary)

            VStack(alignment: .leading, spacing: 2) {
                Text(volume.name)
                    .font(.body)
                Text(volume.path)
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }

            Spacer()

            Button(action: {
                viewModel.scanVolume(volume)
            }) {
                Image(systemName: "arrow.down.circle")
            }
            .buttonStyle(.borderless)
            .help("Scan volume")
            .disabled(viewModel.isScanning)
        }
        .padding(.vertical, 4)
    }
}

#Preview {
    SidebarView(viewModel: SearchViewModel.shared)
        .frame(width: 250, height: 400)
}
