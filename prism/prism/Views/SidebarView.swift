//
//  SidebarView.swift
//  prism
//

import SwiftUI

struct SidebarView: View {
    @ObservedObject var viewModel: SearchViewModel

    private var externalVolumes: [VolumeInfo] {
        viewModel.volumes.filter { !$0.isInternal }
    }

    private var footerCountText: String {
        let n = viewModel.totalFilesIndexed
        let formatted = n.formatted(.number)
        return "\(formatted) \(n == 1 ? "track" : "tracks")"
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
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
                        VolumeRow(
                            volume: volume,
                            state: viewModel.liveIndexStates[volume.uuid],
                            viewModel: viewModel
                        )
                    }
                }
                .listStyle(.sidebar)
            }

            // Auto-fade when error clears.
            if let err = viewModel.liveIndexError {
                LiveIndexBanner(error: err) {
                    viewModel.retryLiveIndex()
                }
                .padding(.horizontal, 8)
                .padding(.bottom, 8)
                .transition(.opacity)
            }

            Divider()

            VStack(alignment: .leading, spacing: 4) {
                Text(footerCountText)
                    .font(.caption)
                    .foregroundStyle(.secondary)

                if viewModel.isScanning {
                    HStack(spacing: 4) {
                        ProgressView()
                            .scaleEffect(0.6)
                        Text(viewModel.scanProgress)
                            .font(.caption)
                            .foregroundStyle(.secondary)
                            .lineLimit(1)
                            .truncationMode(.tail)
                    }
                }
            }
            .padding()
        }
        .animation(.easeInOut(duration: 0.3), value: viewModel.liveIndexError != nil)
    }
}

#Preview {
    SidebarView(viewModel: SearchViewModel.shared)
        .frame(width: 250, height: 400)
}
