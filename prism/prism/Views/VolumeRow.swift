//
//  VolumeRow.swift
//  prism
//
//  Three visual channels for state: icon color (connection + error),
//  caption text (activity), overlay badge (polling).
//

import SwiftUI

struct VolumeRow: View {
    let volume: VolumeInfo
    let state: LiveIndexState?
    @ObservedObject var viewModel: SearchViewModel
    @State private var fileCount: Int = -1

    /// True when the volume is mounted but has never been indexed.
    /// Distinct from `.offline` (mounted = treated as online for display).
    private var isUnindexed: Bool {
        volume.isOnline && fileCount == 0
    }

    private var mode: LiveIndexState.Mode {
        state?.mode ?? (volume.isOnline ? .listening : .offline)
    }

    private var iconName: String {
        switch mode {
        case .offline: return "externaldrive"
        default: return "externaldrive.fill"
        }
    }

    private var iconColor: Color {
        if isUnindexed { return .secondary }
        switch mode {
        case .listening, .scanning, .polling, .reconnecting: return .blue
        case .offline: return .secondary
        case .error: return .red
        }
    }

    /// Caption is intentionally minimal. Steady states (listening/polling/
    /// reconnecting) show no caption — the index either works or it
    /// doesn't, and "it doesn't" surfaces via the error banner. Only the
    /// states the user can actually act on get text.
    private var captionText: String? {
        if isUnindexed { return "Not scanned yet" }
        switch mode {
        case .scanning: return "Indexing…"
        case .offline: return "Not connected"
        case .error: return "Live index paused"
        case .listening, .reconnecting, .polling: return nil
        }
    }

    private var captionColor: Color {
        switch mode {
        case .error: return .red
        default: return .secondary
        }
    }

    private var accessibilityLabelText: String {
        if let caption = captionText { return "\(volume.name), \(caption)" }
        return volume.name
    }

    var body: some View {
        HStack {
            Image(systemName: iconName)
                .foregroundStyle(iconColor)
                .frame(width: 20, height: 20)

            VStack(alignment: .leading, spacing: 2) {
                Text(volume.name)
                    .font(.body)
                    .foregroundStyle(mode == .offline || isUnindexed ? Color.secondary : Color.primary)
                    .lineLimit(1)
                    .truncationMode(.tail)

                if let caption = captionText {
                    Text(caption)
                        .font(.caption)
                        .foregroundStyle(captionColor)
                        .lineLimit(1)
                }
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
        .accessibilityElement(children: .combine)
        .accessibilityLabel(accessibilityLabelText)
        .task(id: volume.uuid) {
            fileCount = (try? viewModel.getVolumeFileCount(volume.uuid)) ?? 0
        }
        .onChange(of: viewModel.totalFilesIndexed) {
            fileCount = (try? viewModel.getVolumeFileCount(volume.uuid)) ?? 0
        }
    }
}
