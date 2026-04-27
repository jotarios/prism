//
//  SettingsView.swift
//  prism
//

import SwiftUI

struct SettingsView: View {
    @ObservedObject var viewModel: SearchViewModel

    // Mirror the UserDefaults flag locally so the Toggle animates without
    // a round-trip through Combine.
    @State private var liveIndexOn: Bool = SearchViewModel.shared.isLiveIndexEnabled

    private var externalVolumes: [VolumeInfo] {
        viewModel.volumes.filter { !$0.isInternal }
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 20) {
            VStack(alignment: .leading, spacing: 6) {
                Toggle("Keep library up to date automatically", isOn: $liveIndexOn)
                    .onChange(of: liveIndexOn) { _, newValue in
                        viewModel.setLiveIndexEnabled(newValue)
                    }
                Text(liveIndexOn
                    ? "New, changed, and deleted tracks appear automatically."
                    : "Tracks only update when you click Scan.")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }

            Divider()

            Text("Drives")
                .font(.headline)

            if externalVolumes.isEmpty {
                Text("No external drives connected")
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
        }
        .padding()
        .frame(minWidth: 500, minHeight: 400)
    }
}

struct VolumeSettingsRow: View {
    let volume: VolumeInfo
    @ObservedObject var viewModel: SearchViewModel

    @State private var fileCount: Int = 0
    @State private var lastScannedAt: Date?
    @State private var showRemoveConfirm = false

    private var trackCountText: String {
        let formatted = fileCount.formatted(.number)
        return "\(formatted) \(fileCount == 1 ? "track" : "tracks")"
    }

    /// Fallback text when there's no timestamp. If the drive has indexed
    /// files, the scan happened before we tracked timestamps (pre-migration).
    /// Don't claim "never" — it'd be wrong.
    private var lastScannedFallback: String {
        fileCount > 0 ? "Indexed" : "Not scanned yet"
    }

    private static let relativeFormatter: RelativeDateTimeFormatter = {
        let f = RelativeDateTimeFormatter()
        f.unitsStyle = .full
        return f
    }()

    /// Collapse to a single largest unit ("4 minutes ago") and round seconds
    /// up to "moments ago" so we never show two-unit strings like
    /// "4 min, 37 seconds ago".
    private func lastScannedString(at last: Date, now: Date) -> String {
        let elapsed = now.timeIntervalSince(last)
        if elapsed < 60 {
            return "Last scanned a few seconds ago"
        }
        return "Last scanned \(Self.relativeFormatter.localizedString(for: last, relativeTo: now))"
    }

    @ViewBuilder
    private var lastScannedView: some View {
        if !volume.isOnline {
            Text("Not connected")
        } else if let last = lastScannedAt {
            // TimelineView re-renders on its own schedule so the relative
            // string stays fresh as wall-clock time advances. Tick every
            // 30s — fine-grained enough to flip from the under-60s string
            // to "1 minute" without being wasteful.
            TimelineView(.periodic(from: .now, by: 30)) { context in
                Text(lastScannedString(at: last, now: context.date))
            }
        } else {
            Text(lastScannedFallback)
        }
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                Image(systemName: volume.isInternal ? "internaldrive" : "externaldrive")
                    .font(.title2)
                    .foregroundStyle(volume.isOnline ? Color.blue : Color.secondary)

                VStack(alignment: .leading, spacing: 4) {
                    Text(volume.name)
                        .font(.headline)
                        .foregroundStyle(volume.isOnline ? Color.primary : Color.secondary)

                    Text(volume.path)
                        .font(.caption)
                        .foregroundColor(.secondary)
                }

                Spacer()

                VStack(alignment: .trailing, spacing: 4) {
                    Text(trackCountText)
                        .font(.subheadline)
                        .foregroundColor(.secondary)

                    lastScannedView
                        .font(.caption)
                        .foregroundColor(.secondary)
                }

                ProgressView()
                    .scaleEffect(0.6)
                    .frame(width: 16, height: 16)
                    .opacity(viewModel.isScanning ? 1 : 0)
                    .padding(.leading, 6)
            }

            HStack {
                Button("Scan") {
                    scanVolume()
                }
                .buttonStyle(.borderedProminent)
                .disabled(viewModel.isScanning || !volume.isOnline)

                Button("Force rebuild") {
                    rebuildVolume()
                }
                .buttonStyle(.bordered)
                .help("Use this if search results look wrong.")
                .disabled(viewModel.isScanning || !volume.isOnline)

                Button("Remove from library") {
                    showRemoveConfirm = true
                }
                .buttonStyle(.bordered)
                .tint(.red)
                .disabled(viewModel.isScanning)
            }
        }
        .padding(.vertical, 8)
        .task {
            await loadCounts()
        }
        // Re-fetch when total count changes (scan added/removed files) AND
        // when scanning flips false (scan finished, even if count stayed
        // the same — e.g. a no-op rescan still updates last_scanned_at).
        .onChange(of: viewModel.totalFilesIndexed) {
            Task { await loadCounts() }
        }
        .onChange(of: viewModel.isScanning) { _, scanning in
            if !scanning {
                Task { await loadCounts() }
            }
        }
        .confirmationDialog(
            "Remove ‘\(volume.name)’ from your library?",
            isPresented: $showRemoveConfirm,
            titleVisibility: .visible
        ) {
            Button("Remove", role: .destructive) { removeVolume() }
            Button("Cancel", role: .cancel) { }
        } message: {
            Text("Tracks on this drive will no longer appear in search. Your audio files won't be deleted.")
        }
    }

    private func scanVolume() {
        // The Scan button just kicks off the scan; viewModel.isScanning
        // tracks state, and our .onChange(of: isScanning) reloads counts
        // when it finishes. No sleep/poll hack needed.
        viewModel.scanVolume(volume)
    }

    private func rebuildVolume() {
        Task {
            await runRemove()
            viewModel.scanVolume(volume)
        }
    }

    private func removeVolume() {
        Task {
            await runRemove()
            await loadCounts()
        }
    }

    private func runRemove() async {
        do {
            try viewModel.clearVolumeFiles(volume.uuid)
            await viewModel.performSearch(viewModel.searchQuery)
            let totalCount = try viewModel.getStoredFileCount()
            await MainActor.run {
                viewModel.totalFilesIndexed = totalCount
            }
        } catch {
            Log.error("Failed to remove volume \(volume.name): \(error)")
        }
    }

    private func loadCounts() async {
        let count = (try? viewModel.getVolumeFileCount(volume.uuid)) ?? 0
        let last = try? viewModel.getVolumeLastScannedAt(volume.uuid)
        await MainActor.run {
            self.fileCount = count
            self.lastScannedAt = last
        }
    }
}
