//
//  ResultsTableView.swift
//  prism
//

import SwiftUI
import AppKit

struct ResultsTableView: View {
    @ObservedObject var viewModel: SearchViewModel
    @State private var selection = Set<SearchResult.ID>()
    @State private var quickLookURL: URL?
    @State private var sortOrder = [KeyPathComparator(\SearchResult.filename)]

    var body: some View {
        VStack {
            if viewModel.results.isEmpty {
                EmptyStateView()
            } else {
                resultsTable
            }
        }
    }

    // MARK: - Computed Properties

    private var sortedResults: [SearchResult] {
        viewModel.results.sorted(using: sortOrder)
    }

    // MARK: - Subviews

    private var resultsTable: some View {
        createTable()
            .contextMenu(forSelectionType: SearchResult.ID.self) { items in
                contextMenuContent(for: items)
            } primaryAction: { items in
                if let id = items.first,
                   let result = sortedResults.first(where: { $0.id == id }) {
                    if result.isOnline {
                        quickLookURL = URL(fileURLWithPath: result.path)
                    } else {
                        showOfflineAlert(for: result)
                    }
                }
            }
            .onKeyPress(" ") {
                handleSpaceKey()
                return .handled
            }
            // QuickLook lifetime is independent of selection. The user
            // dismisses with Esc / close button. Closing the sheet on
            // selection change conflicts with live-index updates that
            // bump resultsUpdateID and clear selection mid-playback.
            .sheet(item: quickLookBinding) { item in
                QuickLookPreviewView(url: item.url)
                    .frame(minWidth: 700, minHeight: 500)
            }
    }

    private func createTable() -> some View {
        Table(sortedResults, selection: $selection, sortOrder: $sortOrder) {
            TableColumn("Name", value: \.filename) { result in
                HStack(spacing: 8) {
                    ZStack(alignment: .bottomTrailing) {
                        Image(systemName: "music.note")
                            .foregroundStyle(result.isOnline ? Color.blue : Color.secondary)

                        if !result.isOnline {
                            Image(systemName: "minus.circle.fill")
                                .font(.system(size: 8))
                                .foregroundStyle(Color.secondary)
                                .offset(x: 2, y: 2)
                                .accessibilityHidden(true)
                        }
                    }
                    Text(result.filename)
                        .foregroundStyle(result.isOnline ? Color.primary : Color.secondary)
                }
            }
            .width(min: 200, ideal: 300)

            TableColumn("Date Modified", value: \.dateModified) { result in
                Text(result.dateModified.formatted(date: .abbreviated, time: .shortened))
                    .monospacedDigit()
                    .foregroundStyle(result.isOnline ? Color.primary : Color.secondary)
            }
            .width(min: 140, ideal: 160)

            TableColumn("Size", value: \.sizeBytes) { result in
                Text(result.formattedSize)
                    .monospacedDigit()
                    .foregroundStyle(result.isOnline ? Color.primary : Color.secondary)
            }
            .width(min: 80, ideal: 100)

            TableColumn("Duration") { result in
                Text(result.formattedDuration)
                    .monospacedDigit()
                    .foregroundStyle(result.isOnline ? Color.primary : Color.secondary)
            }
            .width(min: 80, ideal: 100)

            TableColumn("Path", value: \.path) { result in
                Text(result.path)
                    .foregroundStyle(.secondary)
                    .font(.caption)
            }
            .width(min: 200)
        }
    }

    private var quickLookBinding: Binding<QuickLookItem?> {
        Binding(
            get: { quickLookURL.map { QuickLookItem(url: $0) } },
            set: { quickLookURL = $0?.url }
        )
    }

    @ViewBuilder
    private func contextMenuContent(for items: Set<SearchResult.ID>) -> some View {
        if items.isEmpty {
            EmptyView()
        } else if items.count == 1 {
            if let result = sortedResults.first(where: { $0.id == items.first }) {
                Button("Show in Finder") {
                    showInFinder(result)
                }
                Button("Open") {
                    openFile(result)
                }
                Divider()
                Button("Copy Path") {
                    copyPath(result)
                }
            }
        } else {
            Button("Show in Finder") {
                for id in items {
                    if let result = sortedResults.first(where: { $0.id == id }) {
                        showInFinder(result)
                    }
                }
            }
        }
    }

    private func handleSpaceKey() {
        if let selectedID = selection.first,
           let result = sortedResults.first(where: { $0.id == selectedID }) {
            quickLookURL = URL(fileURLWithPath: result.path)
        }
    }

    // MARK: - Helper Types

    struct QuickLookItem: Identifiable {
        let id = UUID()
        let url: URL
    }

    // MARK: - Actions

    private func showInFinder(_ result: SearchResult) {
        let url = URL(fileURLWithPath: result.path)
        NSWorkspace.shared.activateFileViewerSelecting([url])
    }

    private func openFile(_ result: SearchResult) {
        let url = URL(fileURLWithPath: result.path)
        NSWorkspace.shared.open(url)
    }

    private func copyPath(_ result: SearchResult) {
        let pasteboard = NSPasteboard.general
        pasteboard.clearContents()
        pasteboard.setString(result.path, forType: .string)
    }

    private func showOfflineAlert(for result: SearchResult) {
        let driveName = (result.volumeUUID as NSString).lastPathComponent
        let alert = NSAlert()
        alert.messageText = "File unavailable"
        alert.informativeText = "This file is on '\(driveName)' which isn't connected. Reconnect the drive to open it."
        alert.alertStyle = .informational
        alert.addButton(withTitle: "Cancel")
        // Placeholder — sidebar scroll-to-volume is Phase 3.1.
        alert.addButton(withTitle: "Show Volume in Sidebar")
        _ = alert.runModal()
    }
}

struct EmptyStateView: View {
    var body: some View {
        VStack(spacing: 16) {
            Image(systemName: "music.note.list")
                .font(.system(size: 64))
                .foregroundStyle(.secondary)

            VStack(spacing: 4) {
                Text("No files indexed yet")
                    .font(.title2)
                Text("Select a volume from the sidebar to start scanning")
                    .font(.body)
                    .foregroundStyle(.secondary)
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }
}

#Preview {
    ResultsTableView(viewModel: SearchViewModel.shared)
        .frame(width: 800, height: 400)
}
