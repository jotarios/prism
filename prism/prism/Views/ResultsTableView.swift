//
//  ResultsTableView.swift
//  prism
//

import SwiftUI
import AppKit
import Quartz

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
            .id(viewModel.resultsUpdateID)
            .contextMenu(forSelectionType: SearchResult.ID.self) { items in
                contextMenuContent(for: items)
            } primaryAction: { items in
                // Double-click action
                if let id = items.first,
                   let result = sortedResults.first(where: { $0.id == id }) {
                    quickLookURL = URL(fileURLWithPath: result.path)
                }
            }
            .onKeyPress(" ") {
                handleSpaceKey()
                return .handled
            }
            .onChange(of: selection) {
                quickLookURL = nil
            }
            .onChange(of: viewModel.resultsUpdateID) {
                // Clear selection when results update
                Task { @MainActor in
                    selection.removeAll()
                }
            }
            .sheet(item: quickLookBinding) { item in
                QuickLookPreviewView(url: item.url)
                    .frame(minWidth: 700, minHeight: 500)
            }
    }

    private func createTable() -> some View {
        Table(sortedResults, selection: $selection, sortOrder: $sortOrder) {
            TableColumn("Name", value: \.filename) { result in
                HStack(spacing: 8) {
                    Image(systemName: "music.note")
                        .foregroundStyle(.blue)
                    Text(result.filename)
                }
            }
            .width(min: 200, ideal: 300)

            TableColumn("Date Modified", value: \.dateModified) { result in
                Text(result.dateModified.formatted(date: .abbreviated, time: .shortened))
                    .monospacedDigit()
            }
            .width(min: 140, ideal: 160)

            TableColumn("Size", value: \.sizeBytes) { result in
                Text(result.formattedSize)
                    .monospacedDigit()
            }
            .width(min: 80, ideal: 100)

            TableColumn("Duration") { result in
                Text(result.formattedDuration)
                    .monospacedDigit()
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
