//
//  SearchBarView.swift
//  prism
//

import SwiftUI

struct SearchBarView: View {
    @ObservedObject var viewModel: SearchViewModel
    @FocusState.Binding var isSearchFocused: Bool

    var body: some View {
        HStack {
            Image(systemName: "magnifyingglass")
                .foregroundStyle(.secondary)

            TextField("Search files...", text: $viewModel.searchQuery)
                .textFieldStyle(.plain)
                .font(.title3)
                .focused($isSearchFocused)
                .onSubmit {
                    // Remove focus when user presses Enter
                    isSearchFocused = false
                }
                .onKeyPress(.escape) {
                    // Remove focus when user presses Escape
                    isSearchFocused = false
                    return .handled
                }

            if !viewModel.searchQuery.isEmpty {
                Button(action: {
                    viewModel.searchQuery = ""
                }) {
                    Image(systemName: "xmark.circle.fill")
                        .foregroundStyle(.secondary)
                }
                .buttonStyle(.borderless)
            }
        }
        .padding(8)
        .background(Color(nsColor: .controlBackgroundColor))
        .cornerRadius(8)
    }
}

private struct SearchBarPreview: View {
    @FocusState var focused: Bool
    var body: some View {
        SearchBarView(viewModel: SearchViewModel.shared, isSearchFocused: $focused)
            .padding()
            .frame(width: 600)
    }
}

#Preview {
    SearchBarPreview()
}
