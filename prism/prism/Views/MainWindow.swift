//
//  MainWindow.swift
//  prism
//

import SwiftUI

struct MainWindow: View {
    @ObservedObject var viewModel = SearchViewModel.shared

    var body: some View {
        NavigationSplitView {
            // Sidebar
            SidebarView(viewModel: viewModel)
                .navigationSplitViewColumnWidth(min: 200, ideal: 250, max: 300)
        } detail: {
            // Main content area
            VStack(spacing: 0) {
                // Search bar
                SearchBarView(viewModel: viewModel)
                    .padding()

                Divider()

                // Results table
                ResultsTableView(viewModel: viewModel)
            }
        }
        .navigationTitle("Prism")
    }
}

#Preview {
    MainWindow()
        .frame(width: 1000, height: 600)
}
