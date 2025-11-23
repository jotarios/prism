//
//  prismApp.swift
//  prism
//
//  Created by Jorge Rios on 23/11/2025.
//

import SwiftUI

@main
struct prismApp: App {
    var body: some Scene {
        WindowGroup {
            MainWindow()
                .preferredColorScheme(.dark)
                .frame(minWidth: 900, minHeight: 600)
        }
        .commands {
            CommandGroup(replacing: .newItem) { }
        }

        // Settings window
        Settings {
            SettingsView(viewModel: SearchViewModel.shared)
                .frame(minWidth: 600, minHeight: 500)
        }
    }
}
