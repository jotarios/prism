//
//  prismApp.swift
//  prism
//
//  Created by Jorge Rios on 23/11/2025.
//

import SwiftUI
import AppKit

@main
struct prismApp: App {
    @NSApplicationDelegateAdaptor(AppDelegate.self) var appDelegate

    var body: some Scene {
        WindowGroup {
            MainWindow()
                .preferredColorScheme(.dark)
                .frame(minWidth: 900, minHeight: 600)
        }
        .commands {
            CommandGroup(replacing: .newItem) { }
        }

        Settings {
            SettingsView(viewModel: SearchViewModel.shared)
                .frame(minWidth: 600, minHeight: 500)
        }
    }
}

/// Removes NSWorkspace observers on quit. applicationWillTerminate runs
/// on the main thread, so assumeIsolated is safe.
final class AppDelegate: NSObject, NSApplicationDelegate {
    func applicationWillTerminate(_ notification: Notification) {
        MainActor.assumeIsolated {
            SearchViewModel.shared.tearDownLiveIndex()
        }
    }
}
