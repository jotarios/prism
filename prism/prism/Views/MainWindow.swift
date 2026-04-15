//
//  MainWindow.swift
//  prism
//

import SwiftUI
import AppKit

struct MainWindow: View {
    @ObservedObject var viewModel = SearchViewModel.shared
    @FocusState private var isSearchFocused: Bool

    var body: some View {
        NavigationSplitView {
            SidebarView(viewModel: viewModel)
                .navigationSplitViewColumnWidth(min: 200, ideal: 250, max: 300)
        } detail: {
            VStack(spacing: 0) {
                SearchBarView(viewModel: viewModel, isSearchFocused: $isSearchFocused)
                    .padding()

                Divider()

                ResultsTableView(viewModel: viewModel)
                    .background(
                        MouseDownCatcher {
                            if isSearchFocused { isSearchFocused = false }
                        }
                    )
            }
        }
        .navigationTitle("Prism")
    }
}

private struct MouseDownCatcher: NSViewRepresentable {
    let onMouseDown: () -> Void

    func makeNSView(context: Context) -> CatcherView {
        let view = CatcherView()
        view.onMouseDown = onMouseDown
        return view
    }

    func updateNSView(_ nsView: CatcherView, context: Context) {
        nsView.onMouseDown = onMouseDown
    }

    final class CatcherView: NSView {
        var onMouseDown: (() -> Void)?
        private var monitor: Any?

        override func viewDidMoveToWindow() {
            super.viewDidMoveToWindow()
            if let monitor { NSEvent.removeMonitor(monitor); self.monitor = nil }
            guard let window else { return }
            monitor = NSEvent.addLocalMonitorForEvents(matching: .leftMouseDown) { [weak self] event in
                guard let self, event.window === window else { return event }
                let locationInSelf = self.convert(event.locationInWindow, from: nil)
                guard self.bounds.contains(locationInSelf) else { return event }

                // If a text field currently has focus, resign it and re-post
                // the click so AppKit routes it freshly to the Table row.
                if let responder = window.firstResponder,
                   responder is NSTextView || responder is NSText {
                    window.makeFirstResponder(nil)
                    self.onMouseDown?()
                    DispatchQueue.main.async {
                        window.postEvent(event, atStart: true)
                    }
                    return nil
                }
                self.onMouseDown?()
                return event
            }
        }

        deinit {
            if let monitor { NSEvent.removeMonitor(monitor) }
        }
    }
}

#Preview {
    MainWindow()
        .frame(width: 1000, height: 600)
}
