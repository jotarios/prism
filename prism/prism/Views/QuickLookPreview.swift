//
//  QuickLookPreview.swift
//  prism
//

import SwiftUI
import Quartz

struct QuickLookPreviewView: View {
    let url: URL
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        VStack(spacing: 0) {
            // Header with close button
            HStack {
                Text(url.lastPathComponent)
                    .font(.headline)
                    .lineLimit(1)
                Spacer()
                Button {
                    dismiss()
                } label: {
                    Image(systemName: "xmark.circle.fill")
                        .font(.title2)
                        .foregroundStyle(.secondary)
                }
                .buttonStyle(.plain)
                .help("Close (Esc)")
            }
            .padding()
            .background(Color(nsColor: .windowBackgroundColor))

            Divider()

            // QuickLook preview
            QuickLookPreview(url: url)
        }
        .onKeyPress(.escape) {
            dismiss()
            return .handled
        }
    }
}

struct QuickLookPreview: NSViewRepresentable {
    let url: URL?

    func makeNSView(context: Context) -> QLPreviewView {
        let preview = QLPreviewView()
        preview.autostarts = true
        return preview
    }

    func updateNSView(_ nsView: QLPreviewView, context: Context) {
        if let url = url {
            nsView.previewItem = url as QLPreviewItem
        } else {
            nsView.previewItem = nil
        }
    }
}
