//
//  LiveIndexBanner.swift
//  prism
//

import SwiftUI

struct LiveIndexBanner: View {
    let error: LiveIndexError
    let onRetry: () -> Void

    private var message: String {
        error.errorDescription ?? "Live index paused"
    }

    var body: some View {
        HStack(spacing: 8) {
            Image(systemName: "exclamationmark.triangle.fill")
                .foregroundStyle(.red)
                .font(.system(size: 12))

            Text(message)
                .font(.caption)
                .foregroundStyle(.primary)
                .lineLimit(1)
                .truncationMode(.tail)
                .help(message)

            Spacer()

            Button("Retry", action: onRetry)
                .buttonStyle(.bordered)
                .controlSize(.small)
        }
        .padding(.vertical, 8)
        .padding(.horizontal, 12)
        .background(
            RoundedRectangle(cornerRadius: 6)
                .fill(Color.red.opacity(0.08))
        )
        .accessibilityElement(children: .combine)
        .accessibilityAddTraits(.isStaticText)
        .accessibilityLabel("Live index paused. \(message). Retry available.")
    }
}
