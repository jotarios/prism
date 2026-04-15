//
//  Log.swift
//  prism
//

import Foundation
import os

enum Log {
    private static let logger = Logger(subsystem: "com.jotarios.prism", category: "general")

    static func debug(_ message: String) {
        #if DEBUG
        logger.notice("\(message)")
        #endif
    }

    static func info(_ message: String) {
        logger.info("\(message)")
    }

    static func error(_ message: String) {
        logger.error("\(message)")
    }
}
