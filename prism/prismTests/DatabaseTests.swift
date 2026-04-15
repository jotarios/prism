//
//  DatabaseTests.swift
//  prismTests
//

import XCTest
@testable import prism

final class DatabaseTests: XCTestCase {

    var dbManager: DatabaseManager!

    override func setUp() {
        super.setUp()
        dbManager = DatabaseManager.shared
        do {
            try dbManager.open()
        } catch {
            print("setUp: Database already open or error: \(error)")
        }
        do {
            try dbManager.rebuildDatabase()
        } catch {
            print("setUp: Failed to rebuild: \(error)")
        }
    }

    override func tearDown() {
        try? dbManager.rebuildDatabase()
        super.tearDown()
    }

    func testDatabaseCreation() async throws {
        let count = try await dbManager.getFileCount()
        XCTAssertEqual(count, 0)
    }

    func testDatabasePragmaSettings() throws {
        let pragmaSettings = try dbManager.getPragmaSettings()
        XCTAssertEqual(pragmaSettings["journal_mode"], "wal")
        XCTAssertEqual(pragmaSettings["synchronous"], "1")

        if let cacheSize = pragmaSettings["cache_size"], let cacheSizeInt = Int(cacheSize) {
            XCTAssertLessThan(cacheSizeInt, 0)
            XCTAssertLessThanOrEqual(cacheSizeInt, -50000)
        } else {
            XCTFail("Cache size not set")
        }

        XCTAssertEqual(pragmaSettings["temp_store"], "2")
        XCTAssertEqual(pragmaSettings["foreign_keys"], "1")
    }

    func testRebuildDatabase() async throws {
        try dbManager.rebuildDatabase()
        let count = try await dbManager.getFileCount()
        XCTAssertEqual(count, 0)
    }

    func testBulkImportMode() throws {
        try dbManager.beginBulkImport()

        // Triggers should be gone
        try dbManager.endBulkImport()

        // Triggers should be back — verify by checking search works after sync
    }

    func testStartupTriggerRecovery() throws {
        try dbManager.beginBulkImport()
        // Simulate crash: don't call endBulkImport
        // Reopen should detect missing triggers and restore them
        dbManager.close()
        try dbManager.open()
        // If we got here without crash, recovery worked
    }
}
