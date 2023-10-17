//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2021-2021 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

@testable import Hummingbird
import HummingbirdXCT
import XCTest

final class PersistTests: XCTestCase {
    static let redisHostname = HBEnvironment.shared.get("REDIS_HOSTNAME") ?? "localhost"

    func createApplication() throws -> (HBApplicationBuilder<HBTestRouterContext>, HBPersistDriver) {
        let app = HBApplicationBuilder(context: HBTestRouterContext.self)
        let persist = HBMemoryPersistDriver(eventLoopGroup: app.eventLoopGroup)

        app.router.put("/persist/:tag") { request, context -> EventLoopFuture<HTTPResponseStatus> in
            guard let tag = context.router.parameters.get("tag") else { return context.failure(.badRequest) }
            guard let buffer = request.body.buffer else { return context.failure(.badRequest) }
            return persist.set(key: tag, value: String(buffer: buffer), request: request)
                .map { _ in .ok }
        }
        app.router.put("/persist/:tag/:time") { request, context -> EventLoopFuture<HTTPResponseStatus> in
            guard let time = context.router.parameters.get("time", as: Int.self) else { return context.failure(.badRequest) }
            guard let tag = context.router.parameters.get("tag") else { return context.failure(.badRequest) }
            guard let buffer = request.body.buffer else { return context.failure(.badRequest) }
            return persist.set(key: tag, value: String(buffer: buffer), expires: .seconds(numericCast(time)), request: request)
                .map { _ in .ok }
        }
        app.router.get("/persist/:tag") { request, context -> EventLoopFuture<String?> in
            guard let tag = context.router.parameters.get("tag", as: String.self) else { return context.failure(.badRequest) }
            return persist.get(key: tag, as: String.self, request: request)
        }
        app.router.delete("/persist/:tag") { request, context -> EventLoopFuture<HTTPResponseStatus> in
            guard let tag = context.router.parameters.get("tag", as: String.self) else { return context.failure(.badRequest) }
            return persist.remove(key: tag, request: request)
                .map { _ in .noContent }
        }
        return (app, persist)
    }

    func testSetGet() async throws {
        let (app, _) = try createApplication()
        try await app.buildAndTest(.router) { client in
            let tag = UUID().uuidString
            try await client.XCTExecute(uri: "/persist/\(tag)", method: .PUT, body: ByteBufferAllocator().buffer(string: "Persist")) { _ in }
            try await client.XCTExecute(uri: "/persist/\(tag)", method: .GET) { response in
                let body = try XCTUnwrap(response.body)
                XCTAssertEqual(String(buffer: body), "Persist")
            }
        }
    }

    func testCreateGet() async throws {
        let (app, persist) = try createApplication()
        app.router.put("/create/:tag") { request, context -> EventLoopFuture<HTTPResponseStatus> in
            guard let tag = context.router.parameters.get("tag") else { return context.failure(.badRequest) }
            guard let buffer = request.body.buffer else { return context.failure(.badRequest) }
            return persist.create(key: tag, value: String(buffer: buffer), request: request)
                .map { _ in .ok }
        }
        try await app.buildAndTest(.router) { client in
            let tag = UUID().uuidString
            try await client.XCTExecute(uri: "/create/\(tag)", method: .PUT, body: ByteBufferAllocator().buffer(string: "Persist")) { _ in }
            try await client.XCTExecute(uri: "/persist/\(tag)", method: .GET) { response in
                let body = try XCTUnwrap(response.body)
                XCTAssertEqual(String(buffer: body), "Persist")
            }
        }
    }

    func testDoubleCreateFail() async throws {
        let (app, persist) = try createApplication()
        app.router.put("/create/:tag") { request, context -> EventLoopFuture<HTTPResponseStatus> in
            guard let tag = context.router.parameters.get("tag") else { return context.failure(.badRequest) }
            guard let buffer = request.body.buffer else { return context.failure(.badRequest) }
            return persist.create(key: tag, value: String(buffer: buffer), request: request)
                .flatMapErrorThrowing { error in
                    if let error = error as? HBPersistError, error == .duplicate { throw HBHTTPError(.conflict) }
                    throw error
                }
                .map { _ in .ok }
        }
        try await app.buildAndTest(.router) { client in
            let tag = UUID().uuidString
            try await client.XCTExecute(uri: "/create/\(tag)", method: .PUT, body: ByteBufferAllocator().buffer(string: "Persist")) { response in
                XCTAssertEqual(response.status, .ok)
            }
            try await client.XCTExecute(uri: "/create/\(tag)", method: .PUT, body: ByteBufferAllocator().buffer(string: "Persist")) { response in
                XCTAssertEqual(response.status, .conflict)
            }
        }
    }

    func testSetTwice() async throws {
        let (app, _) = try createApplication()
        try await app.buildAndTest(.router) { client in

            let tag = UUID().uuidString
            try await client.XCTExecute(uri: "/persist/\(tag)", method: .PUT, body: ByteBufferAllocator().buffer(string: "test1")) { _ in }
            try await client.XCTExecute(uri: "/persist/\(tag)", method: .PUT, body: ByteBufferAllocator().buffer(string: "test2")) { response in
                XCTAssertEqual(response.status, .ok)
            }
            try await client.XCTExecute(uri: "/persist/\(tag)", method: .GET) { response in
                let body = try XCTUnwrap(response.body)
                XCTAssertEqual(String(buffer: body), "test2")
            }
        }
    }

    func testExpires() async throws {
        let (app, _) = try createApplication()
        try await app.buildAndTest(.router) { client in

            let tag1 = UUID().uuidString
            let tag2 = UUID().uuidString

            try await client.XCTExecute(uri: "/persist/\(tag1)/0", method: .PUT, body: ByteBufferAllocator().buffer(string: "ThisIsTest1")) { _ in }
            try await client.XCTExecute(uri: "/persist/\(tag2)/10", method: .PUT, body: ByteBufferAllocator().buffer(string: "ThisIsTest2")) { _ in }
            try await Task.sleep(nanoseconds: 1_000_000_000)
            try await client.XCTExecute(uri: "/persist/\(tag1)", method: .GET) { response in
                XCTAssertEqual(response.status, .noContent)
            }
            try await client.XCTExecute(uri: "/persist/\(tag2)", method: .GET) { response in
                let body = try XCTUnwrap(response.body)
                XCTAssertEqual(String(buffer: body), "ThisIsTest2")
            }
        }
    }

    func testCodable() async throws {
        struct TestCodable: Codable {
            let buffer: String
        }
        let (app, persist) = try createApplication()

        app.router.put("/codable/:tag") { request, context -> EventLoopFuture<HTTPResponseStatus> in
            guard let tag = context.router.parameters.get("tag") else { return context.failure(.badRequest) }
            guard let buffer = request.body.buffer else { return context.failure(.badRequest) }
            return persist.set(key: tag, value: TestCodable(buffer: String(buffer: buffer)), request: request)
                .map { _ in .ok }
        }
        app.router.get("/codable/:tag") { request, context -> EventLoopFuture<String?> in
            guard let tag = context.router.parameters.get("tag") else { return context.failure(.badRequest) }
            return persist.get(key: tag, as: TestCodable.self, request: request).map { $0.map(\.buffer) }
        }
        try await app.buildAndTest(.router) { client in

            let tag = UUID().uuidString
            try await client.XCTExecute(uri: "/codable/\(tag)", method: .PUT, body: ByteBufferAllocator().buffer(string: "Persist")) { _ in }
            try await client.XCTExecute(uri: "/codable/\(tag)", method: .GET) { response in
                let body = try XCTUnwrap(response.body)
                XCTAssertEqual(String(buffer: body), "Persist")
            }
        }
    }

    func testRemove() async throws {
        let (app, _) = try createApplication()
        try await app.buildAndTest(.router) { client in

            let tag = UUID().uuidString
            try await client.XCTExecute(uri: "/persist/\(tag)", method: .PUT, body: ByteBufferAllocator().buffer(string: "ThisIsTest1")) { _ in }
            try await client.XCTExecute(uri: "/persist/\(tag)", method: .DELETE) { _ in }
            try await client.XCTExecute(uri: "/persist/\(tag)", method: .GET) { response in
                XCTAssertEqual(response.status, .noContent)
            }
        }
    }

    func testExpireAndAdd() async throws {
        let (app, _) = try createApplication()
        try await app.buildAndTest(.router) { client in

            let tag = UUID().uuidString
            try await client.XCTExecute(uri: "/persist/\(tag)/0", method: .PUT, body: ByteBufferAllocator().buffer(string: "ThisIsTest1")) { _ in }
            try await Task.sleep(nanoseconds: 1_000_000_000)
            try await client.XCTExecute(uri: "/persist/\(tag)", method: .GET) { response in
                XCTAssertEqual(response.status, .noContent)
            }
            try await client.XCTExecute(uri: "/persist/\(tag)/10", method: .PUT, body: ByteBufferAllocator().buffer(string: "ThisIsTest1")) { response in
                XCTAssertEqual(response.status, .ok)
            }
            try await client.XCTExecute(uri: "/persist/\(tag)", method: .GET) { response in
                XCTAssertEqual(response.status, .ok)
                let body = try XCTUnwrap(response.body)
                XCTAssertEqual(String(buffer: body), "ThisIsTest1")
            }
        }
    }
}
