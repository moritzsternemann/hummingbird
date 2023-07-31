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

import Hummingbird
@testable import HummingbirdJobs
import HummingbirdXCT
import ServiceLifecycle
import XCTest

final class HummingbirdJobsTests: XCTestCase {
    func wait(for expectations: [XCTestExpectation], timeout: TimeInterval) async {
        #if os(Linux)
        super.wait(for: expectation, timeout: timeout)
        #else
        await fulfillment(of: expectations, timeout: timeout)
        #endif
    }

    func testBasic() async throws {
        struct TestJob: HBJob {
            static let name = "testBasic"
            static let expectation = XCTestExpectation(description: "Jobs Completed")

            let value: Int
            func execute(on eventLoop: EventLoop, logger: Logger) -> EventLoopFuture<Void> {
                return eventLoop.scheduleTask(in: .milliseconds(Int64.random(in: 10..<50))) {
                    Self.expectation.fulfill()
                }.futureResult
            }
        }
        TestJob.register()
        TestJob.expectation.expectedFulfillmentCount = 10

        let app = HBApplication(testing: .live)
        app.logger.logLevel = .trace
        app.addJobs(using: .memory, numWorkers: 1)
        app.router.post("/job/${number}") { request -> HTTPResponseStatus in
            let parameter = try request.parameters.require("number", as: Int.self)
            _ = request.jobs.enqueue(job: TestJob(value: parameter))
            return .ok
        }

        try await app.XCTTest { client in
            try await client.XCTExecute(uri: "/job/1", method: .POST)
            try await client.XCTExecute(uri: "/job/2", method: .POST)
            try await client.XCTExecute(uri: "/job/3", method: .POST)
            try await client.XCTExecute(uri: "/job/4", method: .POST)
            try await client.XCTExecute(uri: "/job/5", method: .POST)
            try await client.XCTExecute(uri: "/job/6", method: .POST)
            try await client.XCTExecute(uri: "/job/7", method: .POST)
            try await client.XCTExecute(uri: "/job/8", method: .POST)
            try await client.XCTExecute(uri: "/job/9", method: .POST)
            try await client.XCTExecute(uri: "/job/10", method: .POST)
            await self.wait(for: [TestJob.expectation], timeout: 5)
        }
    }

    func testMultipleWorkers() async throws {
        struct TestJob: HBJob {
            static let name = "testMultipleWorkers"
            static let expectation = XCTestExpectation(description: "Jobs Completed")

            let value: Int
            func execute(on eventLoop: EventLoop, logger: Logger) -> EventLoopFuture<Void> {
                return eventLoop.scheduleTask(in: .milliseconds(Int64.random(in: 10..<50))) {
                    Self.expectation.fulfill()
                }.futureResult
            }
        }
        TestJob.register()
        TestJob.expectation.expectedFulfillmentCount = 10

        let app = HBApplication(testing: .live)
        app.logger.logLevel = .trace
        app.addJobs(using: .memory, numWorkers: 4)
        app.router.post("/job/${number}") { request -> HTTPResponseStatus in
            let parameter = try request.parameters.require("number", as: Int.self)
            _ = request.jobs.enqueue(job: TestJob(value: parameter))
            return .ok
        }

        try await app.XCTTest { client in
            try await client.XCTExecute(uri: "/job/1", method: .POST)
            try await client.XCTExecute(uri: "/job/2", method: .POST)
            try await client.XCTExecute(uri: "/job/3", method: .POST)
            try await client.XCTExecute(uri: "/job/4", method: .POST)
            try await client.XCTExecute(uri: "/job/5", method: .POST)
            try await client.XCTExecute(uri: "/job/6", method: .POST)
            try await client.XCTExecute(uri: "/job/7", method: .POST)
            try await client.XCTExecute(uri: "/job/8", method: .POST)
            try await client.XCTExecute(uri: "/job/9", method: .POST)
            try await client.XCTExecute(uri: "/job/10", method: .POST)
            await self.wait(for: [TestJob.expectation], timeout: 5)
        }
    }

    func testErrorRetryCount() async throws {
        struct FailedError: Error {}

        struct TestJob: HBJob {
            static let name = "testErrorRetryCount"
            static let maxRetryCount = 3
            static let expectation = XCTestExpectation(description: "Jobs Completed")
            func execute(on eventLoop: EventLoop, logger: Logger) -> EventLoopFuture<Void> {
                Self.expectation.fulfill()
                return eventLoop.makeFailedFuture(FailedError())
            }
        }
        TestJob.register()
        TestJob.expectation.expectedFulfillmentCount = 4
        let app = HBApplication(testing: .live)
        app.logger.logLevel = .trace
        app.addJobs(using: .memory, numWorkers: 1)
        app.router.post("/job") { request -> HTTPResponseStatus in
            _ = request.jobs.enqueue(job: TestJob())
            return .ok
        }

        try await app.XCTTest { client in
            try await client.XCTExecute(uri: "/job", method: .POST)
            await self.wait(for: [TestJob.expectation], timeout: 1)
        }
    }

    func testSecondQueue() async throws {
        struct TestJob: HBJob {
            static let name = "testSecondQueue"
            static let expectation = XCTestExpectation(description: "Jobs Completed")
            func execute(on eventLoop: EventLoop, logger: Logger) -> EventLoopFuture<Void> {
                Self.expectation.fulfill()
                return eventLoop.makeSucceededVoidFuture()
            }
        }
        TestJob.register()
        TestJob.expectation.expectedFulfillmentCount = 1
        let app = HBApplication(testing: .live)
        app.logger.logLevel = .trace
        app.addJobs(using: .memory, numWorkers: 1)
        app.jobs.registerQueue(.test, queue: .memory, numWorkers: 1)
        app.router.post("/job") { request -> HTTPResponseStatus in
            _ = request.jobs.enqueue(job: TestJob(), on: .test)
            return .ok
        }

        try await app.XCTTest { client in
            try await client.XCTExecute(uri: "/job", method: .POST)
            await self.wait(for: [TestJob.expectation], timeout: 1)
        }
    }

    func testQueueOutsideApplication() async throws {
        struct TestJob: HBJob {
            static let name = "testSecondQueue"
            static let expectation = XCTestExpectation(description: "Jobs Completed")
            func execute(on eventLoop: EventLoop, logger: Logger) -> EventLoopFuture<Void> {
                Self.expectation.fulfill()
                return eventLoop.makeSucceededVoidFuture()
            }
        }
        TestJob.register()
        TestJob.expectation.expectedFulfillmentCount = 1
        let app = HBApplication(testing: .live)
        app.logger.logLevel = .trace
        let jobQueue = HBMemoryJobQueue(eventLoop: app.eventLoopGroup.any())
        let jobQueueHandler = HBJobQueueHandler(queue: jobQueue, numWorkers: 4, eventLoopGroup: app.eventLoopGroup, logger: app.logger)
        app.addService { jobQueueHandler }
        app.router.post("/job") { request -> HTTPResponseStatus in
            _ = jobQueueHandler.enqueue(TestJob(), on: request.eventLoop)
            return .ok
        }

        try await app.XCTTest { client in
            try await client.XCTExecute(uri: "/job", method: .POST)
            await self.wait(for: [TestJob.expectation], timeout: 1)
        }
    }

    func testShutdown() async throws {
        let app = HBApplication(testing: .live)
        app.logger.logLevel = .trace
        app.addJobs(using: .memory, numWorkers: 1)
        try await app.XCTTest { _ in }
    }

    func testShutdownJob() async throws {
        struct TestJob: HBJob {
            static let name = "testShutdownJob"
            static var started: Bool = false
            static var finished: Bool = false
            func execute(on eventLoop: EventLoop, logger: Logger) -> EventLoopFuture<Void> {
                Self.started = true
                let job = eventLoop.scheduleTask(in: .milliseconds(500)) {
                    Self.finished = true
                }
                return job.futureResult
            }
        }
        TestJob.register()

        let app = HBApplication(testing: .live)
        app.logger.logLevel = .trace
        app.addJobs(using: .memory, numWorkers: 1)
        app.router.post("/job") { request -> HTTPResponseStatus in
            _ = request.jobs.enqueue(job: TestJob())
            return .ok
        }

        try await app.XCTTest { client in
            try await client.XCTExecute(uri: "/job", method: .POST)
            // stall to give job chance to start running
            try await Task.sleep(nanoseconds: 100_000_000)
        }

        XCTAssertTrue(TestJob.started)
        XCTAssertTrue(TestJob.finished)
    }

    func testJobSerialization() throws {
        struct TestJob: HBJob, Equatable {
            static let name = "testJobSerialization"
            let value: Int
            func execute(on eventLoop: EventLoop, logger: Logger) -> EventLoopFuture<Void> {
                return eventLoop.makeSucceededVoidFuture()
            }
        }
        TestJob.register()
        let job = TestJob(value: 2)
        let queuedJob = HBJobContainer(job)
        let data = try JSONEncoder().encode(queuedJob)
        let queuedJob2 = try JSONDecoder().decode(HBJobContainer.self, from: data)
        XCTAssertEqual(queuedJob2.job as? TestJob, job)
    }

    /// test job fails to decode but queue continues to process
    func testFailToDecode() async throws {
        struct TestJob1: HBJob {
            static let name = "testFailToDecode"
            func execute(on eventLoop: EventLoop, logger: Logger) -> EventLoopFuture<Void> {
                return eventLoop.makeSucceededVoidFuture()
            }
        }
        struct TestJob2: HBJob {
            static let name = "testFailToDecode"
            static var value: String?
            let value: String
            func execute(on eventLoop: EventLoop, logger: Logger) -> EventLoopFuture<Void> {
                Self.value = self.value
                return eventLoop.makeSucceededVoidFuture()
            }
        }
        TestJob2.register()

        let app = HBApplication(testing: .live)
        app.logger.logLevel = .trace
        app.addJobs(using: .memory, numWorkers: 1)
        app.router.post("/job") { request -> HTTPResponseStatus in
            _ = request.jobs.enqueue(job: TestJob1())
            _ = request.jobs.enqueue(job: TestJob2(value: "test"))
            return .ok
        }

        try await app.XCTTest { client in
            try await client.XCTExecute(uri: "/job", method: .POST)
            // stall to give job chance to start running
            try await Task.sleep(nanoseconds: 100_000_000)
        }
        XCTAssertEqual(TestJob2.value, "test")
    }

    func testAsyncJob() async throws {
        struct TestAsyncJob: HBAsyncJob {
            static let name = "testAsyncJob"
            static let expectation = XCTestExpectation(description: "Jobs Completed")

            func execute(logger: Logger) async throws {
                try await Task.sleep(nanoseconds: 1_000_000)
                Self.expectation.fulfill()
            }
        }

        TestAsyncJob.register()
        TestAsyncJob.expectation.expectedFulfillmentCount = 3

        let app = HBApplication(testing: .live)
        app.logger.logLevel = .trace
        app.addJobs(using: .memory, numWorkers: 2)
        app.router.post("/job") { request -> HTTPResponseStatus in
            _ = request.jobs.enqueue(job: TestAsyncJob())
            _ = request.jobs.enqueue(job: TestAsyncJob())
            _ = request.jobs.enqueue(job: TestAsyncJob())
            return .ok
        }

        try await app.XCTTest { client in
            try await client.XCTExecute(uri: "/job", method: .POST)
            await self.wait(for: [TestAsyncJob.expectation], timeout: 1)
        }
    }
}

extension HBJobQueueId {
    static var test: HBJobQueueId { "test" }
}
