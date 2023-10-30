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
import HummingbirdCore
import NIOCore
import NIOEmbedded
import NIOHTTP1
import XCTest

/// Test application by running on an EmbeddedChannel
public final class HBApplicationWrapper {
    /// Response structure returned by XCT testing framework
    public struct Response {
        /// response status
        public let status: HTTPResponseStatus
        /// response headers
        public let headers: HTTPHeaders
        /// response body
        public let body: ByteBuffer?
    }

    let application: HBApplication
    var responder: HBResponder!

    init(configuration: HBApplication.Configuration) {
        self.application = HBApplication(configuration: configuration)
    }

    /// Start tests
    func start() throws {
        self.responder = application.router.buildRouter()
    }

    /// Stop tests
    func stop() {
        self.responder = nil
    }

    /// Send request and call test callback on the response returned
    func execute(
        uri: String,
        method: HTTPMethod,
        headers: HTTPHeaders = [:],
        body: ByteBuffer? = nil
    ) -> EventLoopFuture<HBApplicationWrapper.Response> {
    
        func _feedStreamer(_ streamer: HBByteBufferStreamer, buffer: ByteBuffer) {
            guard buffer.readableBytes > 0 else {
                streamer.feed(.end)
                return
            }
            var buffer = buffer
            let size = min(32768, buffer.readableBytes)
            let slice = buffer.readSlice(length: size)!
            streamer.feed(buffer: slice).whenComplete { _ in
                _feedStreamer(streamer, buffer: buffer)
            }
        }
        let eventLoop = self.application.eventLoopGroup.next()
        return eventLoop.flatSubmit {
            // write request
            let requestHead = HTTPRequestHead(version: .init(major: 1, minor: 1), method: method, uri: uri, headers: headers)
            let requestBody: HBRequestBody
            if let body = body, body.readableBytes > 32768 {
                let streamer = HBByteBufferStreamer(eventLoop: eventLoop, maxSize: self.application.configuration.maxStreamedUploadSize)
                requestBody = .stream(streamer)
                _feedStreamer(streamer, buffer: body)
            } else {
                requestBody = .byteBuffer(body)
            }
            let promise = eventLoop.makePromise(of: HBHTTPResponse.self)
            let request = HBRequest(
                head: requestHead,
                body: requestBody,
                application: self.application,
                context: BenchmarkRequestContext(eventLoop: eventLoop)
            )
            let httpVersion = request.version
            // respond to request
            self.responder.respond(to: request).whenComplete { result in
                switch result {
                case .success(let response):
                    let responseHead = HTTPResponseHead(version: httpVersion, status: response.status, headers: response.headers)
                    promise.succeed(HBHTTPResponse(head: responseHead, body: response.body))

                case .failure(let error):
                    promise.fail(error)
                }
            }
            return promise.futureResult.flatMap { response in
                let body: EventLoopFuture<ByteBuffer?>
                switch response.body {
                case .byteBuffer(let buffer):
                    body = eventLoop.makeSucceededFuture(buffer)
                case .stream(let streamer):
                    var collatingBuffer = ByteBuffer()
                    body = streamer.write(on: eventLoop) { buffer in
                        var buffer = buffer
                        collatingBuffer.writeBuffer(&buffer)
                    }.map { collatingBuffer }
                case .empty:
                    body = eventLoop.makeSucceededFuture(nil)
                }
                return body.map {
                    Response(status: response.head.status, headers: response.head.headers, body: $0)
                }
            }
        }
    }
}

extension HBResponseBodyStreamer {
    /// Call closure for every ByteBuffer streamed
    /// - Returns: When everything has been streamed
    func write(on eventLoop: EventLoop, _ writeCallback: @escaping (ByteBuffer) -> Void) -> EventLoopFuture<Void> {
        let promise = eventLoop.makePromise(of: Void.self)
        func _stream() {
            self.read(on: eventLoop).whenComplete { result in
                switch result {
                case .success(.byteBuffer(let buffer)):
                    writeCallback(buffer)
                    _stream()
                case .success(.end):
                    promise.succeed(())
                case .failure(let error):
                    promise.fail(error)
                }
            }
        }
        _stream()
        return promise.futureResult
    }
}

/// Context object for Channel to be provided to HBRequest
struct BenchmarkRequestContext: HBRequestContext {
    let eventLoop: EventLoop
    var allocator: ByteBufferAllocator { ByteBufferAllocator() }
    var remoteAddress: SocketAddress? { return nil }
}
