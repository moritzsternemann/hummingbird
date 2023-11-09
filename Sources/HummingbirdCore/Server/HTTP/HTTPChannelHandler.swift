//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2023 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Logging
import NIOCore
import NIOHTTP1
import NIOExtras
import NIOPosix
#if canImport(Network)
import Network
import NIOTransportServices
#endif

/// Protocol for HTTP channels
public protocol HTTPChannelHandler {
    var responder: @Sendable (HBHTTPRequest, Channel) async throws -> HBHTTPResponse { get set }
}

public protocol HTTP1ChannelHandler: HTTPChannelHandler, HBChannelSetup where In == HTTPServerRequestPart, Out == SendableHTTPServerResponsePart {}

/// Internal error thrown when an unexpected HTTP part is received eg we didn't receive
/// a head part when we expected one
enum HTTPChannelError: Error {
    case unexpectedHTTPPart(HTTPServerRequestPart)
    case closeConnection
}

extension HTTPChannelHandler {
    public func handleHTTP(asyncChannel: NIOAsyncChannel<HTTPServerRequestPart, SendableHTTPServerResponsePart>, logger: Logger) async {
        do {
            try await withThrowingTaskGroup(of: Void.self) { group in
                let responseWriter = HBHTTPServerBodyWriter(outbound: asyncChannel.outbound)
                var iterator = asyncChannel.inbound.makeAsyncIterator()
                while let part = try await iterator.next() {
                    guard case .head(let head) = part else {
                        throw HTTPChannelError.unexpectedHTTPPart(part)
                    }
                    let bodyStream = HBStreamedRequestBody()
                    let body = HBRequestBody.stream(bodyStream)
                    let request = HBHTTPRequest(head: head, body: body)
                    // add task processing request and writing response
                    group.addTask {
                        let response: HBHTTPResponse
                        do {
                            response = try await self.responder(request, asyncChannel.channel)
                        } catch {
                            response = self.getErrorResponse(from: error, allocator: asyncChannel.channel.allocator)
                        }
                        let head = HTTPResponseHead(version: request.head.version, status: response.status, headers: response.headers)
                        do {
                            try await asyncChannel.outbound.write(.head(head))
                            try await response.body.write(responseWriter)
                            try await asyncChannel.outbound.write(.end(nil))
                            // flush request body
                            for try await _ in request.body {}
                        } catch {
                            // flush request body
                            for try await _ in request.body {}
                            throw error
                        }
                        if request.head.headers["connection"].first == "close" {
                            throw HTTPChannelError.closeConnection
                        }
                    }
                    // send body parts to request
                    do {
                        // pass body part to request
                        while case .body(let buffer) = try await iterator.next() {
                            await bodyStream.send(buffer)
                        }
                        bodyStream.finish()
                    } catch {
                        // pass failed to read full http body to request
                        bodyStream.fail(error)
                    }
                    try await group.next()
                }
            }
        } catch HTTPChannelError.closeConnection {
            // channel is being closed because we received a connection: close header
        } catch {
            // we got here because we failed to either read or write to the channel
            logger.trace("Failed to read/write to Channel. Error: \(error)")
        }
        asyncChannel.outbound.finish()
    }

    func getErrorResponse(from error: Error, allocator: ByteBufferAllocator) -> HBHTTPResponse {
        switch error {
        case let httpError as HBHTTPResponseError:
            // this is a processed error so don't log as Error
            return httpError.response(allocator: allocator)
        default:
            // this error has not been recognised
            return HBHTTPResponse(
                status: .internalServerError,
                body: .init()
            )
        }
    }
}

extension HTTP1ChannelHandler {
    public typealias AsyncChildChannel = NIOAsyncChannel<In, Out>
    public typealias AsyncServerChannel = NIOAsyncChannel<AsyncChildChannel, Never>
    
    public func start(eventLoopGroup: EventLoopGroup, configuration: HBServerConfiguration, logger: Logger) async throws {
        let (server, quiescing) = try await createServer(
            eventLoopGroup: eventLoopGroup,
            configuration: configuration, 
            logger: logger
        )

        try await withThrowingDiscardingTaskGroup { group in
            for try await client in server.inbound {
                group.addTask {
                    await self.handle(asyncChannel: client, logger: logger)
                }
            }
        }
    }

    private func createServer(eventLoopGroup: EventLoopGroup, configuration: HBServerConfiguration, logger: Logger) async throws -> (AsyncServerChannel, ServerQuiescingHelper) {
        let quiescingHelper = ServerQuiescingHelper(group: eventLoopGroup)

        @Sendable func setupChildChannel(_ channel: Channel) -> EventLoopFuture<AsyncChildChannel> {
            self.initialize(
                channel: channel,
                configuration: configuration,
                logger: logger
            ).flatMapThrowing { _ in
                try NIOAsyncChannel(
                    synchronouslyWrapping: channel,
                    configuration: .init(
                        inboundType: In.self,
                        outboundType: Out.self
                    )
                )
            }
        }
        
        let bootstrap: ServerBootstrapProtocol
        #if canImport(Network)
        if let tsBootstrap = self.createTSBootstrap(
            eventLoopGroup: eventLoopGroup,
            configuration: configuration,
            quiescingHelper: quiescingHelper
        ) {
            bootstrap = tsBootstrap
        } else {
            #if os(iOS) || os(tvOS)
            logger.warning("Running BSD sockets on iOS or tvOS is not recommended. Please use NIOTSEventLoopGroup, to run with the Network framework")
            #endif
            if configuration.tlsOptions.options != nil {
                logger.warning("tlsOptions set in Configuration will not be applied to a BSD sockets server. Please use NIOTSEventLoopGroup, to run with the Network framework")
            }
            bootstrap = self.createSocketsBootstrap(
                eventLoopGroup: eventLoopGroup,
                configuration: configuration,
                quiescingHelper: quiescingHelper
            )
        }
        #else
        bootstrap = self.createSocketsBootstrap(
            configuration: configuration,
            quiescingHelper: quiescingHelper
        )
        #endif

        do {
            let asyncChannel: AsyncServerChannel
            switch configuration.address {
            case .hostname(let host, let port):
                asyncChannel = try await bootstrap.bind(
                    host: host,
                    port: port,
                    serverBackPressureStrategy: nil
                ) { channel in
                    setupChildChannel(channel)
                }
                logger.info("Server started and listening on \(host):\(port)")
            case .unixDomainSocket(let path):
                asyncChannel = try await bootstrap.bind(
                    unixDomainSocketPath: path,
                    cleanupExistingSocketFile: false,
                    serverBackPressureStrategy: nil
                ) { channel in
                    
                    setupChildChannel(channel)
                }
                logger.info("Server started and listening on socket path \(path)")
            }

            return (asyncChannel, quiescingHelper)
        } catch {
            quiescingHelper.initiateShutdown(promise: nil)
            throw error
        }
    }

    /// create a BSD sockets based bootstrap
    private func createSocketsBootstrap(
        eventLoopGroup: EventLoopGroup,
        configuration: HBServerConfiguration,
        quiescingHelper: ServerQuiescingHelper
    ) -> ServerBootstrap {
        return ServerBootstrap(group: eventLoopGroup)
            // Specify backlog and enable SO_REUSEADDR for the server itself
            .serverChannelOption(ChannelOptions.backlog, value: numericCast(configuration.backlog))
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: configuration.reuseAddress ? 1 : 0)
            .serverChannelOption(ChannelOptions.tcpOption(.tcp_nodelay), value: configuration.tcpNoDelay ? 1 : 0)
            .serverChannelInitializer { channel in
                channel.pipeline.addHandler(quiescingHelper.makeServerChannelHandler(channel: channel))
            }
            .childChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: configuration.reuseAddress ? 1 : 0)
            .childChannelOption(ChannelOptions.tcpOption(.tcp_nodelay), value: configuration.tcpNoDelay ? 1 : 0)
            .childChannelOption(ChannelOptions.maxMessagesPerRead, value: 1)
            .childChannelOption(ChannelOptions.allowRemoteHalfClosure, value: true)
    }

    #if canImport(Network)
    /// create a NIOTransportServices bootstrap using Network.framework
    @available(macOS 10.14, iOS 12, tvOS 12, *)
    private func createTSBootstrap(
        eventLoopGroup: EventLoopGroup,
        configuration: HBServerConfiguration,
        quiescingHelper: ServerQuiescingHelper
    ) -> NIOTSListenerBootstrap? {
        guard let bootstrap = NIOTSListenerBootstrap(validatingGroup: eventLoopGroup)?
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: configuration.reuseAddress ? 1 : 0)
            .serverChannelInitializer({ channel in
                channel.pipeline.addHandler(quiescingHelper.makeServerChannelHandler(channel: channel))
            })
            // Set the handlers that are applied to the accepted Channels
            .childChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: configuration.reuseAddress ? 1 : 0)
            .childChannelOption(ChannelOptions.allowRemoteHalfClosure, value: true)
        else {
            return nil
        }

        if let tlsOptions = configuration.tlsOptions.options {
            return bootstrap.tlsOptions(tlsOptions)
        }
        return bootstrap
    }
    #endif
}

/// Writes ByteBuffers to AsyncChannel outbound writer
struct HBHTTPServerBodyWriter: Sendable, HBResponseBodyWriter {
    typealias Out = SendableHTTPServerResponsePart
    /// The components of a HTTP response from the view of a HTTP server.
    public typealias OutboundWriter = NIOAsyncChannelOutboundWriter<Out>

    let outbound: OutboundWriter

    func write(_ buffer: ByteBuffer) async throws {
        try await self.outbound.write(.body(buffer))
    }
}
