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
import NIOPosix
import NIOHTTP1
import NIOHTTP2
import Hummingbird
import HummingbirdCore
import NIOSSL

public struct HTTP2Channe: HTTPChannelHandler {
    public typealias In = HTTPServerRequestPart
    public typealias Out = SendableHTTPServerResponsePart
    public typealias Client = NIOAsyncChannel<In, Out>

    public var tlsConfiguration: TLSConfiguration
    public let http1: HTTP1Channel
    public var responder: @Sendable (HBHTTPRequest, Channel) async throws -> HBHTTPResponse
    // let additionalChannelHandlers: @Sendable () -> [any RemovableChannelHandler]

    public init(
        tlsConfiguration: TLSConfiguration,
        http1: HTTP1Channel = HTTP1Channel(),
        responder: @escaping @Sendable (HBHTTPRequest, Channel) async throws -> HBHTTPResponse = { _, _ in throw HBHTTPError(.notImplemented) }
    ) {
        self.tlsConfiguration = tlsConfiguration
        self.http1 = http1
        // self.additionalChannelHandlers = additionalChannelHandlers
        self.responder = responder
    }

    public func start(group: EventLoopGroup, configuration: HBServerConfiguration, logger: Logger) async throws {
        var tlsConfiguration = self.tlsConfiguration
        tlsConfiguration.applicationProtocols = NIOHTTP2SupportedALPNProtocols
        let sslContext = try NIOSSLContext(configuration: tlsConfiguration)
        
        let address: SocketAddress

        switch configuration.address {
        case .hostname(let host, port: let port):
            address = try .init(ipAddress: host, port: port)
        case .unixDomainSocket(let path):
            address = try .init(unixDomainSocketPath: path)
        }

        let server = try await ServerBootstrap(group: group)
            .bind(to: address) { channel in
                channel.pipeline.addHandler(NIOSSLServerHandler(context: sslContext)).flatMap {
                    channel.configureAsyncHTTPServerPipeline { http1Channel in
                        http1.initialize(
                            channel: http1Channel,
                            configuration: configuration,
                            logger: logger
                        ).flatMapThrowing {
                            try NIOAsyncChannel<In, Out>(synchronouslyWrapping: http1Channel)
                        }
                    } http2ConnectionInitializer: { http2Channel in
                        http2Channel.eventLoop.makeCompletedFuture {
                            try NIOAsyncChannel<HTTP2Frame, HTTP2Frame>(
                                synchronouslyWrapping: http2Channel
                            )
                        }
                    } http2StreamInitializer: { http2ChildChannel in
                        http2ChildChannel
                            .pipeline
                            .addHandlers(HTTP2FramePayloadToHTTP1ServerCodec(), HBHTTPSendableResponseChannelHandler())
                            .flatMapThrowing {
                                try NIOAsyncChannel<In, Out>(synchronouslyWrapping: http2ChildChannel)
                            }
                    }
                }.flatMap { $0 }
            }

        try await withThrowingDiscardingTaskGroup { group in
            for try await client in server.inbound {
                switch client {
                case .http1_1(let client):
                    group.addTask {
                        await handleHTTP(asyncChannel: client, logger: logger)
                    }
                case .http2((_, let multiplexer)):
                    group.addTask {
                        try await withThrowingDiscardingTaskGroup { group in
                            for try await client in multiplexer.inbound {
                                group.addTask {
                                    await handleHTTP(asyncChannel: client, logger: logger)
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
