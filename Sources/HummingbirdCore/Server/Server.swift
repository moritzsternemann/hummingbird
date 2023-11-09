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
import NIOExtras
import NIOHTTP1
import NIOPosix
#if canImport(Network)
import Network
import NIOTransportServices
#endif
import ServiceLifecycle

/// HTTP server class
public actor HBServer<ChannelSetup: HBChannelSetup>: Service {
    public typealias AsyncChildChannel = NIOAsyncChannel<ChannelSetup.In, ChannelSetup.Out>
    public typealias AsyncServerChannel = NIOAsyncChannel<AsyncChildChannel, Never>
    enum State: CustomStringConvertible {
        case initial(
            childChannelSetup: ChannelSetup,
            configuration: HBServerConfiguration,
            onServerRunning: (@Sendable (Channel) async -> Void)?
        )
        case starting
        case running(
            asyncChannel: AsyncServerChannel,
            quiescingHelper: ServerQuiescingHelper
        )
        case shuttingDown(shutdownPromise: EventLoopPromise<Void>)
        case shutdown

        var description: String {
            switch self {
            case .initial:
                return "initial"
            case .starting:
                return "starting"
            case .running:
                return "running"
            case .shuttingDown:
                return "shuttingDown"
            case .shutdown:
                return "shutdown"
            }
        }
    }

    var state: State {
        didSet { self.logger.trace("Server State: \(self.state)") }
    }

    /// Logger used by Server
    public nonisolated let logger: Logger
    let eventLoopGroup: EventLoopGroup

    /// HTTP server errors
    public enum Error: Swift.Error {
        case serverShuttingDown
        case serverShutdown
    }

    /// Initialize Server
    /// - Parameters:
    ///   - group: EventLoopGroup server uses
    ///   - configuration: Configuration for server
    public init(
        childChannelSetup: ChannelSetup,
        configuration: HBServerConfiguration,
        onServerRunning: (@Sendable (Channel) async -> Void)? = { _ in },
        eventLoopGroup: EventLoopGroup,
        logger: Logger
    ) {
        self.state = .initial(
            childChannelSetup: childChannelSetup,
            configuration: configuration,
            onServerRunning: onServerRunning
        )
        self.eventLoopGroup = eventLoopGroup
        self.logger = logger
    }

    public func run() async throws {
        switch self.state {
        case .initial(let childChannelSetup, let configuration, let onServerRunning):
            self.state = .starting

            do {
                // We have to check our state again since we just awaited on the line above
                switch self.state {
                case .initial, .running:
                    fatalError("We should only be running once")

                case .starting:
                    // self.state = .running(
                    //     asyncChannel: asyncChannel, 
                    //     quiescingHelper: quiescingHelper
                    // )

                    try await childChannelSetup.start(
                        eventLoopGroup: self.eventLoopGroup, 
                        configuration: configuration, 
                        logger: logger
                    )
                case .shuttingDown, .shutdown:
                    return
                }
            } catch {
                self.state = .shutdown
                throw error
            }
        case .starting, .running:
            fatalError("Run should only be called once")

        case .shuttingDown:
            throw Error.serverShuttingDown

        case .shutdown:
            throw Error.serverShutdown
        }
    }

    /// Stop HTTP server
    public func shutdownGracefully() async throws {
        switch self.state {
        case .initial, .starting:
            self.state = .shutdown

        case .running(let channel, let quiescingHelper):
            // quiesce open channels
            let shutdownPromise = channel.channel.eventLoop.makePromise(of: Void.self)
            self.state = .shuttingDown(shutdownPromise: shutdownPromise)
            quiescingHelper.initiateShutdown(promise: shutdownPromise)
            try await shutdownPromise.futureResult.get()

            // We need to check the state here again since we just awaited above
            switch self.state {
            case .initial, .starting, .running, .shutdown:
                fatalError("Unexpected state")

            case .shuttingDown:
                self.state = .shutdown
            }

        case .shuttingDown(let shutdownPromise):
            // We are just going to queue up behind the current graceful shutdown
            try await shutdownPromise.futureResult.get()

        case .shutdown:
            return
        }
    }
}

/// Protocol for bootstrap.
protocol ServerBootstrapProtocol {
    func bind<Output: Sendable>(
        host: String,
        port: Int,
        serverBackPressureStrategy: NIOAsyncSequenceProducerBackPressureStrategies.HighLowWatermark?,
        childChannelInitializer: @escaping @Sendable (Channel) -> EventLoopFuture<Output>
    ) async throws -> NIOAsyncChannel<Output, Never>

    func bind<Output: Sendable>(
        unixDomainSocketPath: String,
        cleanupExistingSocketFile: Bool,
        serverBackPressureStrategy: NIOAsyncSequenceProducerBackPressureStrategies.HighLowWatermark?,
        childChannelInitializer: @escaping @Sendable (Channel) -> EventLoopFuture<Output>
    ) async throws -> NIOAsyncChannel<Output, Never>
}

// Extend both `ServerBootstrap` and `NIOTSListenerBootstrap` to conform to `ServerBootstrapProtocol`
extension ServerBootstrap: ServerBootstrapProtocol {}

#if canImport(Network)
@available(macOS 10.14, iOS 12, tvOS 12, *)
extension NIOTSListenerBootstrap: ServerBootstrapProtocol {
    // need to be able to extend `NIOTSListenerBootstrap` to conform to `ServerBootstrapProtocol`
    // before we can use TransportServices
    func bind<Output: Sendable>(
        unixDomainSocketPath: String,
        cleanupExistingSocketFile: Bool,
        serverBackPressureStrategy: NIOAsyncSequenceProducerBackPressureStrategies.HighLowWatermark?,
        childChannelInitializer: @escaping @Sendable (Channel) -> EventLoopFuture<Output>
    ) async throws -> NIOAsyncChannel<Output, Never> {
        preconditionFailure("Binding to a unixDomainSocketPath is currently not available")
    }
}
#endif

extension HBServer: CustomStringConvertible {
    public nonisolated var description: String {
        "Hummingbird"
    }
}
