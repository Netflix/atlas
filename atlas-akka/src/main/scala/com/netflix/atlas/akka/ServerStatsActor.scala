/*
 * Copyright 2014-2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.atlas.akka

import java.net.BindException
import java.util.concurrent.atomic.AtomicLong

import akka.actor.Actor
import akka.io.Tcp
import com.netflix.spectator.api.Registry
import com.typesafe.scalalogging.StrictLogging
import spray.can.Http
import spray.can.server.Stats

import scala.concurrent.Promise
import scala.util.Failure
import scala.util.Success


/**
 * Update metrics using the spray server stats.
 *
 * @param registry
 *     Spectator registry to use for metrics.
 * @param bound
 *     Promise that will get updated when the server is bound or fails to bind. Used to detect
 *     and fail right away if the server fails to bind to the port.
 */
class ServerStatsActor(registry: Registry, bound: Promise[Http.Bound])
    extends Actor with StrictLogging {

  import scala.concurrent.duration._

  private val requestCount = registry.counter("spray.requests")
  private val requestTimeouts = registry.counter("spray.requestTimeouts")
  private val openRequests = registry.gauge("spray.openRequests", new AtomicLong(0L))
  private val maxOpenRequests = registry.gauge("spray.maxOpenRequests", new AtomicLong(0L))

  private val connectionCount = registry.counter("spray.connections")
  private val openConnections = registry.gauge("spray.openConnections", new AtomicLong(0L))
  private val maxOpenConnections = registry.gauge("spray.maxOpenConnections", new AtomicLong(0L))

  // Previous value used to compute the delta
  private var serverStats: Stats = Stats(
    uptime             = 0.seconds,
    totalRequests      = 0L,
    openRequests       = 0L,
    maxOpenRequests    = 0L,
    totalConnections   = 0L,
    openConnections    = 0L,
    maxOpenConnections = 0L,
    requestTimeouts    = 0L)

  def receive: Receive = {
    case Tcp.CommandFailed(cmd) =>
      bound.complete(Failure(new BindException(s"could not bind: $cmd")))
    case b: Http.Bound =>
      bound.complete(Success(b))
      val sys = context.system
      sys.scheduler.schedule(0.seconds, 10.seconds, sender(), Http.GetStats)(sys.dispatcher, self)
    case stats: Stats =>
      requestCount.increment(stats.totalRequests - serverStats.totalRequests)
      requestTimeouts.increment(stats.requestTimeouts - serverStats.requestTimeouts)
      openRequests.set(stats.openRequests)
      maxOpenRequests.set(stats.maxOpenRequests)
      connectionCount.increment(stats.totalConnections - serverStats.totalConnections)
      openConnections.set(stats.openConnections)
      maxOpenConnections.set(stats.maxOpenConnections)
      serverStats = stats
  }
}
