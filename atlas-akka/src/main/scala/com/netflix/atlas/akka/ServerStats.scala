/*
 * Copyright 2015 Netflix, Inc.
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

import java.util.concurrent.atomic.AtomicLong

import com.netflix.spectator.api.ExtendedRegistry
import spray.can.server.Stats

/**
 * Reports metrics based on the spray server stats.
 */
class ServerStats(registry: ExtendedRegistry) {

  import scala.concurrent.duration._

  private val requestCount = registry.counter("spray.requests")
  private val requestTimeouts = registry.counter("spray.requestTimeouts")
  private val openRequests = registry.gauge("spray.openRequests", new AtomicLong(0L))
  private val maxOpenRequests = registry.gauge("spray.maxOpenRequests", new AtomicLong(0L))
  private val connectionCount = registry.counter("spray.connections")
  private val openConnections = registry.gauge("spray.openConnections", new AtomicLong(0L))
  private val maxOpenConnections = registry.gauge("spray.maxOpenConnections", new AtomicLong(0L))

  // Previous value used to compute the delta
  private var serverStats: Stats = Stats(0.seconds, 0L, 0L, 0L, 0L, 0L, 0L, 0L)

  /** Update the counts with the most recent stats. */
  def update(s: Stats): Unit = {
    requestCount.increment(s.totalRequests - serverStats.totalRequests)
    requestTimeouts.increment(s.requestTimeouts - serverStats.requestTimeouts)
    openRequests.set(s.openRequests)
    maxOpenRequests.set(s.maxOpenRequests)
    connectionCount.increment(s.totalConnections - serverStats.totalConnections)
    openConnections.set(s.openConnections)
    maxOpenConnections.set(s.maxOpenConnections)
    serverStats = s
  }

}
