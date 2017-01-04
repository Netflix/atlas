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

import akka.actor.ActorSystem
import akka.testkit.ImplicitSender
import akka.testkit.TestActorRef
import akka.testkit.TestKit
import com.netflix.spectator.api.DefaultRegistry
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuiteLike
import spray.can.Http
import spray.can.server.Stats

import scala.concurrent.Promise


class ServerStatsActorSuite extends TestKit(ActorSystem())
    with ImplicitSender
    with FunSuiteLike
    with BeforeAndAfterAll {

  import scala.concurrent.duration._

  private val registry = new DefaultRegistry()
  private val bindPromise = Promise[Http.Bound]()
  private val ref = TestActorRef(new ServerStatsActor(registry, bindPromise))

  override def afterAll(): Unit = {
    system.terminate()
  }

  private def get(k: String): Long = {
    import scala.collection.JavaConversions._
    registry.get(registry.createId(k)).measure().head.value.toLong
  }

  test("update") {
    ref ! Stats(5.seconds,
      totalRequests = 100L,
      openRequests = 4L,
      maxOpenRequests = 7L,
      totalConnections = 50L,
      openConnections = 5L,
      maxOpenConnections = 8L,
      requestTimeouts = 2L)

    // counters
    assert(100L === registry.counter("spray.requests").count())
    assert(50L === registry.counter("spray.connections").count())
    assert(2L === registry.counter("spray.requestTimeouts").count())

    // gauges
    assert(4L === get("spray.openRequests"))
    assert(7L === get("spray.maxOpenRequests"))
    assert(5L === get("spray.openConnections"))
    assert(8L === get("spray.maxOpenConnections"))
  }

  test("update 2") {
    ref ! Stats(5.seconds,
      totalRequests = 200L,
      openRequests = 5L,
      maxOpenRequests = 8L,
      totalConnections = 100L,
      openConnections = 6L,
      maxOpenConnections = 9L,
      requestTimeouts = 4L)

    // counters
    assert(200L === registry.counter("spray.requests").count())
    assert(100L === registry.counter("spray.connections").count())
    assert(4L === registry.counter("spray.requestTimeouts").count())

    // gauges
    assert(5L === get("spray.openRequests"))
    assert(8L === get("spray.maxOpenRequests"))
    assert(6L === get("spray.openConnections"))
    assert(9L === get("spray.maxOpenConnections"))
  }

}
