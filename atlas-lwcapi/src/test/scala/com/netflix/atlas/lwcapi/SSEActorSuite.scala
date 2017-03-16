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
package com.netflix.atlas.lwcapi

import java.nio.charset.StandardCharsets

import akka.actor.Props
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.scaladsl.Source
import com.netflix.iep.NetflixEnvironment
import com.netflix.spectator.api.NoopRegistry
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.concurrent.duration._

class SSEActorSuite extends FunSuite with BeforeAndAfter with ScalatestRouteTest {
  import StreamApi._

  val registry = new NoopRegistry

  test("tick and unsubscribe") {
    val mockSM = MockSubscriptionManager()
    val sse = Source.actorPublisher[HttpEntity.Chunk](Props(
      new SSEActor("mySSEId", "myName", mockSM, Nil, registry)))
    val invocations = sse.runFold(List.empty[String]){ (acc, msg) =>
      msg.data.decodeString(StandardCharsets.UTF_8).trim :: acc
    }

    mockSM.waitForUnregister()

    assert(mockSM.invocations === List(
      "register,mySSEId,myName",
      "unsubscribeAll,mySSEId"
    ))

    val vs = Await.result(invocations, 1.minute)
    assert(vs.reverse === List(
      SSEHello("mySSEId", NetflixEnvironment.instanceId, GlobalUUID.get).toSSE,
      SSEStatistics(0).toSSE,
      SSEStatistics(0).toSSE,
      SSEShutdown("test shutdown").toSSE
    ))
  }
}
