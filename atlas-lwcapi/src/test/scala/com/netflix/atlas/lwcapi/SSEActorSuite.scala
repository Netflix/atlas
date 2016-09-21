/*
 * Copyright 2014-2016 Netflix, Inc.
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

import akka.actor.{Actor, Props}
import com.netflix.atlas.lwcapi.RegisterApi.{DeleteRequest, RegisterRequest}
import org.scalatest.{BeforeAndAfter, FunSuite}
import spray.can.Http
import spray.http.{HttpMessage, _}
import spray.testkit.ScalatestRouteTest

import scala.collection.mutable

class SSEActorSuite extends FunSuite with BeforeAndAfter with ScalatestRouteTest {
  import StreamApi._
  import SSEActorSuite._

  before {
    reset()
  }

  def waitForShutdown() = {
    var count = 0
    while (!clientDone && count < 100) {
      Thread.sleep(100)
      count += 1
    }
  }

  test("Registers and unsubscribes from subscription manager") {
    val mockSM = MockSubscriptionManager()

    val testClient = system.actorOf(Props(new TestClient()))
    val sse = system.actorOf(Props(new SSEActor(testClient, "mySSEId", "myName", mockSM)))
    sse ! SSEShutdown("test shutdown")

    waitForShutdown()

    assert(mockSM.invocations === List("register,mySSEId,myName", "unsubscribeAll,mySSEId"))
    assert(invocations === List[String](
      SSEHello("mySSEId").toSSE,
      SSEShutdown("test shutdown").toSSE,
      "close"
    ))
  }
}

object SSEActorSuite {
  private val invocationList = mutable.ListBuffer[String]()

  def reset() = {
    invocationList.clear()
    clientDone = false
  }

  def invocations: List[String] = invocationList.toList

  def record(s: String): Unit = {
    invocationList += s.stripLineEnd.stripLineEnd
  }

  @volatile var clientDone = false

  class TestClient extends Actor {
    def convert(v: Any): String = v match {
      case MessageChunk(data, extension) =>
        data.asString
    }

    def receive = {
      // need to reply to all Confirmed messages
      case Confirmed(x, y) =>
        sender() ! y
        record(convert(x))
      case Http.Close =>
        record("close")
        clientDone = true
    }
  }
}
