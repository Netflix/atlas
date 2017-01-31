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

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import akka.actor.Actor
import akka.actor.Props
import com.netflix.iep.NetflixEnvironment
import com.netflix.spectator.api.Spectator
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite
import spray.can.Http
import spray.http._
import spray.testkit.ScalatestRouteTest

import scala.collection.mutable

class SSEActorSuite extends FunSuite with BeforeAndAfter with ScalatestRouteTest {
  import SSEActorSuite._
  import StreamApi._

  val registry = Spectator.globalRegistry()

  before {
    reset()
  }

  def waitForShutdown() = {
    clientDone.await(5, TimeUnit.MINUTES)
  }

  test("Registers and unsubscribes from subscription manager") {
    val testClient = system.actorOf(Props(new TestClient()))
    val mockSM = MockSubscriptionManager()
    val sse = system.actorOf(Props(new SSEActor(testClient, "mySSEId", "myName", mockSM, registry)))
    val ret1 = List(ExpressionWithFrequency("expr"))

    sse ! SSESubscribe("expr", ret1)

    sse ! SSEShutdown("test shutdown")

    waitForShutdown()

    assert(mockSM.invocations === List(
      "register,mySSEId,myName",
      "unsubscribeAll,mySSEId"
    ))

    assert(invocations === List[String](
      "STARTHTTP:ContentType=text/event-stream:info: Connected {}",
      SSEHello("mySSEId", NetflixEnvironment.instanceId, GlobalUUID.get).toSSE,
      SSEStatistics(0).toSSE,
      SSESubscribe("expr", ret1).toSSE,
      SSEShutdown("test shutdown").toSSE,
      "close"
    ))
  }

  test("tick") {
    val testClient = system.actorOf(Props(new TestClient()))
    val mockSM = MockSubscriptionManager()
    val sse = system.actorOf(Props(new SSEActor(testClient, "mySSEId", "myName", mockSM, registry)))

    Thread.sleep(100)
    sse ! SSEActor.Tick

    sse ! SSEShutdown("test shutdown")

    waitForShutdown()

    assert(invocations === List[String](
      "STARTHTTP:ContentType=text/event-stream:info: Connected {}",
      SSEHello("mySSEId", NetflixEnvironment.instanceId, GlobalUUID.get).toSSE,
      SSEStatistics(0).toSSE,
      SSEShutdown("test shutdown").toSSE,
      "close"
    ))
  }
}

object SSEActorSuite {
  private val invocationList = mutable.ListBuffer[String]()

  def reset() = {
    invocationList.clear()
    clientDone = new CountDownLatch(1)
  }

  def invocations: List[String] = invocationList.toList

  def record(s: String): Unit = {
    invocationList += s.stripLineEnd.stripLineEnd
  }

  @volatile var clientDone = new CountDownLatch(1)

  class TestClient extends Actor {
    def convert(v: Any): List[String] = v match {
      case MessageChunk(data, extension) =>
        data.asString.split("\r\n\r\n").toList
      case ChunkedResponseStart(msg) =>
        val parts = msg.entity.asString.split("\r\n\r\n")
        val contentType = msg.entity.toOption match {
          case Some(entity) =>
            Some(entity.contentType)
          case None =>
            None
        }

        var first = true
        val strings = parts.map { x =>
          val ctype = if (!first) "" else {
            val value = contentType.getOrElse(ContentTypes.NoContentType)
            s"ContentType=$value:"
          }
          val ret = if (first) s"STARTHTTP:$ctype$x" else x
          first = false
          ret
        }
        strings.toList
      case x => List(x.toString)
    }

    def receive = {
      // need to reply to all Confirmed messages
      case Confirmed(x, y) =>
        sender() ! y
        convert(x).foreach { x => record(x) }
      case Http.Close =>
        record("close")
        clientDone.countDown()
      case x => record(x.toString)
    }
  }
}
