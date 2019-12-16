/*
 * Copyright 2014-2019 Netflix, Inc.
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
package com.netflix.atlas.poller

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes
import akka.stream.ActorMaterializer
import akka.stream.Materializer
import akka.testkit.ImplicitSender
import akka.testkit.TestActorRef
import akka.testkit.TestKit
import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.json.Json
import com.netflix.atlas.poller.ClientActorSuite.TestClientActor
import com.netflix.atlas.poller.Messages.MetricsPayload
import com.netflix.atlas.webapi.PublishApi
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.ManualClock
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuiteLike

import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class ClientActorSuite
    extends TestKit(ActorSystem())
    with ImplicitSender
    with AnyFunSuiteLike
    with BeforeAndAfterAll {

  import scala.concurrent.duration._

  private val clock = new ManualClock()
  private val registry = new DefaultRegistry(clock)
  private val config = ConfigFactory.load().getConfig("atlas.poller.sink")
  private val materializer = ActorMaterializer()(system)
  private val ref = TestActorRef(new TestClientActor(registry, config, materializer))

  override def afterAll(): Unit = {
    system.terminate()
  }

  private def waitForCompletion(): Unit = {
    expectMsgPF(1.minute) { case Messages.Ack => }
  }

  private def testSend(
    datapoints: List[Datapoint],
    numSent: Int,
    numDropped: Int,
    batches: Int = 1
  ): Unit = {
    val sent = registry.counter("atlas.client.sent")

    val initSent = sent.count()

    ref ! MetricsPayload(metrics = datapoints)
    (0 until batches).foreach { _ =>
      waitForCompletion()
    }

    assert(sent.count() === initSent + numSent)
  }

  test("publish datapoints, exception") {
    ref ! Failure(new RuntimeException("fail"))
    val datapoints = (0 until 10)
      .map(i => Datapoint(Map("name" -> s"foo_$i"), 0L, i.toDouble))
      .toList
    testSend(datapoints, 10, 10)
  }

  test("publish datapoints, ok") {
    ref ! Success(HttpResponse(StatusCodes.OK))
    val datapoints = (0 until 10)
      .map(i => Datapoint(Map("name" -> s"foo_$i"), 0L, i.toDouble))
      .toList
    testSend(datapoints, 10, 0)
  }

  test("publish datapoints, several batches") {
    ref ! Success(HttpResponse(StatusCodes.OK))
    val datapoints = (0 until 50000)
      .map(i => Datapoint(Map("name" -> s"foo_$i"), 0L, i.toDouble))
      .toList
    testSend(datapoints, 50000, 0, 5)
  }

  test("publish datapoints, partial failure") {
    val json =
      """
        |{
        |  "type": "error",
        |  "errorCount": 7,
        |  "message": ["abc"]
        |}
      """.stripMargin
    ref ! Success(HttpResponse(StatusCodes.Accepted, entity = json))
    val datapoints = (0 until 10)
      .map(i => Datapoint(Map("name" -> s"foo_$i"), 0L, i.toDouble))
      .toList
    testSend(datapoints, 10, 7)
  }

  test("publish datapoints, partial failure, cannot parse response") {
    ref ! Success(HttpResponse(StatusCodes.Accepted))
    val datapoints = Datapoint(Map("name" -> s"invalid name!!"), 0L, 1.0) :: (0 until 10)
        .map(i => Datapoint(Map("name" -> s"foo_$i"), 0L, i.toDouble))
        .toList
    testSend(datapoints, 11, 11)
  }

  test("publish datapoints, complete failure") {
    ref ! Success(HttpResponse(StatusCodes.BadRequest))
    val datapoints = (0 until 10)
      .map(i => Datapoint(Map("name" -> s"foo_$i"), 0L, i.toDouble))
      .toList
    testSend(datapoints, 10, 10)
  }

  test("publish datapoints, unexpected status code") {
    ref ! Success(HttpResponse(StatusCodes.InternalServerError))
    val datapoints = (0 until 10)
      .map(i => Datapoint(Map("name" -> s"foo_$i"), 0L, i.toDouble))
      .toList
    testSend(datapoints, 10, 10)
  }
}

object ClientActorSuite {

  object Done

  class TestClientActor(registry: Registry, config: Config, materializer: Materializer)
      extends ClientActor(registry, config, materializer) {

    var response: Try[HttpResponse] = Failure(new RuntimeException("not set"))

    override protected def post(data: Array[Byte]): Future[HttpResponse] = {
      val t = Try {
        PublishApi.decodeBatch(Json.newSmileParser(data))
        response.get
      }
      Promise.fromTry(t).future
    }

    override def receive: Receive = testReceive.orElse(super.receive)

    private def testReceive: Receive = {
      case t: Try[_] => response = t.asInstanceOf[Try[HttpResponse]]
    }
  }
}
