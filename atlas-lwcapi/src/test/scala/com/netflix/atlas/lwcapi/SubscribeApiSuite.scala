/*
 * Copyright 2014-2018 Netflix, Inc.
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

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.ws.Message
import akka.http.scaladsl.testkit.RouteTestTimeout
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.http.scaladsl.testkit.WSProbe
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.akka.RequestHandler
import com.netflix.atlas.eval.model.LwcDatapoint
import com.netflix.atlas.eval.model.LwcExpression
import com.netflix.atlas.eval.model.LwcMessages
import com.netflix.atlas.eval.model.LwcSubscription
import com.netflix.atlas.json.Json
import com.netflix.spectator.api.NoopRegistry
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite

class SubscribeApiSuite extends FunSuite with BeforeAndAfter with ScalatestRouteTest {

  import scala.concurrent.duration._

  private implicit val routeTestTimeout = RouteTestTimeout(5.second)

  // Dummy queue used for handler
  private val queue = new QueueHandler(
    "test",
    Source
      .queue[SSERenderable](1, OverflowStrategy.dropHead)
      .toMat(Sink.ignore)(Keep.left)
      .run()
  )

  private val sm = new StreamSubscriptionManager
  private val splitter = new ExpressionSplitter(ConfigFactory.load())

  private val api = new SubscribeApi(new NoopRegistry, sm, splitter, system)

  private val routes = RequestHandler.standardOptions(api.routes)

  before {
    sm.clear()
  }

  //
  // Subscribe websocket
  //

  private def parse(msg: Message): AnyRef = {
    LwcMessages.parse(msg.asTextMessage.getStrictText)
  }

  test("subscribe websocket") {
    val client = WSProbe()
    WS("/api/v1/subscribe", client.flow) ~> routes ~> check {
      assert(isWebSocketUpgrade)

      // Send list of expressions to subscribe to
      val exprs = List(LwcExpression("name,cpu,:eq,:avg", 60000))
      client.sendMessage(Json.encode(exprs))

      // Look for subscription messages, one for sum and one for count
      var subscriptions = List.empty[LwcSubscription]
      while (subscriptions.size < 2) {
        parse(client.expectMessage()) match {
          case msg: DiagnosticMessage =>
          case sub: LwcSubscription   => subscriptions = sub :: subscriptions
        }
      }

      // Verify subscription is in the manager, push a message to the queue check that it
      // is received by the client
      assert(subscriptions.flatMap(_.metrics).size === 2)
      subscriptions.flatMap(_.metrics).foreach { m =>
        val tags = Map("name" -> "cpu")
        val sseMetric = StreamApi.SSEMetric(60000, EvaluateApi.Item(m.id, tags, 42.0))
        val handlers = sm.handlersForSubscription(m.id)
        assert(handlers.size === 1)
        handlers.head.offer(sseMetric)

        val expected = LwcDatapoint(60000, m.id, tags, 42.0)
        assert(parse(client.expectMessage()) === expected)
      }
    }
  }

  //
  // Subscribe
  //

  test("subscribe: no content") {
    Post("/lwc/api/v1/subscribe") ~> routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("subscribe: empty object") {
    Post("/lwc/api/v1/subscribe", "{}") ~> routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("subscribe: empty array") {
    val x = Post("/lwc/api/v1/subscribe", "[]")
    x ~> routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("subscribe: correctly formatted expression") {
    sm.register("abc123", queue)

    val json = """
      |{
      |  "streamId": "abc123",
      |  "expressions": [
      |    { "expression": "nf.name,foo,:eq,:sum", "frequency": 99 }
      |  ]
      |}""".stripMargin
    Post("/lwc/api/v1/subscribe", json) ~> routes ~> check {
      assert(response.status === StatusCodes.OK)
      val subs = sm.subscriptionsForStream("abc123")
      assert(subs.length === 1)
      assert(subs.head.metadata.expression === "nf.name,foo,:eq,:sum")
      assert(subs.head.metadata.frequency === 99L)
    }
  }

  test("subscribe: sync expressions") {
    sm.register("abc123", queue)

    val exprs = List(
      "name,foo,:eq,:sum",
      "name,foo,:eq,:max",
      "name,bar,:eq,:sum"
    )
    exprs.foreach { expr =>
      val json = s"""
        |{
        |  "streamId": "abc123",
        |  "expressions": [
        |    { "expression": "$expr", "frequency": 99 }
        |  ]
        |}""".stripMargin
      Post("/lwc/api/v1/subscribe", json) ~> routes ~> check {
        assert(response.status === StatusCodes.OK)
        val subs = sm.subscriptionsForStream("abc123")
        assert(subs.length === 1)
        assert(subs.head.metadata.expression === expr)
        assert(subs.head.metadata.frequency === 99L)
      }
    }
  }

  test("subscribe: sync multiple expressions") {
    sm.register("abc123", queue)

    val exprs = List(
      "name,foo,:eq,:sum",
      "name,foo,:eq,:max",
      "name,bar,:eq,:sum"
    )
    exprs.foreach { expr =>
      val json = s"""
        |{
        |  "streamId": "abc123",
        |  "expressions": [
        |    { "expression": "name,fixed,:eq,:sum", "frequency": 99 },
        |    { "expression": "$expr", "frequency": 99 }
        |  ]
        |}""".stripMargin
      Post("/lwc/api/v1/subscribe", json) ~> routes ~> check {
        assert(response.status === StatusCodes.OK)
        val subs = sm.subscriptionsForStream("abc123")
        assert(subs.length === 2)
        val actual = subs.map(_.metadata.expression).toSet
        val expected = Set("name,fixed,:eq,:sum", expr)
        assert(actual === expected)
      }
    }
  }

  test("subscribe: bad json") {
    Post("/lwc/api/v1/subscribe", "fubar") ~> routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("subscribe: invalid object") {
    Post("/lwc/api/v1/subscribe", "{\"foo\":\"bar\"}") ~> routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("subscribe: expression value is null") {
    val json = s"""{
        "expressions": [
          { "expression": null }
        ]
      }"""
    Post("/lwc/api/v1/subscribe", json) ~> routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }
}
