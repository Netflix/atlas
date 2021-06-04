/*
 * Copyright 2014-2021 Netflix, Inc.
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

import akka.http.scaladsl.model.ws.Message
import akka.http.scaladsl.testkit.RouteTestTimeout
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.http.scaladsl.testkit.WSProbe
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.akka.RequestHandler
import com.netflix.atlas.eval.model.LwcDatapoint
import com.netflix.atlas.eval.model.LwcExpression
import com.netflix.atlas.eval.model.LwcHeartbeat
import com.netflix.atlas.eval.model.LwcMessages
import com.netflix.atlas.eval.model.LwcSubscription
import com.netflix.atlas.json.Json
import com.netflix.spectator.api.NoopRegistry
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class SubscribeApiSuite extends AnyFunSuite with BeforeAndAfter with ScalatestRouteTest {

  import scala.concurrent.duration._

  private implicit val routeTestTimeout = RouteTestTimeout(5.second)

  private val config = ConfigFactory.load()
  private val sm = new StreamSubscriptionManager
  private val splitter = new ExpressionSplitter(config)

  private val api = new SubscribeApi(config, new NoopRegistry, sm, splitter, materializer)

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
    WS("/api/v1/subscribe/111", client.flow) ~> routes ~> check {
      assert(isWebSocketUpgrade)

      // Send list of expressions to subscribe to
      val exprs = List(LwcExpression("name,cpu,:eq,:avg", 60000))
      client.sendMessage(Json.encode(exprs))

      // Look for subscription messages, one for sum and one for count
      var subscriptions = List.empty[LwcSubscription]
      while (subscriptions.size < 2) {
        parse(client.expectMessage()) match {
          case _: DiagnosticMessage =>
          case sub: LwcSubscription => subscriptions = sub :: subscriptions
          case h: LwcHeartbeat      => assert(h.step === 60000)
          case v                    => throw new MatchError(v)
        }
      }

      // Verify subscription is in the manager, push a message to the queue check that it
      // is received by the client
      assert(subscriptions.flatMap(_.metrics).size === 2)
      subscriptions.flatMap(_.metrics).foreach { m =>
        val tags = Map("name" -> "cpu")
        val datapoint = LwcDatapoint(60000, m.id, tags, 42.0)
        val handlers = sm.handlersForSubscription(m.id)
        assert(handlers.size === 1)
        handlers.head.offer(datapoint)

        assert(parse(client.expectMessage()) === datapoint)
      }
    }
  }

  private def parseBatch(msg: Message): List[AnyRef] = {
    LwcMessages.parseBatch(msg.asBinaryMessage.getStrictData)
  }

  test("subscribe websocket V2") {
    val client = WSProbe()
    WS("/api/v2/subscribe/222", client.flow) ~> routes ~> check {
      assert(isWebSocketUpgrade)

      // Send list of expressions to subscribe to
      val exprs = List(LwcExpression("name,disk,:eq,:avg", 60000))
      client.sendMessage(LwcMessages.encodeBatch(exprs))

      // Look for subscription messages, one for sum and one for count
      var subscriptions = List.empty[LwcSubscription]
      while (subscriptions.size < 2) {
        parseBatch(client.expectMessage()).foreach {
          case _: DiagnosticMessage =>
          case sub: LwcSubscription => subscriptions = sub :: subscriptions
          case h: LwcHeartbeat      => assert(h.step === 60000)
          case v                    => throw new MatchError(v)
        }
      }

      // Verify subscription is in the manager, push a message to the queue check that it
      // is received by the client
      assert(subscriptions.flatMap(_.metrics).size === 2)
      subscriptions.flatMap(_.metrics).foreach { m =>
        val tags = Map("name" -> "disk")
        val datapoint = LwcDatapoint(60000, m.id, tags, 42.0)
        val handlers = sm.handlersForSubscription(m.id)
        assert(handlers.size === 1)
        handlers.head.offer(datapoint)

        assert(parseBatch(client.expectMessage()) === List(datapoint))
      }
    }
  }
}
