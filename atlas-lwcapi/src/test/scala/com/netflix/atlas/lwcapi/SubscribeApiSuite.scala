/*
 * Copyright 2014-2024 Netflix, Inc.
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

import org.apache.pekko.http.scaladsl.model.ws.Message
import org.apache.pekko.http.scaladsl.testkit.RouteTestTimeout
import org.apache.pekko.http.scaladsl.testkit.WSProbe
import com.netflix.atlas.pekko.DiagnosticMessage
import com.netflix.atlas.pekko.RequestHandler
import com.netflix.atlas.pekko.testkit.MUnitRouteSuite
import com.netflix.atlas.eval.model.LwcDatapoint
import com.netflix.atlas.eval.model.LwcExpression
import com.netflix.atlas.eval.model.LwcHeartbeat
import com.netflix.atlas.eval.model.LwcMessages
import com.netflix.atlas.eval.model.LwcSubscription
import com.netflix.spectator.api.NoopRegistry
import com.typesafe.config.ConfigFactory

class SubscribeApiSuite extends MUnitRouteSuite {

  import scala.concurrent.duration._

  private implicit val routeTestTimeout: RouteTestTimeout = RouteTestTimeout(5.second)

  private val config = ConfigFactory.load()
  private val sm = new StreamSubscriptionManager(new NoopRegistry)
  private val splitter = new ExpressionSplitter(config)

  private val api = new SubscribeApi(config, new NoopRegistry, sm, splitter, materializer)

  private val routes = RequestHandler.standardOptions(api.routes)

  override def beforeEach(context: BeforeEach): Unit = {
    sm.clear()
  }

  //
  // Subscribe websocket
  //

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
          case h: LwcHeartbeat      => assertEquals(h.step, 60000L)
          case v                    => throw new MatchError(v)
        }
      }

      // Verify subscription is in the manager, push a message to the queue check that it
      // is received by the client
      assertEquals(subscriptions.flatMap(_.metrics).size, 2)
      subscriptions.flatMap(_.metrics).foreach { m =>
        val tags = Map("name" -> "disk")
        val datapoint = LwcDatapoint(60000, m.id, tags, 42.0)
        val handlers = sm.handlersForSubscription(m.id)
        assertEquals(handlers.size, 1)
        handlers.head.offer(Seq(datapoint))

        assertEquals(parseBatch(client.expectMessage()), List(datapoint))
      }
    }
  }
}
