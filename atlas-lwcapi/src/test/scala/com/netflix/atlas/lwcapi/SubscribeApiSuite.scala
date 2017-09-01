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

import akka.actor.Actor
import akka.actor.Props
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.RouteTestTimeout
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.netflix.atlas.akka.ImperativeRequestContext
import com.netflix.atlas.akka.RequestHandler
import com.netflix.atlas.lwcapi.SubscribeApi._
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite

class SubscribeApiSuite extends FunSuite with BeforeAndAfter with ScalatestRouteTest {
  import SubscribeApiSuite._

  import scala.concurrent.duration._

  implicit val routeTestTimeout = RouteTestTimeout(5.second)

  system.actorOf(Props(new TestActor), "lwc.subscribe")

  val routes = RequestHandler.standardOptions((new SubscribeApi).routes)

  before {
    super.beforeAll()
    lastUpdate = Nil
    lastStreamId = ""
    lastKind = 'none
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
      assert(lastUpdate === Nil)
    }
  }

  test("subscribe: empty array") {
    val x = Post("/lwc/api/v1/subscribe", "[]")
    x ~> routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
      assert(lastUpdate === Nil)
    }
  }

  test("subscribe: correctly formatted expression") {
    val json = """
      |{
      |  "streamId": "abc123",
      |  "expressions": [
      |    { "expression": "nf.name,foo,:eq,:sum", "frequency": 99 }
      |  ]
      |}""".stripMargin
    Post("/lwc/api/v1/subscribe", json) ~> routes ~> check {
      assert(response.status === StatusCodes.OK)
      assert(lastUpdate.size === 1)
      assert(lastStreamId === "abc123")
      assert(lastKind === 'subscribe)
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

object SubscribeApiSuite {
  @volatile var lastUpdate: List[ExpressionMetadata] = Nil
  @volatile var lastStreamId: String = ""
  @volatile var lastKind: Symbol = 'none

  class TestActor extends Actor {

    def receive = {
      case rc @ ImperativeRequestContext(SubscribeRequest(sourceId, Nil), _) =>
        lastUpdate = Nil
        lastKind = 'subscribe
        rc.complete(HttpResponse(StatusCodes.BadRequest))
      case rc @ ImperativeRequestContext(SubscribeRequest(sourceId, values), _) =>
        lastUpdate = values
        lastStreamId = sourceId
        lastKind = 'subscribe
        rc.complete(HttpResponse(StatusCodes.OK))
    }
  }
}
