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
import com.netflix.atlas.lwcapi.SubscribeApi._
import org.scalatest.{BeforeAndAfter, FunSuite}
import spray.http.{HttpResponse, StatusCodes}
import spray.testkit.ScalatestRouteTest

class SubscribeApiSuite extends FunSuite with BeforeAndAfter with ScalatestRouteTest {
  import SubscribeApiSuite._

  import scala.concurrent.duration._

  implicit val routeTestTimeout = RouteTestTimeout(5.second)

  system.actorOf(Props(new TestActor), "lwc.subscribe")

  val endpoint = new SubscribeApi

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
    Post("/lwc/api/v1/subscribe") ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("subscribe: empty object") {
    Post("/lwc/api/v1/subscribe", "{}") ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
      assert(lastUpdate === Nil)
    }
  }

  test("subscribe: empty array") {
    val x = Post("/lwc/api/v1/subscribe", "[]")
    x ~> endpoint.routes ~> check {
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
    Post("/lwc/api/v1/subscribe", json) ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.OK)
      assert(lastUpdate.size === 1)
      assert(lastStreamId === "abc123")
      assert(lastKind === 'subscribe)
    }
  }

  test("subscribe: correctly formatted expression with sourceId") {
    val json = """
                 |{
                 |  "streamId": "abc123",
                 |  "expressions": [
                 |    { "expression": "nf.name,foo,:eq,:sum", "frequency": 99 }
                 |  ]
                 |}""".stripMargin
    Post("/lwc/api/v1/subscribe", json) ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.OK)
      assert(lastUpdate.size === 1)
      assert(lastStreamId === "abc123")
      assert(lastKind === 'subscribe)
    }
  }

  test("subscribe: bad json") {
    Post("/lwc/api/v1/subscribe", "fubar") ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("subscribe: invalid object") {
    Post("/lwc/api/v1/subscribe", "{\"foo\":\"bar\"}") ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("subscribe: expression value is null") {
    val json = s"""{
        "expressions": [
          { "expression": null }
        ]
      }"""
    Post("/lwc/api/v1/subscribe", json) ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  //
  // Unsubscribe
  //

  test("unsubscribe: no content") {
    Delete("/lwc/api/v1/subscribe") ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("unsubscribe: empty object") {
    Delete("/lwc/api/v1/subscribe", "{}") ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
      assert(lastUpdate === Nil)
    }
  }

  test("unsubscribe: empty array") {
    val x = Delete("/lwc/api/v1/subscribe", "[]")
    x ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
      assert(lastUpdate === Nil)
    }
  }

  test("unsubscribe: correctly formatted expression") {
    val json = """
                 |{
                 |  "streamId": "abc123",
                 |  "expressions": [
                 |    { "expression": "nf.name,foo,:eq,:sum", "frequency": 99 }
                 |  ]
                 |}""".stripMargin
    Delete("/lwc/api/v1/subscribe", json) ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.OK)
      assert(lastUpdate.size === 1)
      assert(lastStreamId === "abc123")
      assert(lastKind === 'unsubscribe)
    }
  }

  test("unsubscribe: correctly formatted expression with sourceId") {
    val json = """
                 |{
                 |  "streamId": "abc123",
                 |  "expressions": [
                 |    { "expression": "nf.name,foo,:eq,:sum", "frequency": 99 }
                 |  ]
                 |}""".stripMargin
    Delete("/lwc/api/v1/subscribe", json) ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.OK)
      assert(lastUpdate.size === 1)
      assert(lastStreamId === "abc123")
      assert(lastKind === 'unsubscribe)
    }
  }

  test("unsubscribe: bad json") {
    Delete("/lwc/api/v1/subscribe", "fubar") ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("unsubscribe: invalid object") {
    Delete("/lwc/api/v1/subscribe", "{\"foo\":\"bar\"}") ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("unsubscribe: expression value is null") {
    val json = s"""{
        "streamId": "abc123",
        "expressions": [
          { "expression": null }
        ]
      }"""
    Delete("/lwc/api/v1/subscribe", json) ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

}

object SubscribeApiSuite {
  @volatile var lastUpdate: List[ExpressionWithFrequency] = Nil
  @volatile var lastStreamId: String = ""
  @volatile var lastKind: Symbol = 'none

  class TestActor extends Actor {
    def receive = {
      case SubscribeRequest(sourceId, Nil) =>
        lastUpdate = Nil
        sender() ! HttpResponse(StatusCodes.BadRequest)
        lastKind = 'subscribe
      case SubscribeRequest(sourceId, values) =>
        lastUpdate = values
        lastStreamId = sourceId
        lastKind = 'subscribe
        sender() ! HttpResponse(StatusCodes.OK)
      case UnsubscribeRequest(sourceId, values) =>
        lastUpdate = values
        lastStreamId = sourceId
        lastKind = 'unsubscribe
        sender() ! HttpResponse(StatusCodes.OK)
    }
  }
}
