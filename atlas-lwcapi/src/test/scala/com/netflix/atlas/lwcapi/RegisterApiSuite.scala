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
import spray.http.{HttpResponse, StatusCodes}
import spray.testkit.ScalatestRouteTest

class RegisterApiSuite extends FunSuite with BeforeAndAfter with  ScalatestRouteTest {
  import RegisterApiSuite._

  import scala.concurrent.duration._

  implicit val routeTestTimeout = RouteTestTimeout(5.second)

  system.actorOf(Props(new TestActor), "lwc.register")

  val endpoint = new RegisterApi

  before {
    super.beforeAll()
    lastUpdate = Nil
    lastSinkId = None
    lastKind = 'none
  }

  //
  // Publish
  //

  test("publish: no content") {
    Post("/lwc/api/v1/register") ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("publish: empty object") {
    Post("/lwc/api/v1/register", "{}") ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
      assert(lastUpdate === Nil)
    }
  }

  test("publish: empty array") {
    val x = Post("/lwc/api/v1/register", "[]")
    x ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
      assert(lastUpdate === Nil)
    }
  }

  test("publish: correctly formatted expression") {
    val json = """
      |{
      |  "expressions": [
      |    { "expression": "nf.name,foo,:eq,:sum", "frequency": 99 }
      |  ]
      |}""".stripMargin
    Post("/lwc/api/v1/register", json) ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.OK)
      assert(lastUpdate.size === 1)
      assert(lastSinkId === None)
      assert(lastKind === 'register)
    }
  }

  test("publish: correctly formatted expression with sinkId") {
    val json = """
                 |{
                 |  "sinkId": "abc123",
                 |  "expressions": [
                 |    { "expression": "nf.name,foo,:eq,:sum", "frequency": 99 }
                 |  ]
                 |}""".stripMargin
    Post("/lwc/api/v1/register", json) ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.OK)
      assert(lastUpdate.size === 1)
      assert(lastSinkId === Some("abc123"))
      assert(lastKind === 'register)
    }
  }

  test("publish: bad json") {
    Post("/lwc/api/v1/register", "fubar") ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("publish: invalid object") {
    Post("/lwc/api/v1/register", "{\"foo\":\"bar\"}") ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("publish: expression value is null") {
    val json = s"""{
        "expressions": [
          { "expression": null }
        ]
      }"""
    Post("/lwc/api/v1/register", json) ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  //
  // Delete
  //


  test("delete: no content") {
    Delete("/lwc/api/v1/register") ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("delete: empty object") {
    Delete("/lwc/api/v1/register", "{}") ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
      assert(lastUpdate === Nil)
    }
  }

  test("delete: empty array") {
    val x = Delete("/lwc/api/v1/register", "[]")
    x ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
      assert(lastUpdate === Nil)
    }
  }

  test("delete: correctly formatted expression") {
    val json = """
                 |{
                 |  "expressions": [
                 |    { "expression": "nf.name,foo,:eq,:sum", "frequency": 99 }
                 |  ]
                 |}""".stripMargin
    Delete("/lwc/api/v1/register", json) ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.OK)
      assert(lastUpdate.size === 1)
      assert(lastSinkId === None)
      assert(lastKind === 'delete)
    }
  }

  test("delete: correctly formatted expression with sinkId") {
    val json = """
                 |{
                 |  "sinkId": "abc123",
                 |  "expressions": [
                 |    { "expression": "nf.name,foo,:eq,:sum", "frequency": 99 }
                 |  ]
                 |}""".stripMargin
    Delete("/lwc/api/v1/register", json) ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.OK)
      assert(lastUpdate.size === 1)
      assert(lastSinkId === Some("abc123"))
      assert(lastKind === 'delete)
    }
  }

  test("delete: bad json") {
    Delete("/lwc/api/v1/register", "fubar") ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("delete: invalid object") {
    Delete("/lwc/api/v1/register", "{\"foo\":\"bar\"}") ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("delete: expression value is null") {
    val json = s"""{
        "expressions": [
          { "expression": null }
        ]
      }"""
    Delete("/lwc/api/v1/register", json) ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

}

object RegisterApiSuite {
  @volatile var lastUpdate: List[ExpressionWithFrequency] = Nil
  @volatile var lastSinkId: Option[String] = None
  @volatile var lastKind: Symbol = 'none

  class TestActor extends Actor {
    def receive = {
      case RegisterRequest(sinkId, Nil) =>
        lastUpdate = Nil
        sender() ! HttpResponse(StatusCodes.BadRequest)
        lastKind = 'register
      case RegisterRequest(sinkId, values) =>
        lastUpdate = values
        lastSinkId = sinkId
        lastKind = 'register
        sender() ! HttpResponse(StatusCodes.OK)
      case DeleteRequest(sinkId, values) =>
        lastUpdate = values
        lastSinkId = sinkId
        lastKind = 'delete
        sender() ! HttpResponse(StatusCodes.OK)
    }
  }
}
