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

import akka.actor.Actor
import akka.actor.Props
import com.netflix.atlas.lwcapi.RegisterApi.{DeleteRequest, RegisterRequest}
import org.scalatest.FunSuite
import spray.http.HttpResponse
import spray.http.StatusCodes
import spray.testkit.ScalatestRouteTest

class RegisterApiSuite extends FunSuite with ScalatestRouteTest {

  import RegisterApiSuite._

  import scala.concurrent.duration._

  implicit val routeTestTimeout = RouteTestTimeout(5.second)

  system.actorOf(Props(new TestActor), "lwc.register")

  val endpoint = new RegisterApi

  test("publish no content") {
    Post("/lwc/api/v1/register") ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("publish empty object") {
    Post("/lwc/api/v1/register", "{}") ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
      assert(lastUpdate === Nil)
    }
  }

  test("publish empty array") {
    val x = Post("/lwc/api/v1/register", "[]")
    x ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
      assert(lastUpdate === Nil)
    }
  }

  test("publish correctly formatted expression") {
    val json = """
      |{
      |  "expressions": [
      |    { "expression": "nf.name,foo,:eq,:sum", "frequency": 99 }
      |  ]
      |}""".stripMargin
    Post("/lwc/api/v1/register", json) ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.OK)
      assert(lastUpdate.size === 1)
    }
  }

  test("publish bad json") {
    Post("/lwc/api/v1/register", "fubar") ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("publish invalid object") {
    Post("/lwc/api/v1/register", "{\"foo\":\"bar\"}") ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("expression value is null") {
    val json = s"""{
        "cluster": "this",
        "expressions": [
          { "expression": null }
        ]
      }"""
    Post("/lwc/api/v1/register", json) ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }
}

object RegisterApiSuite {
  @volatile var lastUpdate: List[ExpressionWithFrequency] = Nil

  class TestActor extends Actor {
    def receive = {
      case RegisterRequest(Nil) =>
        lastUpdate = Nil
        sender() ! HttpResponse(StatusCodes.BadRequest)
      case RegisterRequest(values) =>
        lastUpdate = values
        sender() ! HttpResponse(StatusCodes.OK)
      case DeleteRequest(values) =>
        lastUpdate = values
        sender() ! HttpResponse(StatusCodes.OK)
    }
  }
}
