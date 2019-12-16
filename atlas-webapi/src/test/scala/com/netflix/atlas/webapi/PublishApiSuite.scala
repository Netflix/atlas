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
package com.netflix.atlas.webapi

import akka.actor.Actor
import akka.actor.Props
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.RouteTestTimeout
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.netflix.atlas.akka.RequestHandler
import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.webapi.PublishApi.FailureMessage
import com.netflix.atlas.webapi.PublishApi.PublishRequest
import org.scalatest.funsuite.AnyFunSuite

class PublishApiSuite extends AnyFunSuite with ScalatestRouteTest {

  import PublishApiSuite._

  import scala.concurrent.duration._

  implicit val routeTestTimeout = RouteTestTimeout(5.second)

  system.actorOf(Props(new TestActor), "publish")

  val routes = RequestHandler.standardOptions((new PublishApi).routes)

  test("publish no content") {
    Post("/api/v1/publish") ~> routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("publish empty object") {
    Post("/api/v1/publish", "{}") ~> routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
      assert(lastUpdate === Nil)
    }
  }

  test("publish simple batch") {
    val json = s"""{
        "tags": {
          "cluster": "foo",
          "node": "i-123"
        },
        "metrics": [
          {
            "tags": {"name": "cpuUser"},
            "timestamp": ${System.currentTimeMillis()},
            "value": 42.0
          }
        ]
      }"""
    Post("/api/v1/publish", json) ~> routes ~> check {
      assert(response.status === StatusCodes.OK)
      assert(lastUpdate === PublishApi.decodeBatch(json))
    }
  }

  test("publish too old") {
    val json = s"""{
        "metrics": [
          {
            "tags": {"name": "cpuUser"},
            "timestamp": ${System.currentTimeMillis() / 1000},
            "value": 42.0
          }
        ]
      }"""
    Post("/api/v1/publish", json) ~> routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("partial failure") {
    val json = s"""{
        "metrics": [
          {
            "tags": {"name": "cpuUser"},
            "timestamp": ${System.currentTimeMillis() / 1000},
            "value": 42.0
          },
          {
            "tags": {"name": "cpuSystem"},
            "timestamp": ${System.currentTimeMillis()},
            "value": 42.0
          }
        ]
      }"""
    Post("/api/v1/publish", json) ~> routes ~> check {
      assert(response.status === StatusCodes.Accepted)
      assert(lastUpdate === PublishApi.decodeBatch(json).tail)
    }
  }

  test("publish bad json") {
    Post("/api/v1/publish", "fubar") ~> routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("publish invalid object") {
    Post("/api/v1/publish", "{\"foo\":\"bar\"}") ~> routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("publish tag value is null") {
    val json = s"""{
        "metrics": [
          {
            "tags": {"name": "cpuUser", "bad": null},
            "timestamp": ${System.currentTimeMillis() / 1000},
            "value": 42.0
          }
        ]
      }"""
    Post("/api/v1/publish", json) ~> routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("publish tag value is empty") {
    val json = s"""{
        "metrics": [
          {
            "tags": {"name": "cpuUser", "bad": ""},
            "timestamp": ${System.currentTimeMillis() / 1000},
            "value": 42.0
          }
        ]
      }"""
    Post("/api/v1/publish", json) ~> routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("publish validation failure") {
    val json = s"""{
        "metrics": [
          {
            "tags": {"no-name": "cpuUser"},
            "timestamp": ${System.currentTimeMillis() / 1000},
            "value": 42.0
          }
        ]
      }"""
    Post("/api/v1/publish", json) ~> routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("publish partial validation failure") {
    val json = s"""{
        "metrics": [
          {
            "tags": {"no-name": "cpuUser"},
            "timestamp": ${System.currentTimeMillis()},
            "value": 42.0
          },
          {
            "tags": {"name": "cpuUser"},
            "timestamp": ${System.currentTimeMillis()},
            "value": 42.0
          }
        ]
      }"""
    Post("/api/v1/publish", json) ~> routes ~> check {
      assert(response.status === StatusCodes.Accepted)
    }
  }

  test("publish-fast alias") {
    val json = s"""{
        "tags": {
          "cluster": "foo",
          "node": "i-123"
        },
        "metrics": [
          {
            "tags": {"name": "cpuUser"},
            "timestamp": ${System.currentTimeMillis()},
            "value": 42.0
          }
        ]
      }"""
    Post("/api/v1/publish-fast", json) ~> routes ~> check {
      assert(response.status === StatusCodes.OK)
      assert(lastUpdate === PublishApi.decodeBatch(json))
    }
  }
}

object PublishApiSuite {

  @volatile var lastUpdate: List[Datapoint] = Nil

  class TestActor extends Actor {

    def receive: Receive = {
      case req @ PublishRequest(Nil, Nil, _, _) =>
        lastUpdate = Nil
        req.complete(HttpResponse(StatusCodes.BadRequest))
      case req @ PublishRequest(Nil, failures, _, _) =>
        lastUpdate = Nil
        val msg = FailureMessage.error(failures)
        req.complete(HttpResponse(StatusCodes.BadRequest, entity = msg.toJson))
      case req @ PublishRequest(values, Nil, _, _) =>
        lastUpdate = values
        req.complete(HttpResponse(StatusCodes.OK))
      case req @ PublishRequest(values, failures, _, _) =>
        lastUpdate = values
        val msg = FailureMessage.partial(failures)
        req.complete(HttpResponse(StatusCodes.Accepted, entity = msg.toJson))
    }
  }
}
