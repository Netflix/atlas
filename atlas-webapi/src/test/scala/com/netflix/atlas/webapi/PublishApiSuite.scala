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
package com.netflix.atlas.webapi

import akka.actor.Actor
import akka.actor.Props
import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.webapi.PublishApi.FailureMessage
import com.netflix.atlas.webapi.PublishApi.PublishRequest
import org.scalatest.FunSuite
import spray.http.HttpResponse
import spray.http.StatusCodes
import spray.testkit.ScalatestRouteTest


class PublishApiSuite extends FunSuite with ScalatestRouteTest {

  import PublishApiSuite._

  import scala.concurrent.duration._

  implicit val routeTestTimeout = RouteTestTimeout(5.second)

  system.actorOf(Props(new TestActor), "publish")

  val endpoint = new PublishApi

  test("publish no content") {
    Post("/api/v1/publish") ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("publish empty object") {
    Post("/api/v1/publish", "{}") ~> endpoint.routes ~> check {
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
    Post("/api/v1/publish", json) ~> endpoint.routes ~> check {
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
    Post("/api/v1/publish", json) ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("partial failure") {
    val json = s"""{
        "metrics": [
          {
            "tags": {"name": "cpuUser"},
            "timestamp": ${System.currentTimeMillis() /  1000},
            "value": 42.0
          },
          {
            "tags": {"name": "cpuSystem"},
            "timestamp": ${System.currentTimeMillis()},
            "value": 42.0
          }
        ]
      }"""
    Post("/api/v1/publish", json) ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.Accepted)
      assert(lastUpdate === PublishApi.decodeBatch(json).tail)
    }
  }

  test("publish bad json") {
    Post("/api/v1/publish", "fubar") ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("publish invalid object") {
    Post("/api/v1/publish", "{\"foo\":\"bar\"}") ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("publish tag value is null") {
    val json = s"""{
        "metrics": [
          {
            "tags": {"name": "cpuUser", "bad": null},
            "timestamp": ${System.currentTimeMillis() /  1000},
            "value": 42.0
          }
        ]
      }"""
    Post("/api/v1/publish", json) ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("publish tag value is empty") {
    val json = s"""{
        "metrics": [
          {
            "tags": {"name": "cpuUser", "bad": ""},
            "timestamp": ${System.currentTimeMillis() /  1000},
            "value": 42.0
          }
        ]
      }"""
    Post("/api/v1/publish", json) ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("publish validation failure") {
    val json = s"""{
        "metrics": [
          {
            "tags": {"no-name": "cpuUser"},
            "timestamp": ${System.currentTimeMillis() /  1000},
            "value": 42.0
          }
        ]
      }"""
    Post("/api/v1/publish", json) ~> endpoint.routes ~> check {
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
    Post("/api/v1/publish", json) ~> endpoint.routes ~> check {
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
    Post("/api/v1/publish-fast", json) ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.OK)
      assert(lastUpdate === PublishApi.decodeBatch(json))
    }
  }
}

object PublishApiSuite {

  @volatile var lastUpdate: List[Datapoint] = Nil

  class TestActor extends Actor {
    def receive = {
      case PublishRequest(Nil, Nil) =>
        lastUpdate = Nil
        sender() ! HttpResponse(StatusCodes.BadRequest)
      case PublishRequest(Nil, failures) =>
        lastUpdate = Nil
        val msg = FailureMessage.error(failures)
        sender() ! HttpResponse(StatusCodes.BadRequest, msg.toJson)
      case PublishRequest(values, Nil) =>
        lastUpdate = values
        sender() ! HttpResponse(StatusCodes.OK)
      case PublishRequest(values, failures) =>
        lastUpdate = values
        val msg = FailureMessage.partial(failures)
        sender() ! HttpResponse(StatusCodes.Accepted, msg.toJson)
    }
  }
}
