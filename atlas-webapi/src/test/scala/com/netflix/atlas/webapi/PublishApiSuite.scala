/*
 * Copyright 2014-2025 Netflix, Inc.
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

import org.apache.pekko.actor.Actor
import org.apache.pekko.actor.Props
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.testkit.RouteTestTimeout
import com.netflix.atlas.core.model.DatapointTuple
import com.netflix.atlas.pekko.RequestHandler
import com.netflix.atlas.pekko.testkit.MUnitRouteSuite
import com.netflix.atlas.webapi.PublishApi.FailureMessage
import com.netflix.atlas.webapi.PublishApi.PublishRequest

class PublishApiSuite extends MUnitRouteSuite {

  import PublishApiSuite.*

  import scala.concurrent.duration.*

  private implicit val routeTestTimeout: RouteTestTimeout = RouteTestTimeout(5.second)

  system.actorOf(Props(new TestActor), "publish")

  private val routes = RequestHandler.standardOptions((new PublishApi).routes)

  test("publish no content") {
    Post("/api/v1/publish") ~> routes ~> check {
      assertEquals(response.status, StatusCodes.BadRequest)
    }
  }

  test("publish empty object") {
    Post("/api/v1/publish", "{}") ~> routes ~> check {
      assertEquals(response.status, StatusCodes.BadRequest)
      assertEquals(lastUpdate, Nil)
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
      assertEquals(response.status, StatusCodes.OK)
      assertEquals(lastUpdate, PublishPayloads.decodeBatch(json))
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
      assertEquals(response.status, StatusCodes.BadRequest)
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
      assertEquals(response.status, StatusCodes.Accepted)
      assertEquals(lastUpdate, PublishPayloads.decodeBatch(json).tail)
    }
  }

  test("publish bad json") {
    Post("/api/v1/publish", "fubar") ~> routes ~> check {
      assertEquals(response.status, StatusCodes.BadRequest)
    }
  }

  test("publish invalid object") {
    Post("/api/v1/publish", "{\"foo\":\"bar\"}") ~> routes ~> check {
      assertEquals(response.status, StatusCodes.BadRequest)
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
      assertEquals(response.status, StatusCodes.BadRequest)
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
      assertEquals(response.status, StatusCodes.BadRequest)
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
      assertEquals(response.status, StatusCodes.BadRequest)
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
      assertEquals(response.status, StatusCodes.Accepted)
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
      assertEquals(response.status, StatusCodes.OK)
      assertEquals(lastUpdate, PublishPayloads.decodeBatch(json))
    }
  }
}

object PublishApiSuite {

  @volatile var lastUpdate: List[DatapointTuple] = Nil

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
