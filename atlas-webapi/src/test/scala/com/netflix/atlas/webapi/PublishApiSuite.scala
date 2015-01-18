/*
 * Copyright 2015 Netflix, Inc.
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

import java.io.PrintStream
import java.net.URI

import akka.actor.Actor
import akka.actor.Props
import com.netflix.atlas.core.db.StaticDatabase
import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.core.util.Hash
import com.netflix.atlas.core.util.PngImage
import com.netflix.atlas.core.util.Streams
import com.netflix.atlas.core.util.Strings
import com.netflix.atlas.test.GraphAssertions
import com.netflix.atlas.webapi.PublishApi.PublishRequest
import org.scalatest.FunSuite
import spray.http.HttpResponse
import spray.http.MediaTypes._
import spray.http.HttpEntity
import spray.http.StatusCodes
import spray.testkit.ScalatestRouteTest


class PublishApiSuite extends FunSuite with ScalatestRouteTest {

  import scala.concurrent.duration._
  import PublishApiSuite._

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
      assert(response.status === StatusCodes.OK)
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

  test("publish bad json") {
    Post("/api/v1/publish", "fubar") ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("publish-fast alias") {
    Post("/api/v1/publish-fast", "{}") ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.OK)
      assert(lastUpdate === Nil)
    }
  }
}

object PublishApiSuite {

  @volatile var lastUpdate: List[Datapoint] = Nil

  class TestActor extends Actor {
    def receive = {
      case PublishRequest(vs) =>
        lastUpdate = vs
        sender() ! HttpResponse(StatusCodes.OK)
    }
  }
}
