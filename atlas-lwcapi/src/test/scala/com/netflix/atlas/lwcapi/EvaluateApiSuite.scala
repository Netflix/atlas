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
package com.netflix.atlas.lwcapi

import com.fasterxml.jackson.databind.JsonNode
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.testkit.RouteTestTimeout
import com.netflix.atlas.core.util.SortedTagMap
import com.netflix.atlas.eval.model.LwcDiagnosticMessage
import com.netflix.atlas.eval.model.LwcEvent
import com.netflix.atlas.json.Json
import com.netflix.atlas.lwcapi.EvaluateApi.*
import com.netflix.atlas.pekko.DiagnosticMessage
import com.netflix.atlas.pekko.RequestHandler
import com.netflix.atlas.pekko.testkit.MUnitRouteSuite
import com.netflix.spectator.api.NoopRegistry

class EvaluateApiSuite extends MUnitRouteSuite {

  import scala.concurrent.duration.*

  private implicit val routeTestTimeout: RouteTestTimeout = RouteTestTimeout(5.second)

  private val sm = new StreamSubscriptionManager(new NoopRegistry)
  private val routes = RequestHandler.standardOptions(new EvaluateApi(new NoopRegistry, sm).routes)

  test("post empty payload") {
    val json = EvaluateRequest(1234L, Nil, Nil).toJson
    Post("/lwc/api/v1/evaluate", json) ~> routes ~> check {
      assertEquals(response.status, StatusCodes.OK)
    }
  }

  test("post metrics") {
    val metrics = List(Item("abc", SortedTagMap("a" -> "1"), 42.0, Nil))
    val json = EvaluateRequest(1234L, metrics, Nil, Nil).toJson
    Post("/lwc/api/v1/evaluate", json) ~> routes ~> check {
      assertEquals(response.status, StatusCodes.OK)
    }
  }

  test("post events") {
    val events = List(LwcEvent("abc", Json.decode[JsonNode]("42.0")))
    val json = EvaluateRequest(1234L, Nil, events, Nil).toJson
    Post("/lwc/api/v1/evaluate", json) ~> routes ~> check {
      assertEquals(response.status, StatusCodes.OK)
    }
  }

  test("post events, missing event id") {
    val json = """{"timestamp":1234,"events":[{"payload":42.0,"type":"event"}]}"""
    Post("/lwc/api/v1/evaluate", json) ~> routes ~> check {
      assertEquals(response.status, StatusCodes.BadRequest)
    }
  }

  test("post diagnostic message") {
    val msgs = List(LwcDiagnosticMessage("abc", DiagnosticMessage.error("bad expression")))
    val json = EvaluateRequest(1234L, Nil, Nil, msgs).toJson
    Post("/lwc/api/v1/evaluate", json) ~> routes ~> check {
      assertEquals(response.status, StatusCodes.OK)
    }
  }

  test("post missing messages field") {
    val json = """{"timestamp":12345,"metrics":[]}"""
    Post("/lwc/api/v1/evaluate", json) ~> routes ~> check {
      assertEquals(response.status, StatusCodes.OK)
    }
  }

  test("post missing metrics field") {
    val json = """{"timestamp":12345,"messages":[]}"""
    Post("/lwc/api/v1/evaluate", json) ~> routes ~> check {
      assertEquals(response.status, StatusCodes.OK)
    }
  }

  test("item samples decode") {
    val json = """{"id":"1","tags":{"name":"test"},"value":1.0}"""
    val expected = EvaluateApi.Item("1", SortedTagMap("name" -> "test"), 1.0, Nil)
    assertEquals(Json.decode[EvaluateApi.Item](json), expected)
  }
}
