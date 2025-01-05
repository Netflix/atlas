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

import org.apache.pekko.actor.Props
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.headers.*
import org.apache.pekko.http.scaladsl.testkit.RouteTestTimeout
import com.netflix.atlas.core.db.MemoryDatabase
import com.netflix.atlas.json.Json
import com.netflix.atlas.pekko.DiagnosticMessage
import com.netflix.atlas.pekko.RequestHandler
import com.netflix.atlas.pekko.testkit.MUnitRouteSuite
import com.typesafe.config.ConfigFactory

class GraphApiMemDbSuite extends MUnitRouteSuite {

  import scala.concurrent.duration.*

  // Set to high value to avoid spurious failures with code coverage. Typically 5s shows no
  // issues outside of running with code coverage.
  private implicit val routeTestTimeout: RouteTestTimeout = RouteTestTimeout(5.seconds)

  private val dbConfig = ConfigFactory.parseString("""
      |atlas.core.db {
      |  rebuild-frequency = 10s
      |  num-blocks = 2
      |  block-size = 60
      |  test-mode = true
      |  intern-while-building = true
      |}
    """.stripMargin)

  private val config = dbConfig.withFallback(ConfigFactory.load())

  private val db = MemoryDatabase(dbConfig)
  system.actorOf(Props(new LocalDatabaseActor(db)), "db")

  private val routes = RequestHandler.standardOptions(new GraphApi(config, system).routes)

  test("sendError image if browser") {
    val agent = `User-Agent`("Mozilla/5.0 (Android; Mobile; rv:13.0) Gecko/13.0 Firefox/13.0")
    Get("/api/v1/graph?q=:foo").addHeader(agent) ~> routes ~> check {
      assertEquals(response.status, StatusCodes.OK)
    }
  }

  test("sendError json if not browser") {
    val agent = `User-Agent`("java")
    Get("/api/v1/graph?q=:foo").addHeader(agent) ~> routes ~> check {
      assertEquals(response.status, StatusCodes.BadRequest)
      val msg = Json.decode[DiagnosticMessage](responseAs[String])
      assertEquals(msg.typeName, "error")
      assertEquals(msg.message, "IllegalStateException: unknown word ':foo'")
    }
  }

  test("sendError txt") {
    Get("/api/v1/graph?q=:foo&format=txt") ~> routes ~> check {
      assertEquals(response.status, StatusCodes.BadRequest)
    }
  }

  test("image: IAE if not match for argument to binary op") {
    Get("/api/v1/graph?q=name,foo,:eq,:sum,name,bar,:eq,:sum,:add") ~> routes ~> check {
      assertEquals(response.status, StatusCodes.OK)
    }
  }

  test("txt: IAE if not match for argument to binary op") {
    Get("/api/v1/graph?q=name,bar,:eq,:sum,name,foo,:eq,:sum,:add&format=txt") ~> routes ~> check {
      assertEquals(response.status, StatusCodes.OK)
    }
  }

}
