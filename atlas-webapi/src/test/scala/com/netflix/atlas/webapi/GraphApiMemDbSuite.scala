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

import akka.actor.Props
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.testkit.RouteTestTimeout
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.akka.RequestHandler
import com.netflix.atlas.core.db.MemoryDatabase
import com.netflix.atlas.json.Json
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

class GraphApiMemDbSuite extends AnyFunSuite with ScalatestRouteTest {

  import scala.concurrent.duration._

  // Set to high value to avoid spurious failures with code coverage. Typically 5s shows no
  // issues outside of running with code coverage.
  private implicit val routeTestTimeout = RouteTestTimeout(5.seconds)

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

  private val routes = RequestHandler.standardOptions((new GraphApi(config, system)).routes)

  test("sendError image if browser") {
    val agent = `User-Agent`("Mozilla/5.0 (Android; Mobile; rv:13.0) Gecko/13.0 Firefox/13.0")
    Get("/api/v1/graph?q=:foo").addHeader(agent) ~> routes ~> check {
      assert(response.status === StatusCodes.OK)
    }
  }

  test("sendError json if not browser") {
    val agent = `User-Agent`("java")
    Get("/api/v1/graph?q=:foo").addHeader(agent) ~> routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
      val msg = Json.decode[DiagnosticMessage](responseAs[String])
      assert(msg.typeName === "error")
      assert(msg.message === "IllegalStateException: unknown word ':foo'")
    }
  }

  test("sendError txt") {
    Get("/api/v1/graph?q=:foo&format=txt") ~> routes ~> check {
      assert(response.status === StatusCodes.BadRequest)
    }
  }

  test("image: IAE if not match for argument to binary op") {
    Get("/api/v1/graph?q=name,foo,:eq,:sum,name,bar,:eq,:sum,:add") ~> routes ~> check {
      assert(response.status === StatusCodes.OK)
    }
  }

  test("txt: IAE if not match for argument to binary op") {
    Get("/api/v1/graph?q=name,bar,:eq,:sum,name,foo,:eq,:sum,:add&format=txt") ~> routes ~> check {
      assert(response.status === StatusCodes.OK)
    }
  }

}
