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

import akka.actor.Props
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.RouteTestTimeout
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.netflix.atlas.akka.RequestHandler
import com.netflix.atlas.core.db.MemoryDatabase
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite


class GraphApiMemDbSuite extends FunSuite with ScalatestRouteTest {

  import scala.concurrent.duration._

  // Set to high value to avoid spurious failures with code coverage. Typically 5s shows no
  // issues outside of running with code coverage.
  private implicit val routeTestTimeout = RouteTestTimeout(5.seconds)

  private val db = MemoryDatabase(ConfigFactory.parseString(
    """
      |atlas.core.db {
      |  rebuild-frequency = 10s
      |  num-blocks = 2
      |  block-size = 60
      |  test-mode = true
      |  intern-while-building = true
      |}
    """.stripMargin))
  system.actorOf(Props(new LocalDatabaseActor(db)), "db")

  private val routes = RequestHandler.standardOptions((new GraphApi).routes)

  test("sendError image") {
    Get("/api/v1/graph?q=:foo") ~> routes ~> check {
      assert(response.status === StatusCodes.OK)
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
