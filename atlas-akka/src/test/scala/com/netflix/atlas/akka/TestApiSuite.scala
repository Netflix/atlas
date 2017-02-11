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
package com.netflix.atlas.akka

import org.scalatest.FunSuite
import spray.testkit.ScalatestRouteTest


class TestApiSuite extends FunSuite with ScalatestRouteTest {

  import scala.concurrent.duration._

  implicit val routeTestTimeout = RouteTestTimeout(5.second)

  val endpoint = new TestApi(system)

  test("/chunked") {
    Get("/chunked") ~> endpoint.routes ~> check {
      assert(response.status.intValue === 200)
      assert(chunks.size === 42)
    }
  }

}

