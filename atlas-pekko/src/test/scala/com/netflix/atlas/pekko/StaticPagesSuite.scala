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
package com.netflix.atlas.pekko

import com.netflix.atlas.pekko.testkit.MUnitRouteSuite
import org.apache.pekko.http.scaladsl.model.StatusCodes
import com.typesafe.config.ConfigFactory

class StaticPagesSuite extends MUnitRouteSuite {

  val endpoint = new StaticPages(ConfigFactory.load())

  test("/static/test") {
    Get("/static/test") ~> endpoint.routes ~> check {
      assertEquals(responseAs[String], "test text file\n")
    }
  }

  test("/static") {
    Get("/static") ~> endpoint.routes ~> check {
      assertEquals(response.status, StatusCodes.OK)
      assert(responseAs[String].contains("Index Page"))
    }
  }

  test("/static/") {
    Get("/static/") ~> endpoint.routes ~> check {
      assertEquals(response.status, StatusCodes.OK)
      assert(responseAs[String].contains("Index Page"))
    }
  }

  test("/") {
    Get("/") ~> endpoint.routes ~> check {
      assertEquals(response.status, StatusCodes.MovedPermanently)
      val loc = response.headers.find(_.is("location")).map(_.value)
      assertEquals(loc, Some("/ui"))
    }
  }

  test("/ui") {
    Get("/ui") ~> endpoint.routes ~> check {
      assertEquals(response.status, StatusCodes.OK)
      assert(responseAs[String].contains("Index Page"))
    }
  }

  test("/ui/foo/bar") {
    Get("/ui/foo/bar") ~> endpoint.routes ~> check {
      assertEquals(response.status, StatusCodes.OK)
      assert(responseAs[String].contains("Index Page"))
    }
  }
}
