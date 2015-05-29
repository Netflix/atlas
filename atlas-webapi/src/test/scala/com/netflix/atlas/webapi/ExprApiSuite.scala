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

import com.netflix.atlas.json.Json
import org.scalatest.FunSuite
import spray.http.StatusCodes
import spray.testkit.ScalatestRouteTest


class ExprApiSuite extends FunSuite with ScalatestRouteTest {

  import scala.concurrent.duration._

  implicit val routeTestTimeout = RouteTestTimeout(5.second)

  val endpoint = new ExprApi

  def testGet(uri: String)(f: => Unit): Unit = {
    test(uri) {
      Get(uri) ~> endpoint.routes ~> check(f)
    }
  }

  testGet("/api/v1/expr") {
    assert(response.status === StatusCodes.BadRequest)
  }

  testGet("/api/v1/expr?q=name,sps,:eq") {
    assert(response.status === StatusCodes.OK)
    val data = Json.decode[List[ExprApiSuite.Output]](responseAs[String])
    assert(data.size === 4)
  }

  testGet("/api/v1/expr?q=name,sps,:eq,:sum,$name,:legend&vocab=style") {
    assert(response.status === StatusCodes.OK)
    val data = Json.decode[List[ExprApiSuite.Output]](responseAs[String])
    assert(data.size === 7)
  }

  testGet("/api/v1/expr?q=name,sps,:eq,:sum,$name,:legend,foo,:sset,foo,:get") {
    assert(response.status === StatusCodes.OK)
    val data = Json.decode[List[ExprApiSuite.Output]](responseAs[String])
    assert(data.size === 11)
    assert(data.last.context.variables("foo") == "name,sps,:eq,:sum,$name,:legend")
  }

  testGet("/api/v1/expr?q=name,sps,:eq,:sum,$name,:legend&vocab=query") {
    assert(response.status === StatusCodes.BadRequest)
  }

  testGet("/api/v1/expr?q=name,sps,:eq,cluster,:has&vocab=query") {
    assert(response.status === StatusCodes.BadRequest)
  }

  testGet("/api/v1/expr?q=name,sps,:eq,:clear&vocab=query") {
    assert(response.status === StatusCodes.BadRequest)
  }

  testGet("/api/v1/expr?q=name,sps,:eq,:sum,$name,:legend,foo") {
    assert(response.status === StatusCodes.BadRequest)
  }

  testGet("/api/v1/expr?q=name,sps,:eq,:sum,$name,:legend,foo,:clear") {
    assert(response.status === StatusCodes.BadRequest)
  }

}

object ExprApiSuite {
  case class Output(program: List[String], context: Context)
  case class Context(stack: List[String], variables: Map[String, String])
}
