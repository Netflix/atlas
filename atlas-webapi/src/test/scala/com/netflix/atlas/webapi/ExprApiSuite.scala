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

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.RouteTestTimeout
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.netflix.atlas.akka.RequestHandler
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.MathExpr
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.model.StyleVocabulary
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.json.Json
import org.scalatest.funsuite.AnyFunSuite

class ExprApiSuite extends AnyFunSuite with ScalatestRouteTest {

  import scala.concurrent.duration._

  implicit val routeTestTimeout = RouteTestTimeout(5.second)

  val endpoint = new ExprApi

  def testGet(uri: String)(f: => Unit): Unit = {
    test(uri) {
      Get(uri) ~> routes ~> check(f)
    }
  }

  private def routes: Route = RequestHandler.standardOptions(endpoint.routes)

  testGet("/api/v1/expr") {
    assert(response.status === StatusCodes.BadRequest)
  }

  testGet("/api/v1/expr?q=name,sps,:eq") {
    assert(response.status === StatusCodes.OK)
    val data = Json.decode[List[ExprApiSuite.Output]](responseAs[String])
    assert(data.size === 4)
  }

  testGet("/api/v1/expr/debug") {
    assert(response.status === StatusCodes.BadRequest)
  }

  testGet("/api/v1/expr/debug?q=name,sps,:eq") {
    assert(response.status === StatusCodes.OK)
    val data = Json.decode[List[ExprApiSuite.Output]](responseAs[String])
    assert(data.size === 4)
  }

  testGet("/api/v1/expr/debug?q=name,sps,:eq,:sum,$name,:legend&vocab=style") {
    assert(response.status === StatusCodes.OK)
    val data = Json.decode[List[ExprApiSuite.Output]](responseAs[String])
    assert(data.size === 7)
  }

  testGet("/api/v1/expr/debug?q=name,sps,:eq,:sum,$name,:legend,foo,:sset,foo,:get") {
    assert(response.status === StatusCodes.OK)
    val data = Json.decode[List[ExprApiSuite.Output]](responseAs[String])
    assert(data.size === 11)
    assert(data.last.context.variables("foo") == "name,sps,:eq,:sum,$name,:legend")
  }

  testGet("/api/v1/expr/debug?q=name,sps,:eq,:sum,$name,:legend&vocab=query") {
    assert(response.status === StatusCodes.BadRequest)
  }

  testGet("/api/v1/expr/debug?q=name,sps,:eq,cluster,:has&vocab=query") {
    assert(response.status === StatusCodes.BadRequest)
  }

  testGet("/api/v1/expr/debug?q=name,sps,:eq,:clear&vocab=query") {
    assert(response.status === StatusCodes.BadRequest)
  }

  testGet("/api/v1/expr/debug?q=name,sps,:eq,:sum,$name,:legend,foo") {
    assert(response.status === StatusCodes.BadRequest)
  }

  testGet("/api/v1/expr/debug?q=name,sps,:eq,:sum,$name,:legend,foo,:clear") {
    assert(response.status === StatusCodes.BadRequest)
  }

  testGet("/api/v1/expr/normalize") {
    assert(response.status === StatusCodes.BadRequest)
  }

  testGet("/api/v1/expr/normalize?q=name,sps,:eq") {
    assert(response.status === StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assert(data === List("name,sps,:eq,:sum"))
  }

  testGet("/api/v1/expr/normalize?q=name,sps,:eq,:dup,2,:mul,:swap") {
    assert(response.status === StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assert(data === List("name,sps,:eq,:sum,2.0,:mul", "name,sps,:eq,:sum"))
  }

  testGet(
    "/api/v1/expr/normalize?q=(,name,:swap,:eq,nf.cluster,foo,:eq,:and,:sum,),foo,:sset,cpu,foo,:fcall,disk,foo,:fcall"
  ) {
    assert(response.status === StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    val expected =
      List("name,cpu,:eq,:sum", "name,disk,:eq,:sum", ":list,(,nf.cluster,foo,:eq,:cq,),:each")
    assert(data === expected)
  }

  testGet("/api/v1/expr/complete") {
    assert(response.status === StatusCodes.BadRequest)
  }

  testGet("/api/v1/expr/complete?q=name,sps,:eq") {
    assert(response.status === StatusCodes.OK)
    val data = Json.decode[List[ExprApiSuite.Candidate]](responseAs[String]).map(_.name)
    assert(data.nonEmpty)
    assert(!data.contains("add"))
  }

  testGet("/api/v1/expr/complete?q=name,sps,:eq,(,nf.cluster,)") {
    assert(response.status === StatusCodes.OK)
    val data = Json.decode[List[ExprApiSuite.Candidate]](responseAs[String]).map(_.name)
    assert(data === List("by", "by", "offset"))
  }

  // TODO: Right now these fail. As a future improvement suggestions should be possible within
  // a list context by ignoring everything outside of the list.
  testGet("/api/v1/expr/complete?q=name,sps,:eq,(,nf.cluster,name") {
    assert(response.status === StatusCodes.BadRequest)
  }

  testGet("/api/v1/expr/complete?q=1,2") {
    assert(response.status === StatusCodes.OK)
    val data = Json.decode[List[ExprApiSuite.Candidate]](responseAs[String]).map(_.name)
    assert(data.nonEmpty)
    assert(data.contains("add"))
  }

  testGet("/api/v1/expr/queries?q=1,2") {
    assert(response.status === StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assert(data.isEmpty)
  }

  testGet("/api/v1/expr/queries?q=name,sps,:eq") {
    assert(response.status === StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assert(data === List("name,sps,:eq"))
  }

  testGet("/api/v1/expr/queries?q=name,sps,:eq,(,nf.cluster,),:by") {
    assert(response.status === StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assert(data === List("name,sps,:eq"))
  }

  testGet("/api/v1/expr/queries?q=name,sps,:eq,(,nf.cluster,),:by,:dup,:dup,4,:add") {
    assert(response.status === StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assert(data === List("name,sps,:eq"))
  }

  testGet("/api/v1/expr/queries?q=name,sps,:eq,(,nf.cluster,),:by,:true,:sum,name,:has,:add") {
    assert(response.status === StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assert(data === List(":true", "name,:has", "name,sps,:eq"))
  }

  import Query._

  private def normalize(expr: String): List[String] = {
    val interpreter = Interpreter(StyleVocabulary.allWords)
    ExprApi.normalize(expr, interpreter)
  }

  test("normalize query order") {
    val q1 = "app,foo,:eq,name,cpu,:eq,:and"
    val q2 = "name,cpu,:eq,app,foo,:eq,:and"
    assert(normalize(q1) === normalize(q2))
  }

  test("normalize single query") {
    val e1 = DataExpr.Sum(And(Equal("app", "foo"), Equal("name", "cpuUser")))
    val add = StyleExpr(MathExpr.Add(e1, e1), Map.empty)
    assert(
      normalize(add.toString) === List(
          ":true,:sum,:true,:sum,:add",
          ":list,(,app,foo,:eq,name,cpuUser,:eq,:and,:cq,),:each"
        )
    )
  }

  test("normalize common query") {
    val e1 = DataExpr.Sum(And(Equal("app", "foo"), Equal("name", "cpuUser")))
    val e2 = DataExpr.Sum(And(Equal("app", "foo"), Equal("name", "cpuSystem")))
    val add = StyleExpr(MathExpr.Add(e1, e2), Map.empty)
    assert(
      normalize(add.toString) === List(
          "name,cpuUser,:eq,:sum,name,cpuSystem,:eq,:sum,:add",
          ":list,(,app,foo,:eq,:cq,),:each"
        )
    )
  }

  test("normalize :avg") {
    val avg = "app,foo,:eq,name,cpuUser,:eq,:and,:avg"
    assert(normalize(avg) === List(avg))
  }

  test("normalize :dist-avg") {
    val avg = "app,foo,:eq,name,cpuUser,:eq,:and,:dist-avg"
    assert(normalize(avg) === List(avg))
  }

  test("normalize :dist-avg,(,nf.cluster,),:by") {
    val avg = "app,foo,:eq,name,cpuUser,:eq,:and,:dist-avg,(,nf.cluster,),:by"
    assert(normalize(avg) === List(avg))
  }

  test("normalize :dist-stddev") {
    val stddev = "app,foo,:eq,name,cpuUser,:eq,:and,:dist-stddev"
    assert(normalize(stddev) === List(stddev))
  }

  test("normalize :dist-max") {
    val max = "app,foo,:eq,name,cpuUser,:eq,:and,:dist-max"
    assert(normalize(max) === List(max))
  }

  test("normalize :dist-avg + expr2") {
    val avg =
      "app,foo,:eq,name,cpuUser,:eq,:and,:dist-avg,app,foo,:eq,name,cpuSystem,:eq,:and,:max"
    assert(
      normalize(avg) === List(
          "name,cpuUser,:eq,:dist-avg",
          "name,cpuSystem,:eq,:max",
          ":list,(,app,foo,:eq,:cq,),:each"
        )
    )
  }

  test("normalize :avg,(,nf.cluster,),:by,:pct") {
    val avg = "app,foo,:eq,name,cpuUser,:eq,:and,:avg,(,nf.cluster,),:by,:pct"
    assert(normalize(avg) === List(avg))
  }

  test("normalize :des-fast") {
    val expr = "app,foo,:eq,name,cpuUser,:eq,:and,:sum,:des-fast"
    assert(normalize(expr) === List(expr))
  }

  test("normalize legend vars, include parenthesis") {
    val expr = "name,cpuUser,:eq,:sum,$name,:legend"
    val expected = "name,cpuUser,:eq,:sum,$(name),:legend"
    assert(normalize(expr) === List(expected))
  }

  test("normalize legend vars, noop if parens already present") {
    val expr = "name,cpuUser,:eq,:sum,$(name),:legend"
    assert(normalize(expr) === List(expr))
  }

  test("normalize legend vars, mix") {
    val expr = "name,cpuUser,:eq,:sum,foo$name$abc bar$(def)baz,:legend"
    val expected = "name,cpuUser,:eq,:sum,foo$(name)$(abc) bar$(def)baz,:legend"
    assert(normalize(expr) === List(expected))
  }
}

object ExprApiSuite {

  case class Output(program: List[String], context: Context)

  case class Context(stack: List[String], variables: Map[String, String])

  case class Candidate(name: String, signature: String, description: String)
}
