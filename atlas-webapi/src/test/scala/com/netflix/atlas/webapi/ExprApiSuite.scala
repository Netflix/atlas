/*
 * Copyright 2014-2022 Netflix, Inc.
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
import com.netflix.atlas.akka.RequestHandler
import com.netflix.atlas.akka.testkit.MUnitRouteSuite
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.MathExpr
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.model.StyleVocabulary
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.json.Json

class ExprApiSuite extends MUnitRouteSuite {

  import scala.concurrent.duration._

  private implicit val routeTestTimeout: RouteTestTimeout = RouteTestTimeout(5.second)

  val endpoint = new ExprApi

  def testGet(uri: String)(f: => Unit): Unit = {
    test(uri) {
      Get(uri) ~> routes ~> check(f)
    }
  }

  private def routes: Route = RequestHandler.standardOptions(endpoint.routes)

  testGet("/api/v1/expr") {
    assertEquals(response.status, StatusCodes.BadRequest)
  }

  testGet("/api/v1/expr?q=name,sps,:eq") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[ExprApiSuite.Output]](responseAs[String])
    assertEquals(data.size, 4)
  }

  testGet("/api/v1/expr/debug") {
    assertEquals(response.status, StatusCodes.BadRequest)
  }

  testGet("/api/v1/expr/debug?q=name,sps,:eq") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[ExprApiSuite.Output]](responseAs[String])
    assertEquals(data.size, 4)
  }

  testGet("/api/v1/expr/debug?q=name,sps,:eq,:sum,$name,:legend&vocab=style") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[ExprApiSuite.Output]](responseAs[String])
    assertEquals(data.size, 7)
  }

  testGet("/api/v1/expr/debug?q=name,sps,:eq,:sum,$name,:legend,foo,:sset,foo,:get") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[ExprApiSuite.Output]](responseAs[String])
    assertEquals(data.size, 11)
    assert(data.last.context.variables("foo") == "name,sps,:eq,:sum,$name,:legend")
  }

  testGet("/api/v1/expr/debug?q=name,sps,:eq,:sum,$name,:legend&vocab=query") {
    assertEquals(response.status, StatusCodes.BadRequest)
  }

  testGet("/api/v1/expr/debug?q=name,sps,:eq,cluster,:has&vocab=query") {
    assertEquals(response.status, StatusCodes.BadRequest)
  }

  testGet("/api/v1/expr/debug?q=name,sps,:eq,:clear&vocab=query") {
    assertEquals(response.status, StatusCodes.BadRequest)
  }

  testGet("/api/v1/expr/debug?q=name,sps,:eq,:sum,$name,:legend,foo") {
    assertEquals(response.status, StatusCodes.BadRequest)
  }

  testGet("/api/v1/expr/debug?q=name,sps,:eq,:sum,$name,:legend,foo,:clear") {
    assertEquals(response.status, StatusCodes.BadRequest)
  }

  testGet("/api/v1/expr/normalize") {
    assertEquals(response.status, StatusCodes.BadRequest)
  }

  testGet("/api/v1/expr/normalize?q=name,sps,:eq") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assertEquals(data, List("name,sps,:eq,:sum"))
  }

  testGet("/api/v1/expr/normalize?q=name,sps,:eq,:dup,2,:mul,:swap") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assertEquals(data, List("name,sps,:eq,:sum,2.0,:mul", "name,sps,:eq,:sum"))
  }

  testGet("/api/v1/expr/normalize?q=name,sps,:eq,:dup,:and") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assertEquals(data, List("name,sps,:eq,:sum"))
  }

  testGet("/api/v1/expr/normalize?q=name,sps,:eq,name,(,sps,),:in,:and") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assertEquals(data, List("name,sps,:eq,:sum"))
  }

  testGet("/api/v1/expr/normalize?q=name,sps,:eq,name,(,sps,sps,),:in,:and") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assertEquals(data, List("name,sps,:eq,:sum"))
  }

  testGet("/api/v1/expr/normalize?q=name,(,sps1,sps2,),:in,name,(,sps2,sps1,),:in,:and") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assertEquals(data, List("name,(,sps1,sps2,),:in,:sum"))
  }

  testGet(
    "/api/v1/expr/normalize?q=(,name,:swap,:eq,nf.cluster,foo,:eq,:and,:sum,),foo,:sset,cpu,foo,:fcall,disk,foo,:fcall"
  ) {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    val expected = List(
      "name,cpu,:eq,nf.cluster,foo,:eq,:and,:sum",
      "name,disk,:eq,nf.cluster,foo,:eq,:and,:sum"
    )
    assertEquals(data, expected)
  }

  testGet("/api/v1/expr/complete") {
    assertEquals(response.status, StatusCodes.BadRequest)
  }

  testGet("/api/v1/expr/complete?q=name,sps,:eq") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[ExprApiSuite.Candidate]](responseAs[String]).map(_.name)
    assert(data.nonEmpty)
    assert(!data.contains("add"))
  }

  testGet("/api/v1/expr/complete?q=name,sps,:eq,(,nf.cluster,)") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[ExprApiSuite.Candidate]](responseAs[String]).map(_.name)
    assertEquals(data, List("by", "by", "cg", "offset", "palette"))
  }

  // TODO: Right now these fail. As a future improvement suggestions should be possible within
  // a list context by ignoring everything outside of the list.
  testGet("/api/v1/expr/complete?q=name,sps,:eq,(,nf.cluster,name") {
    assertEquals(response.status, StatusCodes.BadRequest)
  }

  testGet("/api/v1/expr/complete?q=1,2") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[ExprApiSuite.Candidate]](responseAs[String]).map(_.name)
    assert(data.nonEmpty)
    assert(data.contains("add"))
  }

  testGet("/api/v1/expr/queries?q=1,2") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assert(data.isEmpty)
  }

  testGet("/api/v1/expr/queries?q=name,sps,:eq") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assertEquals(data, List("name,sps,:eq"))
  }

  testGet("/api/v1/expr/queries?q=name,sps,:eq,(,nf.cluster,),:by") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assertEquals(data, List("name,sps,:eq"))
  }

  testGet("/api/v1/expr/queries?q=name,sps,:eq,(,nf.cluster,),:by,:dup,:dup,4,:add") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assertEquals(data, List("name,sps,:eq"))
  }

  testGet("/api/v1/expr/queries?q=name,sps,:eq,(,nf.cluster,),:by,:true,:sum,name,:has,:add") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assertEquals(data, List(":true", "name,:has", "name,sps,:eq"))
  }

  testGet("/api/v1/expr/strip?q=name,sps,:eq") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assertEquals(data, List("name,sps,:eq,:sum"))
  }

  testGet("/api/v1/expr/strip?q=name,sps,:eq,foo,bar,:eq,:and&k=foo") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assertEquals(data, List("name,sps,:eq,:sum"))
  }

  testGet("/api/v1/expr/strip?q=name,sps,:eq,foo,bar,:eq,:and&k=foo") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assertEquals(data, List("name,sps,:eq,:sum"))
  }

  testGet("/api/v1/expr/strip?q=name,sps,:eq,foo,bar,:eq,:and&k=foo&k=name") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assertEquals(data, List(":true,:sum"))
  }

  testGet("/api/v1/expr/strip?q=name,sps,:eq,foo,bar,:eq,:and,:dist-avg&k=foo") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assertEquals(data, List("name,sps,:eq,:dist-avg"))
  }

  testGet("/api/v1/expr/strip?q=name,sps,:eq,foo,bar,:eq,:not,:and,:dist-avg&k=foo") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assertEquals(data, List("name,sps,:eq,:dist-avg"))
  }

  testGet("/api/v1/expr/strip?q=name,sps,:eq,foo,bar,:re,:and,:dist-avg&k=foo") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assertEquals(data, List("name,sps,:eq,:dist-avg"))
  }

  testGet("/api/v1/expr/strip?q=name,sps,:eq,foo,:has,:and,:dist-avg&k=foo") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assertEquals(data, List("name,sps,:eq,:dist-avg"))
  }

  testGet("/api/v1/expr/strip?q=name,sps,:eq,foo,:has,:or,:dist-avg&k=foo") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assertEquals(data, List(":true,:dist-avg"))
  }

  testGet("/api/v1/expr/strip?q=name,sps,:eq,:stat-max,5,:gt,:filter&r=filter") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assertEquals(data, List("name,sps,:eq,:sum"))
  }

  testGet("/api/v1/expr/strip?q=name,sps,:eq,:stat-max,5,:gt,:filter&r=style") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assertEquals(data, List("name,sps,:eq,:sum,:stat-max,5.0,:const,:gt,:filter"))
  }

  testGet(
    "/api/v1/expr/strip?q=name,sps,:eq,:stat-max,5,:gt,:filter,foo,:legend&r=filter&r=style"
  ) {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assertEquals(data, List("name,sps,:eq,:sum"))
  }

  testGet("/api/v1/expr/strip?q=name,sps,:eq,max,5,:topk&r=filter") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assertEquals(data, List("name,sps,:eq,:sum"))
  }

  testGet("/api/v1/expr/strip?q=name,sps,:eq,max,:stat&r=filter") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assertEquals(data, List("name,sps,:eq,:sum"))
  }

  testGet("/api/v1/expr/strip?q=name,sps,:eq,max,:stat&r=math") {
    assertEquals(response.status, StatusCodes.BadRequest)
    val data = responseAs[String]
    assert(data.contains("IllegalArgumentException: vocabulary 'math' not supported"))
  }

  testGet("/api/v1/expr/strip?q=name,sps,:eq,:stack,foo,:legend,fff,:color&r=style") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assertEquals(data, List("name,sps,:eq,:sum"))
  }

  import Query._

  private def normalize(expr: String): List[String] = {
    val interpreter = Interpreter(StyleVocabulary.allWords)
    ExprApi.normalize(expr, interpreter)
  }

  test("normalize query order") {
    val q1 = "app,foo,:eq,name,cpu,:eq,:and"
    val q2 = "name,cpu,:eq,app,foo,:eq,:and"
    assertEquals(normalize(q1), normalize(q2))
  }

  test("normalize single query") {
    val e1 = DataExpr.Sum(And(Equal("app", "foo"), Equal("name", "cpuUser")))
    val add = StyleExpr(MathExpr.Add(e1, e1), Map.empty)
    assertEquals(
      normalize(add.toString),
      List(
        "app,foo,:eq,name,cpuUser,:eq,:and,:sum,app,foo,:eq,name,cpuUser,:eq,:and,:sum,:add"
      )
    )
  }

  test("normalize common query") {
    // No longer performed, UI team prefers not having the cq in the
    // normalized query
    val e1 = DataExpr.Sum(And(Equal("app", "foo"), Equal("name", "cpuUser")))
    val e2 = DataExpr.Sum(And(Equal("app", "foo"), Equal("name", "cpuSystem")))
    val add = StyleExpr(MathExpr.Add(e1, e2), Map.empty)
    assertEquals(
      normalize(add.toString),
      List(
        "app,foo,:eq,name,cpuUser,:eq,:and,:sum,app,foo,:eq,name,cpuSystem,:eq,:and,:sum,:add"
      )
    )
  }

  test("normalize :avg") {
    val avg = "app,foo,:eq,name,cpuUser,:eq,:and,:avg"
    assertEquals(normalize(avg), List(avg))
  }

  test("normalize :dist-avg") {
    val avg = "app,foo,:eq,name,cpuUser,:eq,:and,:dist-avg"
    assertEquals(normalize(avg), List(avg))
  }

  test("normalize :dist-avg,(,nf.cluster,),:by") {
    val avg = "app,foo,:eq,name,cpuUser,:eq,:and,:dist-avg,(,nf.cluster,),:by"
    assertEquals(normalize(avg), List(avg))
  }

  test("normalize :dist-stddev") {
    val stddev = "app,foo,:eq,name,cpuUser,:eq,:and,:dist-stddev"
    assertEquals(normalize(stddev), List(stddev))
  }

  test("normalize :dist-max") {
    val max = "app,foo,:eq,name,cpuUser,:eq,:and,:dist-max"
    assertEquals(normalize(max), List(max))
  }

  test("normalize :dist-avg + expr2") {
    val avg =
      "app,foo,:eq,name,cpuUser,:eq,:and,:dist-avg,app,foo,:eq,name,cpuSystem,:eq,:and,:max"
    assertEquals(
      normalize(avg),
      List(
        "app,foo,:eq,name,cpuUser,:eq,:and,:dist-avg",
        "app,foo,:eq,name,cpuSystem,:eq,:and,:max"
      )
    )
  }

  test("normalize :avg,(,nf.cluster,),:by,:pct") {
    val avg = "app,foo,:eq,name,cpuUser,:eq,:and,:avg,(,nf.cluster,),:by,:pct"
    assertEquals(normalize(avg), List(avg))
  }

  test("normalize :stat-aggr filters") {
    val avg = "app,foo,:eq,name,cpuUser,:eq,:and,:sum,(,nf.cluster,),:by,:stat-max,5.0,:gt,:filter"
    assertEquals(normalize(avg), List(avg))
  }

  test("normalize :stat-aggr with no condition filters") {
    val expr = "app,foo,:eq,name,cpuUser,:eq,:and,:sum,(,nf.cluster,),:by,:stat-max,:filter"
    assertEquals(normalize(expr), List(expr))
  }

  test("normalize :stat to :stat-aggr if possible") {
    val expr = "name,sps,:eq,(,nf.cluster,),:by,:dup,max,:stat,5,:gt,:filter"
    val expected = "name,sps,:eq,:sum,(,nf.cluster,),:by,:stat-max,5.0,:gt,:filter"
    assertEquals(normalize(expr), List(expected))
  }

  test("normalize :stat to :stat-aggr nested") {
    val expr =
      "name,sps,:eq,(,nf.cluster,),:by,:dup,:dup,max,:stat,:swap,avg,:stat,:sub,5,:gt,:filter"
    val expected = "name,sps,:eq,:sum,(,nf.cluster,),:by,:stat-max,:stat-avg,:sub,5.0,:gt,:filter"
    assertEquals(normalize(expr), List(expected))
  }

  test("normalize duplicate or clauses") {
    val expr = "name,a,:eq,name,b,:eq,:or,name,a,:eq,:or"
    val expected = "name,a,:eq,name,b,:eq,:or,:sum"
    assertEquals(normalize(expr), List(expected))
  }

  test("normalize, remove redundant clauses") {
    val expr = "name,a,:eq,:sum,b,:has,c,:has,:or,:cq,b,:has,c,:has,:or,:cq"
    val expected = "b,:has,name,a,:eq,:and,c,:has,name,a,:eq,:and,:or,:sum"
    assertEquals(normalize(expr), List(expected))
  }

  test("normalize simplify query") {
    val input = "app,foo,:eq,name,cpuUser,:eq,:and,:true,:and,:sum"
    val expected = "app,foo,:eq,name,cpuUser,:eq,:and,:sum"
    assertEquals(normalize(input), List(expected))
  }

  test("normalize sort query") {
    val input = "name,cpuUser,:eq,app,foo,:eq,:and,:sum"
    val expected = "app,foo,:eq,name,cpuUser,:eq,:and,:sum"
    assertEquals(normalize(input), List(expected))
  }

  test("normalize sensible OR handling") {
    val input = "name,cpuUser,:eq,app,foo,:eq,:and,name,cpuUser2,:eq,app,bar,:eq,:and,:or,:sum"
    val expected = "app,bar,:eq,name,cpuUser2,:eq,:and,app,foo,:eq,name,cpuUser,:eq,:and,:or,:sum"
    assertEquals(normalize(input), List(expected))
  }

  test("normalize :des-fast") {
    val expr = "app,foo,:eq,name,cpuUser,:eq,:and,:sum,:des-fast"
    assertEquals(normalize(expr), List(expr))
  }

  test("normalize legend vars, include parenthesis") {
    val expr = "name,cpuUser,:eq,:sum,$name,:legend"
    val expected = "name,cpuUser,:eq,:sum,$(name),:legend"
    assertEquals(normalize(expr), List(expected))
  }

  test("normalize legend vars, noop if parens already present") {
    val expr = "name,cpuUser,:eq,:sum,$(name),:legend"
    assertEquals(normalize(expr), List(expr))
  }

  test("normalize legend vars, mix") {
    val expr = "name,cpuUser,:eq,:sum,foo$name$abc bar$(def)baz,:legend"
    val expected = "name,cpuUser,:eq,:sum,foo$(name)$(abc) bar$(def)baz,:legend"
    assertEquals(normalize(expr), List(expected))
  }
}

object ExprApiSuite {

  case class Output(program: List[String], context: Context)

  case class Context(stack: List[String], variables: Map[String, String])

  case class Candidate(name: String, signature: String, description: String)
}
