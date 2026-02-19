/*
 * Copyright 2014-2026 Netflix, Inc.
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

import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.testkit.RouteTestTimeout
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.MathExpr
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.model.StyleVocabulary
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.json3.Json
import com.netflix.atlas.pekko.RequestHandler
import com.netflix.atlas.pekko.testkit.MUnitRouteSuite

class ExprApiSuite extends MUnitRouteSuite {

  import scala.concurrent.duration.*

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

  testGet("/api/v1/expr/normalize?q=name,latency,:eq,0,5,:sample-count") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assertEquals(data, List("name,latency,:eq,0.0,5.0,:sample-count"))
  }

  testGet("/api/v1/expr/normalize?q=name,latency,:eq,0,5,:sample-count,(,app,),:by") {
    assertEquals(response.status, StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assertEquals(data, List("name,latency,:eq,0.0,5.0,:sample-count,(,app,),:by"))
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
    assertEquals(data, List("by", "by", "pi", "cg", "offset", "palette"))
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

  import Query.*

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
        "name,cpuUser,:eq,app,foo,:eq,:and,:sum,name,cpuUser,:eq,app,foo,:eq,:and,:sum,:add"
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
        "name,cpuUser,:eq,app,foo,:eq,:and,:sum,name,cpuSystem,:eq,app,foo,:eq,:and,:sum,:add"
      )
    )
  }

  test("normalize :avg") {
    val avg = "name,cpuUser,:eq,app,foo,:eq,:and,:avg"
    assertEquals(normalize(avg), List(avg))
  }

  test("normalize :dist-avg") {
    val avg = "name,cpuUser,:eq,app,foo,:eq,:and,:dist-avg"
    assertEquals(normalize(avg), List(avg))
  }

  test("normalize :dist-avg,(,nf.cluster,),:by") {
    val avg = "name,cpuUser,:eq,app,foo,:eq,:and,:dist-avg,(,nf.cluster,),:by"
    assertEquals(normalize(avg), List(avg))
  }

  test("normalize :dist-stddev") {
    val stddev = "name,cpuUser,:eq,app,foo,:eq,:and,:dist-stddev"
    assertEquals(normalize(stddev), List(stddev))
  }

  test("normalize :dist-max") {
    val max = "name,cpuUser,:eq,app,foo,:eq,:and,:dist-max"
    assertEquals(normalize(max), List(max))
  }

  test("normalize :dist-avg + expr2") {
    val avg =
      "name,cpuUser,:eq,app,foo,:eq,:and,:dist-avg,name,cpuSystem,:eq,app,foo,:eq,:and,:max"
    assertEquals(
      normalize(avg),
      List(
        "name,cpuUser,:eq,app,foo,:eq,:and,:dist-avg",
        "name,cpuSystem,:eq,app,foo,:eq,:and,:max"
      )
    )
  }

  test("normalize :avg,(,nf.cluster,),:by,:pct") {
    val avg = "name,cpuUser,:eq,app,foo,:eq,:and,:avg,(,nf.cluster,),:by,:pct"
    assertEquals(normalize(avg), List(avg))
  }

  test("normalize :stat-aggr filters") {
    val avg = "name,cpuUser,:eq,app,foo,:eq,:and,:sum,(,nf.cluster,),:by,:stat-max,5.0,:gt,:filter"
    assertEquals(normalize(avg), List(avg))
  }

  test("normalize :stat-aggr with no condition filters") {
    val expr = "name,cpuUser,:eq,app,foo,:eq,:and,:sum,(,nf.cluster,),:by,:stat-max,:filter"
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
    val expected = "name,a,:eq,b,:has,:and,name,a,:eq,c,:has,:and,:or,:sum"
    assertEquals(normalize(expr), List(expected))
  }

  test("normalize simplify query") {
    val input = "app,foo,:eq,name,cpuUser,:eq,:and,:true,:and,:sum"
    val expected = "name,cpuUser,:eq,app,foo,:eq,:and,:sum"
    assertEquals(normalize(input), List(expected))
  }

  test("normalize sort query") {
    val input = "name,cpuUser,:eq,app,foo,:eq,:and,:sum"
    val expected = "name,cpuUser,:eq,app,foo,:eq,:and,:sum"
    assertEquals(normalize(input), List(expected))
  }

  test("normalize sensible OR handling") {
    val input = "name,cpuUser,:eq,app,foo,:eq,:and,name,cpuUser2,:eq,app,bar,:eq,:and,:or,:sum"
    val expected = "name,cpuUser,:eq,app,foo,:eq,:and,name,cpuUser2,:eq,app,bar,:eq,:and,:or,:sum"
    assertEquals(normalize(input), List(expected))
  }

  test("normalize :des-fast") {
    val expr = "name,cpuUser,:eq,app,foo,:eq,:and,:sum,:des-fast"
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

  test("normalize :line") {
    val expr = "name,cpuUser,:eq,app,foo,:eq,:and,:sum,(,stack,),:by"
    assertEquals(normalize(expr), List(expr))
    assertEquals(normalize(s"$expr,:line"), List(expr))
  }

  test("normalize :stack") {
    val expr = "name,cpuUser,:eq,app,foo,:eq,:and,:sum,(,stack,),:by,:stack"
    assertEquals(normalize(expr), List(expr))
  }

  test("normalize :area") {
    val expr = "name,cpuUser,:eq,app,foo,:eq,:and,:sum,(,stack,),:by,:area"
    assertEquals(normalize(expr), List(expr))
  }

  test("normalize :vspan") {
    val expr = "name,cpuUser,:eq,app,foo,:eq,:and,:sum,(,stack,),:by,:vspan"
    assertEquals(normalize(expr), List(expr))
  }

  private def rewrite(expr: String): List[String] = {
    val interpreter = Interpreter(StyleVocabulary.allWords)
    ExprApi.rewrite(expr, interpreter)
  }

  test("rewrite: no offset") {
    val expr = "name,cpu,:eq,:avg"
    val rewritten = rewrite(expr).mkString(",")
    assertEquals(rewritten, "name,cpu,:eq,:avg")
  }

  test("rewrite: single zero offset removed") {
    val expr = "name,cpu,:eq,:avg,(,0s,),:offset"
    val rewritten = rewrite(expr).mkString(",")
    assertEquals(rewritten, "name,cpu,:eq,:avg")
  }

  test("rewrite: single non-zero offset") {
    val expr = "name,cpu,:eq,:avg,(,1h,),:offset"
    val rewritten = rewrite(expr).mkString(",")
    assertEquals(rewritten, "name,cpu,:eq,:avg,1h,:offset")
  }

  test("rewrite: single non-zero offset with minutes") {
    val expr = "name,cpu,:eq,:avg,(,30m,),:offset"
    val rewritten = rewrite(expr).mkString(",")
    assertEquals(rewritten, "name,cpu,:eq,:avg,30m,:offset")
  }

  test("rewrite: multiple offsets expanded") {
    val expr = "name,cpu,:eq,:avg,(,0s,1h,),:offset"
    val rewritten = rewrite(expr)
    assertEquals(rewritten.size, 1)
    assert(rewritten.head.contains("Query0"))
    assert(rewritten.head.contains(":set"))
    assert(rewritten.head.contains(":get"))
  }

  test("rewrite: multiple offsets with zero and non-zero") {
    val expr = "name,cpu,:eq,:avg,(,0s,1h,2h,),:offset"
    val rewritten = rewrite(expr)
    assertEquals(rewritten.size, 1)
    val result = rewritten.head
    // Should contain Query0 variable
    assert(result.contains("Query0"))
    // Should have :set to define the base query
    assert(result.contains(":set"))
    // Should have multiple :get references
    assert(result.split(":get").length >= 3)
    // Should handle 0s offset without explicit :offset
    assert(result.contains("Query0,:get,"))
    // Should have explicit :offset for non-zero values
    assert(result.contains(",1h,:offset"))
    assert(result.contains(",2h,:offset"))
  }

  test("rewrite: multiple expressions with offsets") {
    val expr = "name,cpu,:eq,:avg,(,0s,1h,),:offset,name,disk,:eq,:sum"
    val rewritten = rewrite(expr)
    assertEquals(rewritten.size, 2)
    // Second expression (index 0 in result) has no offset
    assertEquals(rewritten.head, "name,disk,:eq,:sum")
    // First expression (index 1 in result) has offset rewriting
    assert(rewritten(1).contains("Query1"))
  }

  test("rewrite: offset with style settings preserved") {
    val expr = "name,cpu,:eq,:avg,(,0s,1h,),:offset,foo,:legend"
    val rewritten = rewrite(expr)
    assertEquals(rewritten.size, 1)
    assert(rewritten.head.contains("foo"))
    assert(rewritten.head.contains(":legend"))
  }

  test("rewrite: complex offset durations") {
    val expr = "name,cpu,:eq,:avg,(,0s,1h,1d,1w,),:offset"
    val rewritten = rewrite(expr)
    assertEquals(rewritten.size, 1)
    val result = rewritten.head
    assert(result.contains("1h,:offset"))
    assert(result.contains("1d,:offset"))
    assert(result.contains("1w,:offset"))
  }

  // Comprehensive ordering tests
  test("ordering: prefix keys ordered by position") {
    // nf.app should come before nf.cluster (positions 1 and 3 in prefix-keys)
    val expr = "nf.cluster,foo,:eq,nf.app,bar,:eq,:and,:sum"
    val expected = "nf.app,bar,:eq,nf.cluster,foo,:eq,:and,:sum"
    assertEquals(normalize(expr), List(expected))
  }

  test("ordering: multiple prefix keys in order") {
    // Order should be: name, nf.app, nf.stack, nf.cluster (positions 0, 1, 2, 3)
    val expr = "nf.cluster,c,:eq,name,n,:eq,nf.stack,s,:eq,nf.app,a,:eq,:and,:and,:and,:sum"
    val expected = "name,n,:eq,nf.app,a,:eq,:and,nf.stack,s,:eq,:and,nf.cluster,c,:eq,:and,:sum"
    assertEquals(normalize(expr), List(expected))
  }

  test("ordering: prefix key before regular key") {
    // name is prefix key, app is regular key (not in prefix list as 'app', only 'nf.app')
    val expr = "app,foo,:eq,name,bar,:eq,:and,:sum"
    val expected = "name,bar,:eq,app,foo,:eq,:and,:sum"
    assertEquals(normalize(expr), List(expected))
  }

  test("ordering: regular keys lexically ordered") {
    // app, foo, and zoo are all regular keys, should be ordered lexically
    val expr = "zoo,z,:eq,app,a,:eq,foo,f,:eq,:and,:and,:sum"
    val expected = "app,a,:eq,foo,f,:eq,:and,zoo,z,:eq,:and,:sum"
    assertEquals(normalize(expr), List(expected))
  }

  test("ordering: suffix key after regular key") {
    // statistic is suffix key, app is regular key
    val expr = "statistic,count,:eq,app,foo,:eq,:and,:sum"
    val expected = "app,foo,:eq,statistic,count,:eq,:and,:sum"
    assertEquals(normalize(expr), List(expected))
  }

  test("ordering: suffix key after prefix key") {
    // name is prefix key, statistic is suffix key
    val expr = "statistic,count,:eq,name,foo,:eq,:and,:sum"
    val expected = "name,foo,:eq,statistic,count,:eq,:and,:sum"
    assertEquals(normalize(expr), List(expected))
  }

  test("ordering: prefix, regular, and suffix keys together") {
    // name (prefix), app (regular), statistic (suffix)
    val expr = "statistic,count,:eq,app,foo,:eq,name,bar,:eq,:and,:and,:sum"
    val expected = "name,bar,:eq,app,foo,:eq,:and,statistic,count,:eq,:and,:sum"
    assertEquals(normalize(expr), List(expected))
  }

  test("ordering: all prefix keys in reverse order") {
    // Testing all prefix keys: name, nf.app, nf.stack, nf.cluster, nf.asg, nf.region, nf.zone, nf.node
    val expr =
      "nf.node,8,:eq,nf.zone,7,:eq,nf.region,6,:eq,nf.asg,5,:eq,nf.cluster,4,:eq,nf.stack,3,:eq,nf.app,2,:eq,name,1,:eq,:and,:and,:and,:and,:and,:and,:and,:sum"
    val expected =
      "name,1,:eq,nf.app,2,:eq,:and,nf.stack,3,:eq,:and,nf.cluster,4,:eq,:and,nf.asg,5,:eq,:and,nf.region,6,:eq,:and,nf.zone,7,:eq,:and,nf.node,8,:eq,:and,:sum"
    assertEquals(normalize(expr), List(expected))
  }

  test("ordering: same key with different values uses toString") {
    // When keys are equal, should compare by toString of queries
    val expr1 = "name,aaa,:eq,:sum"
    val expr2 = "name,zzz,:eq,:sum"
    // Both should normalize to themselves and be different
    assertEquals(normalize(expr1), List("name,aaa,:eq,:sum"))
    assertEquals(normalize(expr2), List("name,zzz,:eq,:sum"))

    // When combined with OR, the toString ordering determines the order
    val exprOr = "name,zzz,:eq,name,aaa,:eq,:or,:sum"
    val expectedOr = "name,aaa,:eq,name,zzz,:eq,:or,:sum"
    assertEquals(normalize(exprOr), List(expectedOr))
  }

  test("ordering: complex mix with all key types") {
    // Multiple of each type: prefix (name, nf.app), regular (app, foo, zoo), suffix (statistic)
    val expr =
      "statistic,s,:eq,zoo,z,:eq,nf.app,na,:eq,foo,f,:eq,name,n,:eq,app,a,:eq,:and,:and,:and,:and,:and,:sum"
    val expected =
      "name,n,:eq,nf.app,na,:eq,:and,app,a,:eq,:and,foo,f,:eq,:and,zoo,z,:eq,:and,statistic,s,:eq,:and,:sum"
    assertEquals(normalize(expr), List(expected))
  }

  test("ordering: regular keys between prefix and suffix") {
    // name (prefix), bar, foo (regular, lexical), statistic (suffix)
    val expr = "statistic,s,:eq,foo,f,:eq,bar,b,:eq,name,n,:eq,:and,:and,:and,:sum"
    val expected = "name,n,:eq,bar,b,:eq,:and,foo,f,:eq,:and,statistic,s,:eq,:and,:sum"
    assertEquals(normalize(expr), List(expected))
  }

  test("ordering: prefix keys preserve position with gaps") {
    // Using non-consecutive prefix keys: name (0), nf.cluster (3), nf.zone (6)
    val expr = "nf.zone,z,:eq,nf.cluster,c,:eq,name,n,:eq,:and,:and,:sum"
    val expected = "name,n,:eq,nf.cluster,c,:eq,:and,nf.zone,z,:eq,:and,:sum"
    assertEquals(normalize(expr), List(expected))
  }
}

object ExprApiSuite {

  case class Output(program: List[String], context: Context)

  case class Context(stack: List[String], variables: Map[String, String])

  case class Candidate(name: String, signature: String, description: String)
}
