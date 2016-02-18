/*
 * Copyright 2014-2016 Netflix, Inc.
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

import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.json.Json
import org.scalatest.FunSuite
import spray.http.StatusCodes
import spray.routing.RequestContext
import spray.routing.Route
import spray.routing.directives.ExecutionDirectives
import spray.testkit.ScalatestRouteTest


class ExprApiSuite extends FunSuite with ScalatestRouteTest {

  import scala.concurrent.duration._

  implicit val routeTestTimeout = RouteTestTimeout(5.second)

  val endpoint = new ExprApi

  def testGet(uri: String)(f: => Unit): Unit = {
    test(uri) {
      Get(uri) ~> routes ~> check(f)
    }
  }

  private def routes: RequestContext => Unit = {
    ExecutionDirectives.handleExceptions(exceptionHandler) { endpoint.routes }
  }

  private def exceptionHandler: PartialFunction[Throwable, Route] = {
    case t: Throwable => { ctx => DiagnosticMessage.handleException(ctx.responder)(t) }
  }

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
    assert(data === List("name,sps,:eq"))
  }

  testGet("/api/v1/expr/normalize?q=name,sps,:eq,:dup,2,:mul,:swap") {
    assert(response.status === StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    assert(data === List("name,sps,:eq,:sum,2.0,:const,:mul", "name,sps,:eq"))
  }

  testGet("/api/v1/expr/normalize?q=(,name,:swap,:eq,nf.cluster,foo,:eq,:and,:sum,),foo,:sset,cpu,foo,:fcall,disk,foo,:fcall") {
    assert(response.status === StatusCodes.OK)
    val data = Json.decode[List[String]](responseAs[String])
    val expected = List(
      "name,cpu,:eq,nf.cluster,foo,:eq,:and,:sum",
      "name,disk,:eq,nf.cluster,foo,:eq,:and,:sum")
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
}

object ExprApiSuite {
  case class Output(program: List[String], context: Context)
  case class Context(stack: List[String], variables: Map[String, String])
  case class Candidate(name: String, signature: String, description: String)
}
