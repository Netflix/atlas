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
import org.apache.pekko.http.scaladsl.testkit.RouteTestTimeout
import com.netflix.atlas.core.db.Database
import com.netflix.atlas.core.db.StaticDatabase
import com.netflix.atlas.core.index.TagIndex
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.EvalContext
import com.netflix.atlas.core.model.TaggedItem
import com.netflix.atlas.core.model.TimeSeries
import com.netflix.atlas.pekko.RequestHandler
import com.netflix.atlas.pekko.testkit.MUnitRouteSuite

class TagsApiSuite extends MUnitRouteSuite {

  import scala.concurrent.duration.*

  private implicit val routeTestTimeout: RouteTestTimeout = RouteTestTimeout(5.second)

  private val staticDb = StaticDatabase.range(0, 11)

  private var exception: Exception = _

  private val db = new Database {

    override def index: TagIndex[? <: TaggedItem] = {
      if (exception != null) throw exception
      staticDb.index
    }

    override def execute(eval: EvalContext, expr: DataExpr): List[TimeSeries] = {
      if (exception != null) throw exception
      staticDb.execute(eval, expr)
    }
  }

  system.actorOf(Props(new LocalDatabaseActor(db)), "db")

  val endpoint = new TagsApi

  override def beforeEach(context: BeforeEach): Unit = {
    exception = null
  }

  def testGet(uri: String)(f: => Unit): Unit = {
    test(uri) {
      Get(uri) ~> RequestHandler.standardOptions(endpoint.routes) ~> check(f)
    }
  }

  testGet("/api/v1/tags") {
    assertEquals(responseAs[String], """["class","name","prime"]""")
  }

  testGet("/api/v1/tags/") {
    assertEquals(responseAs[String], """["class","name","prime"]""")
  }

  testGet("/api/v1/tags?format=txt") {
    assertEquals(responseAs[String], "class\nname\nprime")
  }

  testGet("/api/v1/tags?limit=1") {
    assert(response.headers.exists(h => h.is(TagsApi.offsetHeader) && h.value == "class"))
    assertEquals(responseAs[String], """["class"]""")
  }

  testGet("/api/v1/tags?limit=1&offset=class") {
    assert(response.headers.exists(h => h.is(TagsApi.offsetHeader) && h.value == "name"))
    assertEquals(responseAs[String], """["name"]""")
  }

  testGet("/api/v1/tags?limit=1&offset=name") {
    assert(response.headers.exists(h => h.is(TagsApi.offsetHeader) && h.value == "prime"))
    assertEquals(responseAs[String], """["prime"]""")
  }

  testGet("/api/v1/tags?limit=1&offset=prime") {
    assert(response.headers.forall(_.isNot(TagsApi.offsetHeader)))
    assertEquals(responseAs[String], """[]""")
  }

  testGet("/api/v1/tags?limit=4&offset=class") {
    assert(response.headers.forall(_.isNot(TagsApi.offsetHeader)))
    assertEquals(responseAs[String], """["name","prime"]""")
  }

  testGet("/api/v1/tags?limit=foo") {
    assertEquals(response.status, StatusCodes.BadRequest)
  }

  testGet("/api/v1/tags?verbose=1") {
    assertEquals(responseAs[String], "[]")
  }

  testGet("/api/v1/tags/name") {
    val expected = (0 to 11).map(i => f"$i%02d").mkString("[\"", "\",\"", "\"]")
    assertEquals(responseAs[String], expected)
  }

  testGet("/api/v1/tags/name?q=name,01,:eq") {
    assertEquals(responseAs[String], """["01"]""")
  }

  testGet("/api/v1/tags/name?q=name,01,:eq,:list,(,class,odd,:eq,:cq,),:each") {
    assertEquals(responseAs[String], """["01"]""")
  }

  testGet("/api/v1/tags/name?verbose=1") {
    val expected = (0 to 11).map(toTagJson).mkString("[", ",", "]")
    assertEquals(responseAs[String], expected)
  }

  testGet("/api/v1/tags/name?verbose=1&format=txt") {
    val expected = (0 to 11).map(toTagText).mkString("\n")
    assertEquals(responseAs[String], expected)
  }

  test("failure in db actor gets sent back") {
    exception = new RuntimeException("broken")
    Get("/api/v1/tags") ~> RequestHandler.standardOptions(endpoint.routes) ~> check {
      assertEquals(response.status, StatusCodes.InternalServerError)
      assertEquals(responseAs[String], """{"type":"error","message":"RuntimeException: broken"}""")
    }
  }

  private def toTagJson(v: Int): String = toTagJson("name", "%02d".format(v), -1)

  private def toTagJson(k: String, v: String, c: Int): String = {
    s"""{"key":"$k","value":"$v","count":$c}"""
  }

  private def toTagText(v: Int): String = f"name\t$v%02d\t-1"

}
