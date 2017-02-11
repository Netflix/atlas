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
import com.netflix.atlas.core.db.StaticDatabase
import org.scalatest.FunSuite
import spray.http.StatusCodes
import spray.testkit.ScalatestRouteTest


class TagsApiSuite extends FunSuite with ScalatestRouteTest {

  import scala.concurrent.duration._

  implicit val routeTestTimeout = RouteTestTimeout(5.second)

  val db = StaticDatabase.range(0, 11)
  system.actorOf(Props(new LocalDatabaseActor(db)), "db")

  val endpoint = new TagsApi

  def testGet(uri: String)(f: => Unit): Unit = {
    test(uri) {
      Get(uri) ~> endpoint.routes ~> check(f)
    }
  }

  testGet("/api/v1/tags") {
    assert(responseAs[String] === """["class","name","prime"]""")
  }

  testGet("/api/v1/tags/") {
    assert(responseAs[String] === """["class","name","prime"]""")
  }

  testGet("/api/v1/tags?format=txt") {
      assert(responseAs[String] === "class\nname\nprime")
  }

  testGet("/api/v1/tags?limit=1") {
    assert(response.headers.exists(h => h.is(TagsApi.offsetHeader) && h.value == "class"))
    assert(responseAs[String] === """["class"]""")
  }

  testGet("/api/v1/tags?limit=1&offset=class") {
    assert(response.headers.exists(h => h.is(TagsApi.offsetHeader) && h.value == "name"))
    assert(responseAs[String] === """["name"]""")
  }

  testGet("/api/v1/tags?limit=1&offset=name") {
    assert(response.headers.exists(h => h.is(TagsApi.offsetHeader) && h.value == "prime"))
    assert(responseAs[String] === """["prime"]""")
  }

  testGet("/api/v1/tags?limit=1&offset=prime") {
    assert(response.headers.forall(_.isNot(TagsApi.offsetHeader)))
    assert(responseAs[String] === """[]""")
  }

  testGet("/api/v1/tags?limit=4&offset=class") {
    assert(response.headers.forall(_.isNot(TagsApi.offsetHeader)))
    assert(responseAs[String] === """["name","prime"]""")
  }

  testGet("/api/v1/tags?limit=foo") {
    assert(response.status === StatusCodes.BadRequest)
  }

  testGet("/api/v1/tags?verbose=1") {
    val expected = Seq(toTagJson("class", "even", 6), toTagJson("class", "odd", 6)) ++
      (0 to 11).map(toTagJson).sortWith(_ < _) ++
      Seq(toTagJson("prime", "probably", 5))
    assert(responseAs[String] === expected.mkString("[", ",", "]"))
  }

  testGet("/api/v1/tags/name") {
    val expected = (0 to 11).map(i => f"$i%02d").mkString("[\"", "\",\"", "\"]")
    assert(responseAs[String] === expected)
  }

  testGet("/api/v1/tags/name?q=name,01,:eq") {
    assert(responseAs[String] === """["01"]""")
  }

  testGet("/api/v1/tags/name?verbose=1") {
    val expected = (0 to 11).map(toTagJson).mkString("[", ",", "]")
    assert(responseAs[String] === expected)
  }

  testGet("/api/v1/tags/name?verbose=1&format=txt") {
    val expected = (0 to 11).map(toTagText).mkString("\n")
    assert(responseAs[String] === expected)
  }

  private def toTagJson(v: Int): String = toTagJson("name", "%02d".format(v), 1)

  private def toTagJson(k: String, v: String, c: Int): String = {
    s"""{"key":"$k","value":"$v","count":$c}"""
  }

  private def toTagText(v: Int): String = f"name\t$v%02d\t1"

}
