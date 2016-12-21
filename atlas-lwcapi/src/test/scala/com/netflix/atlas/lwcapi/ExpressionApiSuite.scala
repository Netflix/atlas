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
package com.netflix.atlas.lwcapi

import akka.actor.ActorSystem
import com.netflix.spectator.api.Spectator
import org.scalatest.FunSuite
import spray.http.HttpHeaders.RawHeader
import spray.http.StatusCodes
import spray.testkit.ScalatestRouteTest

class ExpressionApiSuite extends FunSuite with ScalatestRouteTest {
  import scala.concurrent.duration._

  implicit val routeTestTimeout = RouteTestTimeout(5.second)

  val splitter = new ExpressionSplitterImpl()

  val alertmap = ExpressionDatabaseImpl()
  val endpoint = ExpressionApi(alertmap, Spectator.globalRegistry(), system)

  test("get of a path returns empty data") {
    val expected_etag = ExpressionApi.compute_etag(List())
    Get("/lwc/api/v1/expressions/123") ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.OK)
      assert(responseAs[String] === """{"expressions":[]}""")
      assert(header("ETag").isDefined)
      assert(header("ETag").get.value === expected_etag)
    }
  }

  test("get with empty-content etag returns NotModified") {
    val etag = ExpressionApi.compute_etag(List())
    Get("/lwc/api/v1/expressions/123").withHeaders(List(RawHeader("ETag", etag))) ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.NotModified)
      assert(responseAs[String].isEmpty)
      assert(header("ETag").isDefined)
      assert(header("ETag").get.value === etag)
    }
  }

  test("get with non-matching etag returns OK and content") {
    val etag = """"never-gonna-match""""
    val empty_etag = ExpressionApi.compute_etag(List())
    Get("/lwc/api/v1/expressions/123").withHeaders(List(RawHeader("ETag", etag))) ~> endpoint.routes ~> check {
      assert(response.status === StatusCodes.OK)
      assert(responseAs[String] === """{"expressions":[]}""")
      assert(header("ETag").isDefined)
      assert(header("ETag").get.value === empty_etag)
    }
  }

  test("has data") {
    val split = splitter.split("nf.cluster,skan,:eq,:avg", 60000)
    split.queries.zip(split.expressions).foreach { case (query, expr) =>
        alertmap.addExpr(expr, query)
    }
    alertmap.regenerateQueryIndex()
    Get("/lwc/api/v1/expressions/skan") ~> endpoint.routes ~> check {
      val expected = """{"expressions":[{"expression":"nf.cluster,skan,:eq,:count","frequency":60000,"id":"Ynj6YEfAcxbX4mWhAEiCq54QB68"},{"expression":"nf.cluster,skan,:eq,:sum","frequency":60000,"id":"NuCixhtI4GK7pTYdBZr9MTyCxnQ"}]}"""
      assert(responseAs[String] === expected)
    }
  }

  test("etags match for different orderings") {
    val unordered = List(ExpressionWithFrequency("a", 2), ExpressionWithFrequency("z", 1), ExpressionWithFrequency("c", 3))
    val ordered = List(ExpressionWithFrequency("a", 2), ExpressionWithFrequency("c", 3), ExpressionWithFrequency("z", 1))
    val tag_unordered = ExpressionApi.compute_etag(unordered)
    val tag_ordered = ExpressionApi.compute_etag(ordered)
    assert(tag_unordered === tag_ordered)
  }

  test("etags for no expressions works") {
    val empty = List()
    val tag = ExpressionApi.compute_etag(empty)
    assert(tag.nonEmpty)
  }

  test("etags don't match for different content") {
    val e1 = List(ExpressionWithFrequency("a", 2))
    val e2 = List(ExpressionWithFrequency("b", 2))
    val tag_e1 = ExpressionApi.compute_etag(e1)
    val tag_e2 = ExpressionApi.compute_etag(e2)
    assert(tag_e1 != tag_e2)
  }

  test("etags don't match for empty and non-empty lists") {
    val e1 = List()
    val e2 = List(ExpressionWithFrequency("b", 2))
    val tag_e1 = ExpressionApi.compute_etag(e1)
    val tag_e2 = ExpressionApi.compute_etag(e2)
    assert(tag_e1 != tag_e2)
  }

  test("splitting etags: spaces and commas") {
    // space between comma and quote
    assert(ExpressionApi.split_sent_tags(Some("\"a\", \"b\"")) == List("\"a\"", "\"b\""))
  }

  test("splitting etags: missing quotes") {
    // simple list without quotes, which is an error but we'll take it
    assert(ExpressionApi.split_sent_tags(Some("a,b")) === List("a", "b"))
  }

  test("splitting etags: None") {
    assert(ExpressionApi.split_sent_tags(None) === List())
  }
}
