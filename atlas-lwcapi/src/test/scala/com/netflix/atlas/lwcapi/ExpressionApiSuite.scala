/*
 * Copyright 2014-2021 Netflix, Inc.
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

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.testkit.RouteTestTimeout
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import com.netflix.atlas.akka.StreamOps
import com.netflix.atlas.akka.testkit.MUnitRouteSuite
import com.netflix.atlas.core.util.Streams
import com.netflix.atlas.json.JsonSupport
import com.netflix.spectator.api.NoopRegistry
import com.typesafe.config.ConfigFactory

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.util.zip.GZIPInputStream
import scala.util.Using

class ExpressionApiSuite extends MUnitRouteSuite {
  import scala.concurrent.duration._

  private implicit val routeTestTimeout = RouteTestTimeout(5.second)

  private val splitter = new ExpressionSplitter(ConfigFactory.load())

  // Dummy queue used for handler
  private val queue = new QueueHandler(
    "test",
    StreamOps
      .blockingQueue[Seq[JsonSupport]](new NoopRegistry, "test", 1)
      .toMat(Sink.ignore)(Keep.left)
      .run()
  )

  private val sm = new StreamSubscriptionManager
  private val endpoint = ExpressionApi(sm, new NoopRegistry, system)

  private def unzip(bytes: Array[Byte]): String = {
    Using.resource(new GZIPInputStream(new ByteArrayInputStream(bytes))) { in =>
      new String(Streams.byteArray(in), StandardCharsets.UTF_8)
    }
  }

  test("get of a path returns empty data") {
    val expected_etag = ExpressionApi.encode(List()).etag
    Get("/lwc/api/v1/expressions/123") ~> endpoint.routes ~> check {
      assertEquals(response.status, StatusCodes.OK)
      assertEquals(unzip(responseAs[Array[Byte]]), """{"expressions":[]}""")
      assert(header("ETag").isDefined)
      assertEquals(header("ETag").get.value, expected_etag)
    }
  }

  test("get with empty-content etag returns NotModified") {
    val etag = ExpressionApi.encode(List()).etag
    val headers = List(RawHeader("If-None-Match", etag))
    Get("/lwc/api/v1/expressions/123").withHeaders(headers) ~> endpoint.routes ~> check {
      assertEquals(response.status, StatusCodes.NotModified)
      assert(responseAs[String].isEmpty)
      assert(header("ETag").isDefined)
      assertEquals(header("ETag").get.value, etag)
    }
  }

  test("get with non-matching etag returns OK and content") {
    val etag = """"never-gonna-match""""
    val emptyETag = ExpressionApi.encode(List()).etag
    val headers = List(RawHeader("If-None-Match", etag))
    Get("/lwc/api/v1/expressions/123").withHeaders(headers) ~> endpoint.routes ~> check {
      assertEquals(response.status, StatusCodes.OK)
      assertEquals(unzip(responseAs[Array[Byte]]), """{"expressions":[]}""")
      assert(header("ETag").isDefined)
      assertEquals(header("ETag").get.value, emptyETag)
    }
  }

  test("has data") {
    val splits = splitter.split("nf.cluster,skan,:eq,:avg", 60000)
    sm.register("a", queue)
    splits.foreach { s =>
      sm.subscribe("a", s)
    }
    sm.regenerateQueryIndex()
    Get("/lwc/api/v1/expressions/skan") ~> endpoint.routes ~> check {
      val expected = s"""{"expressions":[$skanCount,$skanSum]}"""
      assertEquals(unzip(responseAs[Array[Byte]]), expected)
    }
  }

  test("fetch all with data") {
    val splits = splitter.split("nf.cluster,skan,:eq,:avg,nf.app,brh,:eq,:max", 60000)
    sm.register("a", queue)
    splits.foreach { s =>
      sm.subscribe("a", s)
    }
    sm.regenerateQueryIndex()
    Get("/lwc/api/v1/expressions") ~> endpoint.routes ~> check {
      val expected = s"""{"expressions":[$brhMax,$skanCount,$skanSum]}"""
      assertEquals(unzip(responseAs[Array[Byte]]), expected)
    }
  }

  test("fetch all with empty result set") {
    sm.clear()
    endpoint.clearCache()
    Get("/lwc/api/v1/expressions") ~> endpoint.routes ~> check {
      val expected = """{"expressions":[]}"""
      assertEquals(unzip(responseAs[Array[Byte]]), expected)
    }
  }

  test("fetch all with trailing slash") {
    sm.clear()
    endpoint.clearCache()
    Get("/lwc/api/v1/expressions/") ~> endpoint.routes ~> check {
      val expected = """{"expressions":[]}"""
      assertEquals(unzip(responseAs[Array[Byte]]), expected)
    }
  }

  test("etags match for different orderings") {
    val unordered =
      List(ExpressionMetadata("a", 2), ExpressionMetadata("z", 1), ExpressionMetadata("c", 3))
    val ordered = unordered.sorted
    val tagUnordered = ExpressionApi.encode(unordered).etag
    val tagOrdered = ExpressionApi.encode(ordered).etag
    assertEquals(tagUnordered, tagOrdered)
  }

  test("etags for no expressions works") {
    val empty = List()
    val tag = ExpressionApi.encode(empty).etag
    assert(tag.nonEmpty)
  }

  test("etags don't match for different content") {
    val e1 = List(ExpressionMetadata("a", 2))
    val e2 = List(ExpressionMetadata("b", 2))
    val tag_e1 = ExpressionApi.encode(e1).etag
    val tag_e2 = ExpressionApi.encode(e2).etag
    assert(tag_e1 != tag_e2)
  }

  test("etags don't match for empty and non-empty lists") {
    val e1 = List()
    val e2 = List(ExpressionMetadata("b", 2))
    val tag_e1 = ExpressionApi.encode(e1).etag
    val tag_e2 = ExpressionApi.encode(e2).etag
    assert(tag_e1 != tag_e2)
  }

  private val skanCount =
    """{"expression":"nf.cluster,skan,:eq,:count","frequency":60000,"id":"6278fa6047c07316d7e265a1004882ab9e1007af"}"""

  private val skanSum =
    """{"expression":"nf.cluster,skan,:eq,:sum","frequency":60000,"id":"36e0a2c61b48e062bba5361d059afd313c82c674"}"""

  private val brhMax =
    """{"expression":"nf.app,brh,:eq,:max","frequency":60000,"id":"16f1b0930c0eeae0225374ea88c01e161e589aff"}"""
}
