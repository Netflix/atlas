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
package com.netflix.atlas.lwcapi

import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.headers.RawHeader
import org.apache.pekko.http.scaladsl.testkit.RouteTestTimeout
import org.apache.pekko.stream.scaladsl.Keep
import org.apache.pekko.stream.scaladsl.Sink
import com.netflix.atlas.core.util.Streams
import com.netflix.atlas.eval.model.ExprType
import com.netflix.atlas.json.JsonSupport
import com.netflix.atlas.pekko.StreamOps
import com.netflix.atlas.pekko.testkit.MUnitRouteSuite
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.ManualClock
import com.typesafe.config.ConfigFactory

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.util.concurrent.ArrayBlockingQueue
import java.util.zip.GZIPInputStream
import scala.util.Using

class ExpressionApiSuite extends MUnitRouteSuite {

  import scala.concurrent.duration.*

  private implicit val routeTestTimeout: RouteTestTimeout = RouteTestTimeout(5.second)

  private val splitter = new ExpressionSplitter(ConfigFactory.load())
  private val clock = new ManualClock()
  private val registry = new DefaultRegistry(clock)

  // Dummy queue used for handler
  private val queue = {
    val blockingQueue = new ArrayBlockingQueue[Seq[JsonSupport]](1)
    new QueueHandler(
      StreamMetadata("test"),
      StreamOps
        .wrapBlockingQueue[Seq[JsonSupport]](
          registry,
          "test",
          blockingQueue,
          dropNew = false
        )
        .toMat(Sink.ignore)(Keep.left)
        .run()
    )
  }

  private val sm = new StreamSubscriptionManager(registry)
  private val endpoint = ExpressionApi(sm, registry, ConfigFactory.load())

  private def unzip(bytes: Array[Byte]): String = {
    Using.resource(new GZIPInputStream(new ByteArrayInputStream(bytes))) { in =>
      new String(Streams.byteArray(in), StandardCharsets.UTF_8)
    }
  }

  override def beforeEach(context: BeforeEach): Unit = {
    clock.setWallTime(0L)
    sm.clear()
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
    val splits = splitter.split("nf.cluster,skan,:eq,:avg", ExprType.TIME_SERIES, 60000)
    sm.register(StreamMetadata("a"), queue)
    splits.foreach { s =>
      sm.subscribe("a", s)
    }
    sm.updateQueryIndex()
    Get("/lwc/api/v1/expressions/skan") ~> endpoint.routes ~> check {
      val expected = s"""{"expressions":[$skanCount,$skanSum]}"""
      assertEquals(unzip(responseAs[Array[Byte]]), expected)
    }
  }

  test("fetch all with data") {
    val splits =
      splitter.split("nf.cluster,skan,:eq,:avg,nf.app,brh,:eq,:max", ExprType.TIME_SERIES, 60000)
    sm.register(StreamMetadata("a"), queue)
    splits.foreach { s =>
      sm.subscribe("a", s)
    }
    sm.updateQueryIndex()
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
    val unordered = List(
      ExpressionMetadata("a", ExprType.TIME_SERIES, 2),
      ExpressionMetadata("z", ExprType.TIME_SERIES, 1),
      ExpressionMetadata("c", ExprType.TIME_SERIES, 3)
    )
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
    val e1 = List(ExpressionMetadata("a", ExprType.TIME_SERIES, 2))
    val e2 = List(ExpressionMetadata("b", ExprType.TIME_SERIES, 2))
    val tag_e1 = ExpressionApi.encode(e1).etag
    val tag_e2 = ExpressionApi.encode(e2).etag
    assert(tag_e1 != tag_e2)
  }

  test("etags don't match for empty and non-empty lists") {
    val e1 = List()
    val e2 = List(ExpressionMetadata("b", ExprType.TIME_SERIES, 2))
    val tag_e1 = ExpressionApi.encode(e1).etag
    val tag_e2 = ExpressionApi.encode(e2).etag
    assert(tag_e1 != tag_e2)
  }

  test("cache refresh") {
    val splits =
      splitter.split("nf.cluster,skan,:eq,:avg,nf.app,brh,:eq,:max", ExprType.TIME_SERIES, 60000)
    sm.register(StreamMetadata("a"), queue)
    splits.foreach { s =>
      sm.subscribe("a", s)
    }
    sm.updateQueryIndex()

    val fiveMinutes = 300_000L
    val cache = new ExpressionApi.ExpressionsCache(sm, registry, fiveMinutes, _ => true)
    assertEquals(cache.size, 0)
    cache.refresh()
    assertEquals(cache.size, 0) // No accesses, so it will still be empty

    // Ensure we get the same item back
    val a1 = cache.get(None)
    val a2 = cache.get(None)
    assert(a1 eq a2)

    // Update clock time to allow for expiration. The skan key is accessed after the
    // clock update, so it should expire during refresh. The all key loaded above should
    // expire and get reloaded.
    clock.setWallTime(fiveMinutes + 1)
    val s1 = cache.get(Some("skan"))
    assertEquals(s1.size, 2)
    assertEquals(cache.size, 2)
    cache.refresh()
    val s2 = cache.get(Some("skan"))
    assert(s1 eq s2)
    val a3 = cache.get(None)
    assert(a1 ne a3)
  }

  test("cache refresh, cluster filter") {
    val splits =
      splitter.split("nf.cluster,skan,:eq,:avg,nf.app,brh,:eq,:sum", ExprType.TIME_SERIES, 60000)
    sm.register(StreamMetadata("a"), queue)
    splits.foreach { s =>
      sm.subscribe("a", s)
    }
    sm.updateQueryIndex()

    val fiveMinutes = 300_000L
    val cache = new ExpressionApi.ExpressionsCache(
      sm,
      registry,
      fiveMinutes,
      _.metadata.expression.contains(":sum")
    )
    cache.refresh()

    assertEquals(cache.get(None).size, 3)
    assertEquals(cache.get(Some("skan")).size, 1)
    assertEquals(cache.get(Some("brh")).size, 1)
  }

  private val skanCount =
    """{"expression":"nf.cluster,skan,:eq,:count","exprType":"TIME_SERIES","frequency":60000,"id":"039722fefa66c2cdd0147595fb9b9a50351f90f0"}"""

  private val skanSum =
    """{"expression":"nf.cluster,skan,:eq,:sum","exprType":"TIME_SERIES","frequency":60000,"id":"17e0dc5b1224c825c81cf033c46d0f0490c1ca7f"}"""

  private val brhMax =
    """{"expression":"nf.app,brh,:eq,:max","exprType":"TIME_SERIES","frequency":60000,"id":"b19b2aa2c802c29216d4aa36024f71a3c92f84db"}"""
}
