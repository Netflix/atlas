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
package com.netflix.atlas.eval.model

import com.fasterxml.jackson.databind.JsonNode
import org.apache.pekko.util.ByteString
import com.netflix.atlas.core.util.Streams
import com.netflix.atlas.json.Json
import com.netflix.atlas.pekko.DiagnosticMessage
import munit.FunSuite

import java.util.Random
import java.util.UUID
import scala.util.Using

class LwcMessagesSuite extends FunSuite {

  private val step = 60000

  test("data expr, decode with legacy frequency field") {
    val json = """[{"id":"1234","expression":"name,cpu,:eq,:sum","frequency":10}]"""
    val parser = Json.newJsonParser(json)
    try {
      val actual = LwcMessages.parseDataExprs(parser).head
      val expected = LwcDataExpr("1234", "name,cpu,:eq,:sum", 10)
      assertEquals(actual, expected)
    } finally {
      parser.close()
    }
  }

  test("subscription info") {
    val expr = "name,cpu,:eq,:avg"
    val sum = "name,cpu,:eq,:sum"
    val count = "name,cpu,:eq,:count"
    val dataExprs = List(LwcDataExpr("a", sum, step), LwcDataExpr("b", count, step))
    val expected = LwcSubscription(expr, dataExprs)
    val actual = LwcMessages.parse(Json.encode(expected))
    assertEquals(actual, expected)
  }

  test("subscription-v2 time series") {
    val expr = "name,cpu,:eq,:avg"
    val sum = "name,cpu,:eq,:sum"
    val count = "name,cpu,:eq,:count"
    val dataExprs = List(LwcDataExpr("a", sum, step), LwcDataExpr("b", count, step))
    val expected = LwcSubscriptionV2(expr, ExprType.TIME_SERIES, dataExprs)
    val actual = LwcMessages.parse(Json.encode(expected))
    assertEquals(actual, expected)
  }

  test("subscription-v2 events") {
    val raw = "name,cpu,:eq"
    val table = "name,cpu,:eq,(,name,value,),:table"
    val expr = s"$raw,$table"
    val dataExprs = List(LwcDataExpr("a", raw, 0L), LwcDataExpr("b", table, 0L))
    val expected = LwcSubscriptionV2(expr, ExprType.EVENTS, dataExprs)
    val actual = LwcMessages.parse(Json.encode(expected))
    assertEquals(actual, expected)
  }

  test("subscription-v2 trace events") {
    val q1 = "app,www,:eq,app,db,:eq,:child"
    val q2 = "app,www,:eq,app,foo,:eq,:span-and"
    val expr = s"$q1,$q2"
    val dataExprs = List(LwcDataExpr("a", q1, 0L), LwcDataExpr("b", q2, 0L))
    val expected = LwcSubscriptionV2(expr, ExprType.TRACE_EVENTS, dataExprs)
    val actual = LwcMessages.parse(Json.encode(expected))
    assertEquals(actual, expected)
  }

  test("subscription-v2 trace time series") {
    val q1 = "app,www,:eq,app,db,:eq,:child"
    val q2 = "app,www,:eq,app,foo,:eq,:span-and"
    val expr = s"$q1,$q2"
    val dataExprs = List(LwcDataExpr("a", q1, 0L), LwcDataExpr("b", q2, 0L))
    val expected = LwcSubscriptionV2(expr, ExprType.TRACE_TIME_SERIES, dataExprs)
    val actual = LwcMessages.parse(Json.encode(expected))
    assertEquals(actual, expected)
  }

  test("datapoint") {
    val expected = LwcDatapoint(step, "a", Map("foo" -> "bar"), 42.0)
    val actual = LwcMessages.parse(Json.encode(expected))
    assertEquals(actual, expected)
  }

  test("datapoint, custom encode") {
    val expected = LwcDatapoint(step, "a", Map("foo" -> "bar"), 42.0)
    val actual = LwcMessages.parse(expected.toJson)
    assertEquals(actual, expected)
  }

  private def checkSamples(samples: List[List[Any]]): Unit = {
    val tags = Map("foo" -> "bar")
    val input = LwcDatapoint(step, "a", tags, 42.0, samples)
    val actual = LwcMessages.parse(Json.encode(input))
    val expected = input.copy(samples = Json.decode[List[List[JsonNode]]](Json.encode(samples)))
    assertEquals(actual, expected)

    val encoded = LwcMessages.encodeBatch(List(input))
    val decoded = LwcMessages.parseBatch(encoded)
    assertEquals(decoded.size, 1)
    assertEquals(decoded.head, expected)
  }

  test("datapoint, with samples empty") {
    checkSamples(Nil)
  }

  test("datapoint, with samples empty rows") {
    checkSamples(List(Nil, Nil, Nil))
  }

  test("datapoint, with samples") {
    val tags = Map("foo" -> "bar")
    checkSamples(List(List("a", tags)))
  }

  test("datapoint, with samples uneven") {
    val tags = Map("foo" -> "bar")
    checkSamples(List(List("a", tags), Nil, List("b", tags)))
  }

  test("event") {
    val payload = Json.decode[JsonNode]("""{"foo":"bar"}""")
    val expected = LwcEvent("123", payload)
    val actual = LwcMessages.parse(Json.encode(expected))
    assertEquals(actual, expected)
  }

  test("diagnostic message") {
    val expected = DiagnosticMessage.error("something bad happened")
    val actual = LwcMessages.parse(Json.encode(expected))
    assertEquals(actual, expected)
  }

  test("diagnostic message for a particular expression") {
    val expected = LwcDiagnosticMessage("abc", DiagnosticMessage.error("something bad happened"))
    val actual = LwcMessages.parse(Json.encode(expected))
    assertEquals(actual, expected)
  }

  test("heartbeat") {
    val expected = LwcHeartbeat(1234567890L, 10L)
    val actual = LwcMessages.parse(Json.encode(expected))
    assertEquals(actual, expected)
  }

  test("heartbeat not on step boundary") {
    intercept[IllegalArgumentException] {
      LwcHeartbeat(1234567891L, 10L)
    }
  }

  test("batch: expression") {
    val expected = (0 until 10).map { i =>
      LwcExpression("name,cpu,:eq,:max", ExprType.TIME_SERIES, i)
    }
    val actual = LwcMessages.parseBatch(LwcMessages.encodeBatch(expected))
    assertEquals(actual, expected.toList)
  }

  test("batch: events expression") {
    val expected = (0 until 10).map { i =>
      LwcExpression("name,cpu,:eq", ExprType.EVENTS, i)
    }
    val actual = LwcMessages.parseBatch(LwcMessages.encodeBatch(expected))
    assertEquals(actual, expected.toList)
  }

  test("batch: subscription") {
    val expected = (0 until 10).map { i =>
      LwcSubscription(
        "name,cpu,:eq,:avg",
        List(
          LwcDataExpr(s"$i", "name,cpu,:eq,:sum", i),
          LwcDataExpr(s"$i", "name,cpu,:eq,:count", i)
        )
      )
    }
    val actual = LwcMessages.parseBatch(LwcMessages.encodeBatch(expected))
    assertEquals(actual, expected.toList)
  }

  test("batch: subscription-v2") {
    val expected = (0 until 10).map { i =>
      LwcSubscriptionV2(
        "name,cpu,:eq,:avg",
        ExprType.TIME_SERIES,
        List(
          LwcDataExpr(s"$i", "name,cpu,:eq,:sum", i),
          LwcDataExpr(s"$i", "name,cpu,:eq,:count", i)
        )
      )
    }
    val actual = LwcMessages.parseBatch(LwcMessages.encodeBatch(expected))
    assertEquals(actual, expected.toList)
  }

  test("batch: datapoint") {
    val expected = (0 until 10).map { i =>
      LwcDatapoint(
        System.currentTimeMillis(),
        s"$i",
        if (i % 2 == 0) Map.empty else Map("name" -> "cpu", "node" -> s"i-$i"),
        i
      )
    }
    val actual = LwcMessages.parseBatch(LwcMessages.encodeBatch(expected))
    assertEquals(actual, expected.toList)
  }

  test("batch: event") {
    val expected = (0 until 10).map { i =>
      val tags = Map("name" -> "cpu", "node" -> s"i-$i")
      val payload = Json.decode[JsonNode](Json.encode(tags))
      LwcEvent(s"$i", payload)
    }
    val actual = LwcMessages.parseBatch(LwcMessages.encodeBatch(expected))
    assertEquals(actual, expected.toList)
  }

  test("batch: lwc diagnostic") {
    val expected = (0 until 10).map { i =>
      LwcDiagnosticMessage(s"$i", DiagnosticMessage.error("foo"))
    }
    val actual = LwcMessages.parseBatch(LwcMessages.encodeBatch(expected))
    assertEquals(actual, expected.toList)
  }

  test("batch: diagnostic") {
    val expected = (0 until 10).map { i =>
      DiagnosticMessage.error(s"error $i")
    }
    val actual = LwcMessages.parseBatch(LwcMessages.encodeBatch(expected))
    assertEquals(actual, expected.toList)
  }

  test("batch: heartbeat") {
    val expected = (1 to 10).map { i =>
      val step = i * 1000
      LwcHeartbeat(System.currentTimeMillis() / step * step, step)
    }
    val actual = LwcMessages.parseBatch(LwcMessages.encodeBatch(expected))
    assertEquals(actual, expected.toList)
  }

  test("batch: compatibility") {
    // Other tests generate new payloads, but this could mean we do a change that breaks
    // compatibility with existing versions. To check for that this test loads a file
    // that has been pre-encoded.
    val expected = List(
      LwcExpression("name,cpu,:eq,:max", ExprType.TIME_SERIES, 60_000),
      LwcSubscription(
        "name,cpu,:eq,:avg",
        List(
          LwcDataExpr("0", "name,cpu,:eq,:sum", 10_000),
          LwcDataExpr("1", "name,cpu,:eq,:count", 10_000)
        )
      ),
      LwcDatapoint(
        1234567890,
        "id",
        Map.empty,
        1.0
      ),
      LwcDatapoint(
        1234567890,
        "id",
        Map("name" -> "cpu", "node" -> s"i-12345"),
        2.0
      ),
      LwcDiagnosticMessage("2", DiagnosticMessage.error("foo")),
      DiagnosticMessage.info("bar"),
      LwcHeartbeat(1234567890, 10)
    )
    val actual = Using.resource(Streams.resource("lwc-batch.smile")) { in =>
      LwcMessages.parseBatch(ByteString(Streams.byteArray(in)))
    }
    assertEquals(actual, expected)
  }

  test("batch: random") {
    val random = new Random()
    (0 until 100).foreach { _ =>
      val n = random.nextInt(1000)
      val expected = (0 until n).map(_ => randomObject(random)).toList
      val actual = LwcMessages.parseBatch(LwcMessages.encodeBatch(expected))
      assertEquals(actual, expected)
    }
  }

  private def randomObject(random: Random): AnyRef = {
    random.nextInt(6) match {
      case 0 =>
        LwcExpression(randomString, ExprType.TIME_SERIES, randomStep(random))
      case 1 =>
        val n = random.nextInt(10) + 1
        LwcSubscription(
          randomString,
          (0 until n).map(_ => LwcDataExpr(randomString, randomString, randomStep(random))).toList
        )
      case 2 =>
        // Use positive infinity to test special double values. Do not use NaN here
        // because NaN != NaN so it will break the assertions for tests.
        LwcDatapoint(
          random.nextLong(),
          randomString,
          randomTags(random),
          if (random.nextDouble() < 0.1) Double.PositiveInfinity else random.nextDouble()
        )
      case 3 =>
        val msg = DiagnosticMessage(randomString, randomString, None)
        LwcDiagnosticMessage(randomString, msg)
      case 4 =>
        DiagnosticMessage(randomString, randomString, None)
      case _ =>
        val step = randomStep(random)
        val timestamp = System.currentTimeMillis() / step * step
        LwcHeartbeat(timestamp, step)
    }
  }

  private def randomTags(random: Random): Map[String, String] = {
    val n = random.nextInt(10)
    (0 until n).map(_ => randomString -> randomString).toMap
  }

  private def randomStep(random: Random): Long = {
    random.nextInt(10) * 1000 + 1000
  }

  private def randomString: String = UUID.randomUUID().toString
}
