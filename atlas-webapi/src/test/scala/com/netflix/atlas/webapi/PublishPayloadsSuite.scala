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

import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.core.model.DatapointTuple
import com.netflix.atlas.core.model.ItemIdCalculator
import com.netflix.atlas.core.util.SortedTagMap
import munit.FunSuite

import java.util.UUID
import scala.util.Random

class PublishPayloadsSuite extends FunSuite {

  private val timestamp = 1636116180000L

  private def datapointTuples(n: Int): List[DatapointTuple] = {
    datapoints(n).map(_.toTuple)
  }

  private def datapoints(n: Int): List[Datapoint] = {
    (0 until n).toList.map { i =>
      val tags = SortedTagMap(
        "name" -> "test",
        "i"    -> i.toString,
        "u"    -> UUID.randomUUID().toString
      )
      val value = i match {
        case 0 => Double.NaN
        case 1 => Double.MinValue
        case 2 => Double.MaxValue
        case 3 => Double.MinPositiveValue
        case 4 => Double.NegativeInfinity
        case 5 => Double.PositiveInfinity
        case _ => Random.nextDouble()
      }
      Datapoint(tags, timestamp, value)
    }
  }

  test("encode and decode empty batch tuples") {
    val input = datapointTuples(0)
    val encoded = PublishPayloads.encodeBatch(Map.empty, input)
    val decoded = PublishPayloads.decodeBatch(encoded)
    assertEquals(decoded, input)
  }

  test("encode and decode empty batch") {
    val input = datapoints(0)
    val encoded = PublishPayloads.encodeBatchDatapoints(Map.empty, input)
    val decoded = PublishPayloads.decodeBatchDatapoints(encoded)
    assertEquals(decoded, input)
  }

  test("encode and decode batch tuples") {
    val input = datapointTuples(10)
    val encoded = PublishPayloads.encodeBatch(Map.empty, input)
    val decoded = PublishPayloads.decodeBatch(encoded)
    assert(decoded.head.value.isNaN)
    assertEquals(decoded.head.tags, input.head.tags)
    assertEquals(decoded.tail, input.tail)
  }

  test("encode and decode batch tuples with common tags") {
    val input = datapointTuples(10)
    val encoded = PublishPayloads.encodeBatch(Map("common" -> "v"), input)
    val decoded = PublishPayloads.decodeBatch(encoded)

    val expected = input.map { d =>
      val tags = d.tags + ("common" -> "v")
      val id = ItemIdCalculator.compute(tags)
      d.copy(id = id, tags = tags)
    }

    assert(decoded.head.value.isNaN)
    assertEquals(decoded.head.tags, expected.head.tags)
    assertEquals(decoded.tail, expected.tail)
  }

  test("encode and decode batch tuples with common tags conflict") {
    val input = datapointTuples(10)
    // value from metric should override
    val encoded = PublishPayloads.encodeBatch(Map("i" -> "c"), input)
    val decoded = PublishPayloads.decodeBatch(encoded)

    assert(decoded.head.value.isNaN)
    assertEquals(decoded.head.tags, input.head.tags)
    assertEquals(decoded.tail, input.tail)
  }

  test("decode batch tuples with common tags after data") {
    val input =
      """
        |{
        |  "metrics": [
        |    {
        |      "tags": {"name": "test", "a": "1", "b": "2"},
        |      "timestamp": 1651170761000,
        |      "value": 1.0
        |    }
        |  ],
        |  "tags": {"common": "v", "b": "3"}
        |}
        |""".stripMargin
    val decoded = PublishPayloads.decodeBatch(input)

    val tags = Map("name" -> "test", "a" -> "1", "b" -> "2", "common" -> "v")
    val id = ItemIdCalculator.compute(tags)
    val expected = List(
      DatapointTuple(
        id = id,
        tags = tags,
        timestamp = 1651170761000L,
        value = 1.0
      )
    )

    assertEquals(decoded, expected)
  }

  test("decode batch tuples with common tags before and after data") {
    val metrics =
      """
        |"metrics": [
        |  {
        |    "tags": {"name": "test", "a": "1", "b": "2"},
        |    "timestamp": 1651170761000,
        |    "value": 1.0
        |  }
        |]
        |""".stripMargin
    val inputBefore = s"""{"tags": {"common": "v", "b": "3"}, $metrics}"""
    val inputAfter = s"""{$metrics, "tags": {"common": "v", "b": "3"}}"""

    val decodedBefore = PublishPayloads.decodeBatch(inputBefore)
    val decodedAfter = PublishPayloads.decodeBatch(inputAfter)

    assertEquals(decodedBefore, decodedAfter)
  }

  test("decode batch datapoints with common tags before and after data") {
    val metrics =
      """
        |"metrics": [
        |  {
        |    "tags": {"name": "test", "a": "1", "b": "2"},
        |    "timestamp": 1651170761000,
        |    "value": 1.0
        |  }
        |]
        |""".stripMargin
    val inputBefore = s"""{"tags": {"common": "v", "b": "3"}, $metrics}"""
    val inputAfter = s"""{$metrics, "tags": {"common": "v", "b": "3"}}"""

    val decodedBefore = PublishPayloads.decodeBatchDatapoints(inputBefore)
    val decodedAfter = PublishPayloads.decodeBatchDatapoints(inputAfter)

    assertEquals(decodedBefore, decodedAfter)
  }

  test("encode and decode batch") {
    val input = datapoints(10)
    val encoded = PublishPayloads.encodeBatchDatapoints(Map.empty, input)
    val decoded = PublishPayloads.decodeBatchDatapoints(encoded)
    assert(decoded.head.value.isNaN)
    assertEquals(decoded.head.tags, input.head.tags)
    assertEquals(decoded.tail, input.tail)
  }

  test("encode and decode empty compact batch tuples") {
    val input = datapointTuples(0)
    val encoded = PublishPayloads.encodeCompactBatch(input)
    val decoded = PublishPayloads.decodeCompactBatch(encoded)
    assertEquals(decoded, input)
  }

  test("encode and decode compact batch tuples") {
    val input = datapointTuples(10)
    val encoded = PublishPayloads.encodeCompactBatch(input)
    val decoded = PublishPayloads.decodeCompactBatch(encoded)
    assert(decoded.head.value.isNaN)
    assertEquals(decoded.head.tags, input.head.tags)
    assertEquals(decoded.tail, input.tail)
  }

  test("encode and decode empty tuples list") {
    val input = datapointTuples(0)
    val encoded = PublishPayloads.encodeList(input)
    val decoded = PublishPayloads.decodeList(encoded)
    assertEquals(decoded, input)
  }

  test("encode and decode tuples list") {
    val input = datapointTuples(10)
    val encoded = PublishPayloads.encodeList(input)
    val decoded = PublishPayloads.decodeList(encoded)
    assert(decoded.head.value.isNaN)
    assertEquals(decoded.head.tags, input.head.tags)
    assertEquals(decoded.tail, input.tail)
  }

  test("datapoints: encode and decode batch tuples") {
    val input = datapointTuples(10)
    val encoded = PublishPayloads.encodeBatch(Map.empty, input)
    val decoded = PublishPayloads.decodeBatchDatapoints(encoded)
    assert(decoded.head.value.isNaN)
    assertEquals(decoded.head.tags, input.head.tags)
    assertEquals(decoded.tail, input.tail.map(_.toDatapoint))
  }
}
