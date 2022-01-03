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

import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.core.model.DatapointTuple
import com.netflix.atlas.core.util.SortedTagMap
import munit.FunSuite

import java.util.UUID
import scala.util.Random

class PublishPayloadsSuite extends FunSuite {

  private val timestamp = 1636116180000L

  private def datapoints(n: Int): List[DatapointTuple] = {
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
      Datapoint(tags, timestamp, value).toTuple
    }
  }

  test("encode and decode empty batch") {
    val input = datapoints(0)
    val encoded = PublishPayloads.encodeBatch(Map.empty, input)
    val decoded = PublishPayloads.decodeBatch(encoded)
    assertEquals(decoded, input)
  }

  test("encode and decode batch") {
    val input = datapoints(10)
    val encoded = PublishPayloads.encodeBatch(Map.empty, input)
    val decoded = PublishPayloads.decodeBatch(encoded)
    assert(decoded.head.value.isNaN)
    assertEquals(decoded.head.tags, input.head.tags)
    assertEquals(decoded.tail, input.tail)
  }

  test("encode and decode empty compact batch") {
    val input = datapoints(0)
    val encoded = PublishPayloads.encodeCompactBatch(input)
    val decoded = PublishPayloads.decodeCompactBatch(encoded)
    assertEquals(decoded, input)
  }

  test("encode and decode compact batch") {
    val input = datapoints(10)
    val encoded = PublishPayloads.encodeCompactBatch(input)
    val decoded = PublishPayloads.decodeCompactBatch(encoded)
    assert(decoded.head.value.isNaN)
    assertEquals(decoded.head.tags, input.head.tags)
    assertEquals(decoded.tail, input.tail)
  }

  test("encode and decode empty list") {
    val input = datapoints(0)
    val encoded = PublishPayloads.encodeList(input)
    val decoded = PublishPayloads.decodeList(encoded)
    assertEquals(decoded, input)
  }

  test("encode and decode list") {
    val input = datapoints(10)
    val encoded = PublishPayloads.encodeList(input)
    val decoded = PublishPayloads.decodeList(encoded)
    assert(decoded.head.value.isNaN)
    assertEquals(decoded.head.tags, input.head.tags)
    assertEquals(decoded.tail, input.tail)
  }

  test("datapoints: encode and decode batch") {
    val input = datapoints(10)
    val encoded = PublishPayloads.encodeBatch(Map.empty, input)
    val decoded = PublishPayloads.decodeBatchDatapoints(encoded)
    assert(decoded.head.value.isNaN)
    assertEquals(decoded.head.tags, input.head.tags)
    assertEquals(decoded.tail, input.tail.map(_.toDatapoint))
  }
}
