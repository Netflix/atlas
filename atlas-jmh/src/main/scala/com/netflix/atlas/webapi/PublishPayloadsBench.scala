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
import com.netflix.atlas.core.model.ItemId
import com.netflix.atlas.core.util.SortedTagMap
import com.netflix.atlas.core.util.Streams
import com.netflix.atlas.json.Json
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole

import java.io.ByteArrayInputStream
import scala.util.Using

/**
  * > jmh:run -prof stack -prof gc -wi 5 -i 10 -f1 -t1 .*PublishPayloads.*
  *
  * ```
  * Benchmark                         Mode  Cnt         Score         Error   Units
  * decodeBatch                      thrpt    5        23.787 ±       1.148   ops/s
  * decodeBatchDatapoints            thrpt    5       129.900 ±       3.767   ops/s
  * decodeCompactBatch               thrpt    5       173.148 ±       1.835   ops/s
  * decodeList                       thrpt    5        25.277 ±       0.254   ops/s
  * encodeBatch                      thrpt    5       179.382 ±      39.696   ops/s
  * encodeCompactBatch               thrpt    5        78.944 ±      13.537   ops/s
  * encodeList                       thrpt    5       170.112 ±      19.728   ops/s
  * ```
  */
@State(Scope.Thread)
class PublishPayloadsBench {

  import PublishPayloadsBench.*

  private val tagMap = SortedTagMap(
    "nf.app"     -> "atlas_backend",
    "nf.cluster" -> "atlas_backend-dev",
    "nf.asg"     -> "atlas_backend-dev-v001",
    "nf.stack"   -> "dev",
    "nf.region"  -> "us-east-1",
    "nf.zone"    -> "us-east-1e",
    "nf.node"    -> "i-123456789",
    "nf.ami"     -> "ami-987654321",
    "nf.vmtype"  -> "r3.2xlarge",
    "name"       -> "jvm.gc.pause",
    "cause"      -> "Allocation_Failure",
    "action"     -> "end_of_major_GC",
    "statistic"  -> "totalTime"
  )

  private val datapoints = {
    (0 until 10_000).toList.map { i =>
      val tags = SortedTagMap(tagMap + ("i" -> i.toString))
      Datapoint(tags, 1636116180000L, Math.PI).toTuple
    }
  }

  private def decodeBatch(data: Array[Byte]): List[DatapointTuple] = {
    Using.resource(Json.newSmileParser(new ByteArrayInputStream(data))) { parser =>
      PublishPayloads.decodeBatch(parser)
    }
  }

  // Skips the ID calculation.
  private def decodeBatchDatapoints(data: Array[Byte]): List[Datapoint] = {
    Using.resource(Json.newSmileParser(new ByteArrayInputStream(data))) { parser =>
      PublishPayloads.decodeBatchDatapoints(parser)
    }
  }

  private def encodeBatch(values: List[DatapointTuple]): Array[Byte] = {
    Streams.byteArray { out =>
      Using.resource(Json.newSmileGenerator(out)) { gen =>
        PublishPayloads.encodeBatch(gen, Map.empty, values)
      }
    }
  }

  private def decodeCompactBatch(data: Array[Byte], consumer: PublishConsumer): Unit = {
    Using.resource(Json.newSmileParser(new ByteArrayInputStream(data))) { parser =>
      PublishPayloads.decodeCompactBatch(parser, consumer)
    }
  }

  private def encodeCompactBatch(values: List[DatapointTuple]): Array[Byte] = {
    Streams.byteArray { out =>
      Using.resource(Json.newSmileGenerator(out)) { gen =>
        PublishPayloads.encodeCompactBatch(gen, values)
      }
    }
  }

  private def decodeList(data: Array[Byte]): List[DatapointTuple] = {
    Using.resource(Json.newSmileParser(new ByteArrayInputStream(data))) { parser =>
      PublishPayloads.decodeList(parser)
    }
  }

  private def encodeList(values: List[DatapointTuple]): Array[Byte] = {
    Streams.byteArray { out =>
      Using.resource(Json.newSmileGenerator(out)) { gen =>
        PublishPayloads.encodeList(gen, values)
      }
    }
  }

  private val encodedBatch = encodeBatch(datapoints)

  private val encodedCompactBatch = encodeCompactBatch(datapoints)

  private val encodedList = encodeList(datapoints)

  @Benchmark
  def decodeBatch(bh: Blackhole): Unit = {
    val consumer = new BlackholePublishConsumer(bh)
    decodeBatch(encodedBatch).foreach { d =>
      consumer.consume(d.id, d.tags, d.timestamp, d.value)
    }
  }

  @Benchmark
  def decodeBatchDatapoints(bh: Blackhole): Unit = {
    val consumer = new BlackholePublishConsumer(bh)
    decodeBatchDatapoints(encodedBatch).foreach { d =>
      consumer.consume(null, d.tags, d.timestamp, d.value)
    }
  }

  @Benchmark
  def decodeCompactBatch(bh: Blackhole): Unit = {
    val consumer = new BlackholePublishConsumer(bh)
    decodeCompactBatch(encodedCompactBatch, consumer)
  }

  @Benchmark
  def decodeList(bh: Blackhole): Unit = {
    val consumer = new BlackholePublishConsumer(bh)
    decodeList(encodedList).foreach { d =>
      consumer.consume(d.id, d.tags, d.timestamp, d.value)
    }
  }

  @Benchmark
  def encodeBatch(bh: Blackhole): Unit = {
    bh.consume(encodeBatch(datapoints))
  }

  @Benchmark
  def encodeCompactBatch(bh: Blackhole): Unit = {
    bh.consume(encodeCompactBatch(datapoints))
  }

  @Benchmark
  def encodeList(bh: Blackhole): Unit = {
    bh.consume(encodeList(datapoints))
  }
}

object PublishPayloadsBench {

  class BlackholePublishConsumer(bh: Blackhole) extends PublishConsumer {

    override def consume(
      id: ItemId,
      tags: Map[String, String],
      timestamp: Long,
      value: Double
    ): Unit = {
      bh.consume(id)
      bh.consume(tags)
      bh.consume(timestamp)
      bh.consume(value)
    }
  }
}
