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
package com.netflix.atlas.core.index

import java.util.UUID
import com.netflix.atlas.core.model.BasicTaggedItem
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.util.SortedTagMap
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole

/**
  * Check to see how query index performs with simple queries based on index size. With similar test
  * with real data using 17k alert expressions that decomposed into over 33k query expressions, the
  * index was around 1000x faster for processing a metrics payload of 5000 datapoints. The loop took
  * around 6 seconds and the index took around 6ms. The real dataset is slower mostly due to more
  * regex being used in real queries and not being used in this synthetic data.
  *
  * ```
  * > jmh:run -wi 10 -i 10 -f1 -t1 .*RoaringTagIndexBench.*
  * ```
  *
  * Results:
  *
  * ```
  * Benchmark                                Mode  Cnt         Score        Error  Units
  * RoaringTagIndexBench.create             thrpt   10        22.204 ±      0.907  ops/s
  * RoaringTagIndexBench.findKeysAll        thrpt   10  14053463.630 ± 656709.056  ops/s
  * RoaringTagIndexBench.findKeysQuery      thrpt   10      1323.195 ±     81.122  ops/s
  * RoaringTagIndexBench.findValuesAllMany  thrpt   10      4172.656 ±    305.764  ops/s
  * RoaringTagIndexBench.findValuesAllOne   thrpt   10    357671.851 ±  17709.502  ops/s
  * ```
  */
@State(Scope.Thread)
class RoaringTagIndexBench {

  private val baseId = Map(
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

  private val items = (0 until 10000).map { _ =>
    val id = UUID.randomUUID().toString
    BasicTaggedItem(SortedTagMap(baseId ++ Map("nf.node" -> id))) // , i.toString -> id))
  }

  private val index = RoaringTagIndex[BasicTaggedItem](items.toArray, new IndexStats())

  @Benchmark
  def create(bh: Blackhole): Unit = {
    bh.consume(RoaringTagIndex[BasicTaggedItem](items.toArray, new IndexStats()))
  }

  @Benchmark
  def findKeysAll(bh: Blackhole): Unit = {
    bh.consume(index.findKeys(TagQuery(None)))
  }

  @Benchmark
  def findKeysQuery(bh: Blackhole): Unit = {
    bh.consume(index.findKeys(TagQuery(Some(Query.Equal("nf.stack", "dev")))))
  }

  @Benchmark
  def findValuesAllOne(bh: Blackhole): Unit = {
    bh.consume(index.findValues(TagQuery(None, Some("nf.app"))))
  }

  @Benchmark
  def findValuesAllMany(bh: Blackhole): Unit = {
    bh.consume(index.findValues(TagQuery(None, Some("nf.node"))))
  }
}
