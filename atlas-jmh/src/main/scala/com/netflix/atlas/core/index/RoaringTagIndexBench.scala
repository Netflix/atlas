/*
 * Copyright 2014-2018 Netflix, Inc.
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
import com.netflix.atlas.core.util.SmallHashMap
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
  * Initial results:
  *
  * ```
  * Before
  * [info] Benchmark                                Mode  Cnt     Score     Error  Units
  * [info] RoaringTagIndexBench.create             thrpt   10    13.210 ±   1.041  ops/s
  * [info] RoaringTagIndexBench.findKeysAll        thrpt   10  4646.344 ± 176.685  ops/s
  * [info] RoaringTagIndexBench.findKeysQuery      thrpt   10   450.728 ±  39.296  ops/s
  * [info] RoaringTagIndexBench.findValuesAllMany  thrpt   10   739.919 ±  41.530  ops/s
  * [info] RoaringTagIndexBench.findValuesAllOne   thrpt   10  2194.050 ± 248.351  ops/s
  *
  * After
  * [info] Benchmark                                Mode  Cnt        Score        Error  Units
  * [info] RoaringTagIndexBench.create             thrpt   10       20.270 ±      0.798  ops/s
  * [info] RoaringTagIndexBench.findKeysAll        thrpt   10  6382544.499 ± 353321.426  ops/s
  * [info] RoaringTagIndexBench.findKeysQuery      thrpt   10     1118.262 ±    370.960  ops/s
  * [info] RoaringTagIndexBench.findValuesAllMany  thrpt   10     3849.128 ±    416.156  ops/s
  * [info] RoaringTagIndexBench.findValuesAllOne   thrpt   10     4485.419 ±    424.741  ops/s
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

  private val items = (0 until 10000).map { i =>
    val id = UUID.randomUUID().toString
    BasicTaggedItem(SmallHashMap(baseId ++ Map("nf.node" -> id))) //, i.toString -> id))
  }

  private val index = new RoaringTagIndex[BasicTaggedItem](items.toArray)

  @Benchmark
  def create(bh: Blackhole): Unit = {
    bh.consume(new RoaringTagIndex[BasicTaggedItem](items.toArray))
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
