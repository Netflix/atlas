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
package com.netflix.atlas.core.index

import com.netflix.atlas.core.model.Query
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Threads
import org.openjdk.jmh.infra.Blackhole

/**
 * Check to see how query index performs with simple queries based on index size. With similar test
 * with real data using 17k alert expressions that decomposed into over 33k query expressions, the
 * index was around 1000x faster for processing a metrics payload of 5000 datapoints. The loop took
 * around 6 seconds and the index took around 6ms. The real dataset is slower mostly due to more
 * regex being used in real queries and not being used in this synthetic data.
 *
 * ```
 * > jmh:run -wi 10 -i 10 -f1 -t1 .*QueryIndexMatching.*
 * ```
 *
 * Initial results:
 *
 * ```
 * Benchmark                         Mode  Cnt        Score        Error  Units
 * QueryIndexMatching.index_100     thrpt   10  1427970.545 ±  42632.895  ops/s
 * QueryIndexMatching.index_1000    thrpt   10  1337580.661 ± 113418.137  ops/s
 * QueryIndexMatching.index_10000   thrpt   10  1341069.994 ± 104992.441  ops/s
 * QueryIndexMatching.index_100000  thrpt   10  1290159.738 ±  76488.013  ops/s
 * QueryIndexMatching.loop_100      thrpt   10   714393.977 ±  26067.308  ops/s
 * QueryIndexMatching.loop_1000     thrpt   10    68317.877 ±   6006.013  ops/s
 * QueryIndexMatching.loop_10000    thrpt   10     3831.356 ±    454.029  ops/s
 * QueryIndexMatching.loop_100000   thrpt   10      375.074 ±     30.352  ops/s
 * ```
 */
@State(Scope.Thread)
class QueryIndexMatching {

  // CpuUsage for all instances
  private val cpuUsage = Query.Equal("name", "cpuUsage")

  // DiskUsage query per node
  private val diskUsage = Query.Equal("name", "diskUsage")

  private def diskUsagePerNode(n: Int): List[Query] = {
    (0 until n).toList.map { i =>
      val node = f"i-$i%05d"
      Query.And(Query.Equal("nf.node", node), diskUsage)
    }
  }

  private val queries_100 = cpuUsage :: diskUsagePerNode(100)
  private val index_100 = QueryIndex(queries_100)

  private val queries_1000 = cpuUsage :: diskUsagePerNode(1000)
  private val index_1000 = QueryIndex(queries_1000)

  private val queries_10000 = cpuUsage :: diskUsagePerNode(10000)
  private val index_10000 = QueryIndex(queries_10000)

  private val queries_100000 = cpuUsage :: diskUsagePerNode(100000)
  private val index_100000 = QueryIndex(queries_100000)

  // Sample tag map that doesn't match the above queries. Not matching is often the most expensive
  // because it will not be able to short circuit
  private val id = Map(
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

  @Threads(1)
  @Benchmark
  def loop_100(bh: Blackhole): Unit = {
    bh.consume(queries_100.exists(_.matches(id)))
  }

  @Threads(1)
  @Benchmark
  def loop_1000(bh: Blackhole): Unit = {
    bh.consume(queries_1000.exists(_.matches(id)))
  }

  @Threads(1)
  @Benchmark
  def loop_10000(bh: Blackhole): Unit = {
    bh.consume(queries_10000.exists(_.matches(id)))
  }

  @Threads(1)
  @Benchmark
  def loop_100000(bh: Blackhole): Unit = {
    bh.consume(queries_100000.exists(_.matches(id)))
  }

  @Threads(1)
  @Benchmark
  def index_100(bh: Blackhole): Unit = {
    bh.consume(index_100.matches(id))
  }

  @Threads(1)
  @Benchmark
  def index_1000(bh: Blackhole): Unit = {
    bh.consume(index_1000.matches(id))
  }

  @Threads(1)
  @Benchmark
  def index_10000(bh: Blackhole): Unit = {
    bh.consume(index_10000.matches(id))
  }

  @Threads(1)
  @Benchmark
  def index_100000(bh: Blackhole): Unit = {
    bh.consume(index_100000.matches(id))
  }

}
