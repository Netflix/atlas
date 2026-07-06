/*
 * Copyright 2014-2026 Netflix, Inc.
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
package com.netflix.atlas.core.util

import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.TaggedItem
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Threads
import org.openjdk.jmh.infra.Blackhole

/**
  * Evaluate whether lazy map views (concat / filter) are worth using in place of
  * materializing a new immutable Map when combining or filtering tag maps during
  * evaluation. The matrix isolates the two axes that drive the decision:
  *
  *   build*  - cost (and allocation) of constructing the combined/filtered map. This is
  *             where the view is expected to win: a thin wrapper vs a new Map.
  *   match*  - cost of probing the result with a query (an assortment of gets). The view
  *             may be more expensive here.
  *   id*     - cost of iterating the result via foreachEntry (TaggedItem.computeId). The
  *             view may be more expensive here too.
  *
  * Net effect for a real call site is roughly build + N_gets * match + M_iter * id, so the
  * build savings need to outweigh the per-access overhead at the access counts seen in
  * practice.
  *
  * ```
  * > jmh:run -prof gc -wi 5 -i 5 -f1 -t1 .*MapViewBench.*
  * ```
  */
@State(Scope.Thread)
class MapViewBench {

  // commonTags ++ seriesTags, as in the druid accumulator. The common (lhs) tags are the
  // shared nf.* dimensions; the series (rhs) tags carry the metric name and per-series
  // dimensions. nf.cluster overlaps so the concat must prefer the value from the series
  // (rhs) side. The query below probes one key from each side so the lookup is not always a
  // fall-through to the second map.
  private val commonTags = Map(
    "nf.app"     -> "atlas_backend",
    "nf.region"  -> "us-east-1",
    "nf.cluster" -> "atlas_backend-common"
  )

  private val seriesTags = Map(
    "name"       -> "jvm.gc.pause",
    "nf.node"    -> "i-123456789",
    "nf.cluster" -> "atlas_backend-dev"
  )

  // Full tag map filtered down to the exact-match keys, as in commonTags / group by.
  private val fullTags = Map(
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

  private val keys: Set[String] = Set("name", "nf.app", "nf.cluster")

  // Query that probes a few keys present in the result of both the concat and the filter.
  private val query: Query = Query.And(
    Query.Equal("name", "jvm.gc.pause"),
    Query.Equal("nf.app", "atlas_backend")
  )

  // Pre-built results for isolating access cost from build cost.
  private var concatMaterialized: Map[String, String] = _
  private var concatView: Map[String, String] = _
  private var filterMaterialized: Map[String, String] = _
  private var filterView: Map[String, String] = _

  @Setup
  def setup(): Unit = {
    concatMaterialized = commonTags ++ seriesTags
    concatView = new ConcatMap(commonTags, seriesTags)
    filterMaterialized = fullTags.filter(e => keys.contains(e._1))
    filterView = new FilteredMap(fullTags, keys.contains)
  }

  // --- build only: the allocation win the view is going for -------------------------------

  @Threads(1)
  @Benchmark
  def concatBuildMaterialized(bh: Blackhole): Unit = bh.consume(commonTags ++ seriesTags)

  @Threads(1)
  @Benchmark
  def concatBuildView(bh: Blackhole): Unit = bh.consume(new ConcatMap(commonTags, seriesTags))

  @Threads(1)
  @Benchmark
  def filterBuildMaterialized(bh: Blackhole): Unit =
    bh.consume(fullTags.filter(e => keys.contains(e._1)))

  @Threads(1)
  @Benchmark
  def filterBuildView(bh: Blackhole): Unit = bh.consume(new FilteredMap(fullTags, keys.contains))

  // --- match only: per-get cost on a pre-built result ------------------------------------

  @Threads(1)
  @Benchmark
  def concatMatchMaterialized(bh: Blackhole): Unit = bh.consume(query.matches(concatMaterialized))

  @Threads(1)
  @Benchmark
  def concatMatchView(bh: Blackhole): Unit = bh.consume(query.matches(concatView))

  @Threads(1)
  @Benchmark
  def filterMatchMaterialized(bh: Blackhole): Unit = bh.consume(query.matches(filterMaterialized))

  @Threads(1)
  @Benchmark
  def filterMatchView(bh: Blackhole): Unit = bh.consume(query.matches(filterView))

  // --- id only: foreachEntry cost on a pre-built result ----------------------------------

  @Threads(1)
  @Benchmark
  def concatIdMaterialized(bh: Blackhole): Unit =
    bh.consume(TaggedItem.computeId(concatMaterialized))

  @Threads(1)
  @Benchmark
  def concatIdView(bh: Blackhole): Unit = bh.consume(TaggedItem.computeId(concatView))

  @Threads(1)
  @Benchmark
  def filterIdMaterialized(bh: Blackhole): Unit =
    bh.consume(TaggedItem.computeId(filterMaterialized))

  @Threads(1)
  @Benchmark
  def filterIdView(bh: Blackhole): Unit = bh.consume(TaggedItem.computeId(filterView))
}
