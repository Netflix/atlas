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
package com.netflix.atlas.core.model

import com.netflix.atlas.core.util.ArrayHelper
import com.netflix.atlas.core.util.ComparableComparator
import com.netflix.atlas.core.util.Hash
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole

import scala.util.Random

/**
  * Measures the cost of ordering `ItemId`s, which the index rebuild pays via
  * `Arrays.sort`/`ArrayHelper.merge` on `BatchUpdateTagIndex.rebuildIndex` (the comparator is
  * `ItemId.compareTo` -> `java.util.Arrays.compareUnsigned` over the 20-byte SHA1 arrays). Used to
  * size opportunity #5: run on `main` (baseline) and on a prototype branch that changes
  * `ItemId.compareTo`, then diff.
  *
  * Note this sorts `Array[ItemId]` directly, whereas the rebuild sorts `TaggedItem`s via
  * `IdComparator` (`t1.id.compareTo(t2.id)` — an extra deref per comparison). So it slightly
  * over-weights `compareTo`, which is the conservative direction for sizing a `compareTo` change.
  *
  * ```
  * > jmh:run -wi 5 -i 10 -f1 -t1 .*ItemIdSortBench.*
  * ```
  */
@State(Scope.Thread)
class ItemIdSortBench {

  // Distinct, realistic 20-byte SHA1 ids.
  private def id(i: Int): ItemId = ItemId(Hash.sha1bytes(i.toString))

  // Pending batch (sorted on each rebuild).
  private val pending: Array[ItemId] = {
    val rnd = new Random(42)
    rnd.shuffle((0 until 50_000).iterator.map(id).toBuffer).toArray
  }

  // Existing index contents, already sorted by id, merged with the pending batch each rebuild.
  private var existing: Array[ItemId] = _
  private var pendingSorted: Array[ItemId] = _

  private val comparator = new ComparableComparator[ItemId]

  @Setup
  def setup(): Unit = {
    existing = (1_000_000 until 1_500_000).map(id).toArray
    java.util.Arrays.sort(existing.asInstanceOf[Array[AnyRef]])
    pendingSorted = pending.clone()
    java.util.Arrays.sort(pendingSorted.asInstanceOf[Array[AnyRef]])
  }

  /** Sort of the pending batch — `Arrays.sort(a2, IdComparator)` in the rebuild. */
  @Benchmark
  def sortPending(bh: Blackhole): Unit = {
    val a = pending.clone()
    java.util.Arrays.sort(a.asInstanceOf[Array[AnyRef]])
    bh.consume(a)
  }

  /**
    * Merge of the sorted existing contents with the sorted pending batch — the rebuild merge.
    * The inputs are cloned because `ArrayHelper.merge` nulls index 0 of a fully consumed source
    * (a GC-release the rebuild relies on, where the source arrays are throwaway).
    */
  @Benchmark
  def mergeRebuild(bh: Blackhole): Unit = {
    val a1 = existing.clone()
    val a2 = pendingSorted.clone()
    val dst = new Array[ItemId](a1.length + a2.length)
    val n = ArrayHelper.merge[ItemId](comparator, (a, _) => a, a1, a1.length, a2, a2.length, dst)
    bh.consume(dst)
    bh.consume(n)
  }
}
