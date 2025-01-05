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

import com.netflix.atlas.core.model.ItemId
import com.netflix.atlas.core.model.ItemIdCalculator
import com.netflix.atlas.core.util.ArrayHelper
import com.netflix.atlas.core.util.ComparableComparator
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole

import java.util

/**
  * Try different ways of performing batch update of index items.
  *
  * ```
  * > jmh:run -wi 10 -i 10 -f1 -t1 .*BatchRebuild.*
  * ```
  *
  * Results:
  *
  * ```
  * Benchmark                 Mode  Cnt          Score           Error   Units
  * hashMap                  thrpt    5          0.476 ±         0.063   ops/s
  * hashMapMerge             thrpt    5          3.544 ±         0.111   ops/s
  * treeMap                  thrpt    5          1.358 ±         0.013   ops/s
  * treeMapMerge             thrpt    5          2.000 ±         0.407   ops/s
  *
  * Benchmark                 Mode  Cnt          Score           Error   Units
  * hashMap                  alloc    5  114854340.107 ±    112705.849    B/op
  * hashMapMerge             alloc    5   33407504.311 ±      5704.459    B/op
  * treeMap                  alloc    5   88702403.314 ±     12202.989    B/op
  * treeMapMerge             alloc    5   33457019.848 ±     39144.317    B/op
  * ```
  */
@State(Scope.Thread)
class BatchRebuild {

  private val existingItems = (0 until 2_000_000)
    .map(i => ItemIdCalculator.compute(Map("i" -> i.toString)))
    .toList
    .sorted

  private val pendingItems = {
    val vs = (2_000_000 until 2_010_000)
      .map(i => ItemIdCalculator.compute(Map("i" -> i.toString)))
    val jlist = new util.ArrayList[ItemId](10_000)
    vs.foreach(jlist.add)
    jlist
  }

  @Benchmark
  def hashMap(bh: Blackhole): Unit = {
    val items = new java.util.HashMap[ItemId, ItemId](pendingItems.size())
    pendingItems.forEach { i =>
      items.put(i, i)
    }

    val matches = existingItems
    matches.foreach { i =>
      items.put(i, i)
    }

    val array = items.values.toArray(new Array[ItemId](0))
    java.util.Arrays.sort(array.asInstanceOf[Array[AnyRef]])
    bh.consume(array)
  }

  @Benchmark
  def treeMap(bh: Blackhole): Unit = {
    val items = new java.util.TreeMap[ItemId, ItemId]
    pendingItems.forEach { i =>
      items.put(i, i)
    }

    val matches = existingItems
    matches.foreach { i =>
      items.put(i, i)
    }

    val array = items.values.toArray(new Array[ItemId](0))
    java.util.Arrays.sort(array.asInstanceOf[Array[AnyRef]])
    bh.consume(array)
  }

  @Benchmark
  def hashMapMerge(bh: Blackhole): Unit = {
    val items = new java.util.HashMap[ItemId, ItemId](pendingItems.size())
    pendingItems.forEach { i =>
      items.put(i, i)
    }

    val matches = existingItems
    matches.foreach { i =>
      items.remove(i)
    }

    val a1 = matches.toArray // assume already sorted
    val a2 = items.values.toArray(new Array[ItemId](0))
    java.util.Arrays.sort(a2.asInstanceOf[Array[AnyRef]])

    val dst = new Array[ItemId](a1.length + a2.length)
    val length = ArrayHelper.merge[ItemId](
      new ComparableComparator[ItemId],
      (a, _) => a,
      a1,
      a1.length,
      a2,
      a2.length,
      dst
    )

    if (length != dst.length) {
      throw new IllegalStateException("unexpected length")
    }

    bh.consume(dst)
  }

  @Benchmark
  def treeMapMerge(bh: Blackhole): Unit = {
    val items = new java.util.TreeMap[ItemId, ItemId]
    pendingItems.forEach { i =>
      items.put(i, i)
    }

    val matches = existingItems
    matches.foreach { i =>
      items.remove(i)
    }

    val a1 = matches.toArray // assume already sorted
    val a2 = items.values.toArray(new Array[ItemId](0))

    val dst = new Array[ItemId](a1.length + a2.length)
    val length = ArrayHelper.merge[ItemId](
      new ComparableComparator[ItemId],
      (a, _) => a,
      a1,
      a1.length,
      a2,
      a2.length,
      dst
    )

    if (length != dst.length) {
      throw new IllegalStateException("unexpected length")
    }

    bh.consume(dst)
  }
}
