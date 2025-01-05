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
package com.netflix.atlas.core.util

import java.util.UUID

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole

import scala.collection.SortedSet

/**
  * Sanity check performance of utility for merging sorted lists.
  *
  * ```
  * > jmh:run -wi 10 -i 10 -f1 -t1 .*ListMerge.*
  * ...
  * Benchmark                         Mode  Cnt   Score   Error  Units
  * ListMerge.hashSet                thrpt   10   0.386 ± 0.021  ops/s
  * ListMerge.merge                  thrpt   10  42.918 ± 2.044  ops/s
  * ListMerge.sortedSet              thrpt   10   0.238 ± 0.007  ops/s
  * ListMerge.treeSet                thrpt   10   0.316 ± 0.016  ops/s
  * ```
  */
@State(Scope.Thread)
class ListMerge {

  private val vs = (0 until 2000)
    .map(_ => (0 until 1000).map(_ => UUID.randomUUID().toString).toList.sorted)
    .toList

  @Benchmark
  def hashSet(bh: Blackhole): Unit = {
    val set = new java.util.HashSet[String]()
    vs.foreach(_.foreach(set.add))
    val sorted = set.toArray(new Array[String](set.size()))
    java.util.Arrays.sort(sorted.asInstanceOf[Array[AnyRef]])
    bh.consume(sorted.toList.take(1000))
  }

  @Benchmark
  def treeSet(bh: Blackhole): Unit = {
    import scala.jdk.CollectionConverters.*
    val set = new java.util.TreeSet[String]()
    vs.foreach(_.foreach(set.add))
    bh.consume(set.asScala.toList.take(1000))
  }

  @Benchmark
  def sortedSet(bh: Blackhole): Unit = {
    val set = SortedSet.empty[String]
    bh.consume(vs.foldLeft(set)(_ ++ _).toList.take(1000))
  }

  @Benchmark
  def merge(bh: Blackhole): Unit = {
    bh.consume(ListHelper.merge(1000, vs))
  }

}
