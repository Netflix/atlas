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
package com.netflix.atlas.core.util

import java.util.UUID

import org.scalatest.FunSuite

class ShardsSuite extends FunSuite {

  private def createGroups(n: Int, sz: Int): List[Shards.Group[Int]] = {
    val groups = (0 until n).map { i =>
      val name = ('a' + i).toString
      val s = i * sz
      val e = s + sz
      Shards.Group(name, (s until e).toArray)
    }
    groups.toList
  }

  private def minmax(counts: IntIntHashMap): (Int, Int) = {
    var min = Integer.MAX_VALUE
    var max = 0
    counts.foreach { (k, v) =>
      min = math.min(min, v)
      max = math.max(max, v)
    }
    min -> max
  }

  private def printSummary(counts: IntIntHashMap): Unit = {
    val (min, max) = minmax(counts)
    println(s"min: $min, max: $max")
    counts.foreach { (k, v) =>
      println(s"$k => $v")
    }
  }

  test("indices are evenly balanced across instances") {
    for (n <- 1 to 10; sz <- 2 to 7) {
      val groups = createGroups(n, sz)
      val mapper = Shards.mapper(groups)
      val counts = new IntIntHashMap(-1)
      (0 until 20000).foreach { i =>
        counts.increment(mapper.instanceForIndex(i))
      }

      // make sure all shards are used
      assert(counts.size === n * sz)

      // make sure that the distribution is uniform
      val (min, max) = minmax(counts)
      assert(max - min <= 1)
    }
  }

  test("ids are evenly balanced across instances") {
    val ids = (0 until 100000).map { _ =>
      Hash.sha1(UUID.randomUUID().toString)
    }
    for (n <- 1 to 5; sz <- 2 to 7) {
      val groups = createGroups(n, sz)
      val mapper = Shards.mapper(groups)
      val counts = new IntIntHashMap(-1)
      ids.foreach { id =>
        counts.increment(mapper.instanceForId(id))
      }

      // make sure all shards are used
      assert(counts.size === n * sz)

      // make sure that the distribution is uniform
      val (min, max) = minmax(counts)
      val threshold = max / 10.0
      if (max - min > threshold) {
        printSummary(counts)
      }
      assert(max - min <= threshold)
    }
  }

  test("uneven groups") {
    val groups = List(
      Shards.Group("a", Array(0, 1)),
      Shards.Group("b", Array(2, 3, 4, 5)),
      Shards.Group("c", Array(6, 7))
    )

    val mapper = Shards.mapper(groups)
    val counts = new IntIntHashMap(-1)
    (0 until 20000).foreach { i =>
      counts.increment(mapper.instanceForIndex(i))
    }

    // make sure all shards are used
    assert(counts.size === 8)

    // make sure that the distribution is uniform
    val maxAC = 20000 / 6 + 1
    val maxB  = 20000 / 12 + 1
    val expected = Array(
      maxAC, maxAC,
      maxB, maxB, maxB, maxB,
      maxAC, maxAC
    )
    expected.indices.foreach { i =>
      assert(expected(i) - counts.get(i, -1) <= 1)
    }
  }

  test("local mapper containsIndex") {
    val groups = List(
      Shards.Group("a", Array(0, 1)),
      Shards.Group("b", Array(2, 3)),
      Shards.Group("c", Array(4, 5))
    )

    val mapper = Shards.mapper(groups)
    val localInstance = groups.last.instances(0)
    val localMapper = Shards.localMapper(groups.last, 0, groups.size - 1, groups.size)
    (0 until 20000).foreach { i =>
      val idx = mapper.instanceForIndex(i)
      val contains = localMapper.containsIndex(i)
      assert(contains === (idx == localInstance),
        s"$i => instance = $localInstance; mapper.instance = $idx; local.contains = $contains")
    }
  }

  test("local mapper containsId") {
    val groups = List(
      Shards.Group("a", Array(0, 1)),
      Shards.Group("b", Array(2, 3)),
      Shards.Group("c", Array(4, 5))
    )

    val mapper = Shards.mapper(groups)
    val localInstance = groups.last.instances(0)
    val localMapper = Shards.localMapper(groups.last, 0, groups.size - 1, groups.size)
    (0 until 20000).foreach { _ =>
      val id = Hash.sha1(UUID.randomUUID().toString)
      val idx = mapper.instanceForId(id)
      val contains = localMapper.containsId(id)
      assert(contains === (idx == localInstance),
        s"$id => instance = $localInstance; mapper.instance = $idx; local.contains = $contains")
    }
  }

  test("deployment of new group") {
    val groups = List(
      Shards.Group("a", Array(0, 1)),
      Shards.Group("a", Array(6, 7)),
      Shards.Group("b", Array(2, 3)),
      Shards.Group("c", Array(4, 5))
    )

    val expectedMatches = Set(
      Set(0, 6), Set(1, 7), Set(2), Set(3), Set(4), Set(5)
    )

    val mapper = Shards.replicaMapper(groups)
    val counts = new IntIntHashMap(-1)
    (0 until 20000).foreach { i =>
      val instances = mapper.instancesForIndex(i)
      assert(expectedMatches.contains(instances.toSet))
      instances.foreach { v =>
        counts.increment(v)
      }
    }

    // make sure all shards are used
    assert(counts.size === 6 + 2)

    // make sure that the distribution is uniform
    val (min, max) = minmax(counts)
    assert(max - min <= 1)
  }

  test("deployment of new group with change in size") {
    val groups = List(
      Shards.Group("a", Array(0, 1)),
      Shards.Group("a", Array(6, 7, 8, 9)),
      Shards.Group("b", Array(2, 3)),
      Shards.Group("c", Array(4, 5))
    )

    val mapper = Shards.replicaMapper(groups)
    val counts = new IntIntHashMap(-1)
    (0 until 20000).foreach { i =>
      mapper.instancesForIndex(i).foreach { v =>
        counts.increment(v)
      }
    }

    // make sure all shards are used
    assert(counts.size === 6 + 4)

    // make sure data is replicated to new group
    var sum = 0
    counts.foreach((_, v) => sum += v)
    assert(sum >= 20000 + 20000 / 3)
  }
}
