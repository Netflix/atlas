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

import org.openjdk.jol.info.ClassLayout
import org.openjdk.jol.info.GraphLayout
import munit.FunSuite

import scala.util.Random

class IntHashSetSuite extends FunSuite {

  test("add") {
    val s = new IntHashSet(-1, 10)
    s.add(11)
    assertEquals(List(11), s.toList)
    assertEquals(1, s.size)
  }

  test("dedup") {
    val s = new IntHashSet(-1, 10)
    s.add(42)
    assertEquals(List(42), s.toList)
    assertEquals(1, s.size)
    s.add(42)
    assertEquals(List(42), s.toList)
    assertEquals(1, s.size)
  }

  test("resize") {
    val s = new IntHashSet(-1, 10)
    (0 until 10000).foreach(s.add)
    assertEquals((0 until 10000).toSet, s.toList.toSet)
    assertEquals(s.size, 10000)
  }

  test("random") {
    val jset = new scala.collection.mutable.HashSet[Int]
    val iset = new IntHashSet(-1, 10)
    (0 until 10000).foreach { _ =>
      val v = Random.nextInt()
      iset.add(v)
      jset.add(v)
    }
    assertEquals(jset.toSet, iset.toList.toSet)
  }

  private def arrayCompare(a1: Array[Int], a2: Array[Int]): Unit = {

    // Need to sort as traversal order could be different when generating the arrays
    java.util.Arrays.sort(a1)
    java.util.Arrays.sort(a2)
    assertEquals(a1.toSeq, a2.toSeq)
  }

  test("toArray") {
    val jset = new scala.collection.mutable.HashSet[Int]
    val iset = new IntHashSet(-1, 10)
    (0 until 10000).foreach { _ =>
      val v = Random.nextInt()
      iset.add(v)
      jset.add(v)
    }
    arrayCompare(jset.toArray, iset.toArray)
  }

  test("memory per set") {
    // Sanity check to verify if some change introduces more overhead per set
    val bytes = ClassLayout.parseClass(classOf[IntHashSet]).instanceSize()
    assertEquals(bytes, 32L)
  }

  test("memory - 5 items") {
    val iset = new IntHashSet(-1, 10)
    val jset = new java.util.HashSet[Int](10)
    (0 until 5).foreach { i =>
      iset.add(i)
      jset.add(i)
    }

    val igraph = GraphLayout.parseInstance(iset)
    // val jgraph = GraphLayout.parseInstance(jset)

    // println(igraph.toFootprint)
    // println(jgraph.toFootprint)

    // Only objects should be the array and the set itself
    assertEquals(igraph.totalCount(), 2L)

    // Sanity check size is < 100 bytes
    assert(igraph.totalSize() <= 100)
  }

  test("memory - 10k items") {
    val iset = new IntHashSet(-1, 10)
    val jset = new java.util.HashSet[Int](10)
    (0 until 10000).foreach { i =>
      iset.add(i)
      jset.add(i)
    }

    val igraph = GraphLayout.parseInstance(iset)
    // val jgraph = GraphLayout.parseInstance(jset)

    // println(igraph.toFootprint)
    // println(jgraph.toFootprint)

    // Only objects should be the array and the set itself
    assertEquals(igraph.totalCount(), 2L)

    // Sanity check size is < 110kb
    assert(igraph.totalSize() <= 110000)
  }

  test("negative absolute value") {
    val s = new IntHashSet(-1, 10)
    s.add(Integer.MIN_VALUE)
  }
}
