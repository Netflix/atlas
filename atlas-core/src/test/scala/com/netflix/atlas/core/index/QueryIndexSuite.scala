/*
 * Copyright 2015 Netflix, Inc.
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
import org.scalatest.FunSuite

class QueryIndexSuite extends FunSuite {

  private val id1 = Map("a" -> "1")
  private val id2 = Map("a" -> "1", "b" -> "2")
  private val id3 = Map("a" -> "1", "b" -> "2")

  test("empty") {
    val index = QueryIndex(Nil)
    assert(!index.matches(Map.empty))
    assert(!index.matches(Map("a" -> "1")))
  }

  test("matchingEntries empty") {
    val index = QueryIndex(Nil)
    assert(index.matchingEntries(Map.empty).isEmpty)
    assert(index.matchingEntries(Map("a" -> "1")).isEmpty)
  }

  test("single query: simple") {
    val q = Query.And(Query.Equal("a", "1"), Query.Equal("b", "2"))
    val index = QueryIndex(List(q))

    // Not all tags are present
    assert(!index.matches(Map.empty))
    assert(!index.matches(Map("a" -> "1")))

    // matches
    assert(index.matches(Map("a" -> "1", "b" -> "2")))
    assert(index.matches(Map("a" -> "1", "b" -> "2", "c" -> "3")))

    // a doesn't match
    assert(!index.matches(Map("a" -> "2", "b" -> "2", "c" -> "3")))

    // b doesn't match
    assert(!index.matches(Map("a" -> "1", "b" -> "3", "c" -> "3")))
  }

  test("matchingEntries single query: simple") {
    val q = Query.And(Query.Equal("a", "1"), Query.Equal("b", "2"))
    val index = QueryIndex(List(q))

    // Not all tags are present
    assert(index.matchingEntries(Map.empty).isEmpty)
    assert(index.matchingEntries(Map("a" -> "1")).isEmpty)

    // matches
    assert(index.matchingEntries(Map("a" -> "1", "b" -> "2")) === List(q))
    assert(index.matchingEntries(Map("a" -> "1", "b" -> "2", "c" -> "3")) === List(q))

    // a doesn't match
    assert(index.matchingEntries(Map("a" -> "2", "b" -> "2", "c" -> "3")).isEmpty)

    // b doesn't match
    assert(index.matchingEntries(Map("a" -> "1", "b" -> "3", "c" -> "3")).isEmpty)
  }

  test("single query: complex") {
    val q = Query.And(Query.And(Query.Equal("a", "1"), Query.Equal("b", "2")), Query.HasKey("c"))
    val index = QueryIndex(List(q))

    // Not all tags are present
    assert(!index.matches(Map.empty))
    assert(!index.matches(Map("a" -> "1")))
    assert(!index.matches(Map("a" -> "1", "b" -> "2")))

    // matches
    assert(index.matches(Map("a" -> "1", "b" -> "2", "c" -> "3")))

    // a doesn't match
    assert(!index.matches(Map("a" -> "2", "b" -> "2", "c" -> "3")))

    // b doesn't match
    assert(!index.matches(Map("a" -> "1", "b" -> "3", "c" -> "3")))
  }

  test("matchingEntries single query: complex") {
    val q = Query.And(Query.And(Query.Equal("a", "1"), Query.Equal("b", "2")), Query.HasKey("c"))
    val index = QueryIndex(List(q))

    // Not all tags are present
    assert(index.matchingEntries(Map.empty).isEmpty)
    assert(index.matchingEntries(Map("a" -> "1")).isEmpty)
    assert(index.matchingEntries(Map("a" -> "1", "b" -> "2")).isEmpty)

    // matchingEntries
    assert(index.matchingEntries(Map("a" -> "1", "b" -> "2", "c" -> "3")) === List(q))

    // a doesn't match
    assert(index.matchingEntries(Map("a" -> "2", "b" -> "2", "c" -> "3")).isEmpty)

    // b doesn't match
    assert(index.matchingEntries(Map("a" -> "1", "b" -> "3", "c" -> "3")).isEmpty)
  }

  test("many queries") {
    // CpuUsage for all instances
    val cpuUsage = Query.Equal("name", "cpuUsage")

    // DiskUsage query per node
    val diskUsage = Query.Equal("name", "diskUsage")
    val diskUsagePerNode = (0 until 100).toList.map { i =>
      val node = f"i-$i%05d"
      Query.And(Query.Equal("nf.node", node), diskUsage)
    }

    val index = QueryIndex(cpuUsage :: diskUsagePerNode)

    // Not all tags are present
    assert(!index.matches(Map.empty))
    assert(!index.matches(Map("a" -> "1")))

    // matches
    assert(index.matches(Map("name" -> "cpuUsage", "nf.node" -> "unknown")))
    assert(index.matches(Map("name" -> "cpuUsage", "nf.node" -> "i-00099")))
    assert(index.matches(Map("name" -> "diskUsage", "nf.node" -> "i-00099")))

    // shouldn't match
    assert(!index.matches(Map("name" -> "diskUsage", "nf.node" -> "unknown")))
    assert(!index.matches(Map("name" -> "memoryUsage", "nf.node" -> "i-00099")))
  }

  test("matchingEntries many queries") {
    // CpuUsage for all instances
    val cpuUsage = Query.Equal("name", "cpuUsage")

    // DiskUsage query per node
    val diskUsage = Query.Equal("name", "diskUsage")
    val diskUsagePerNode = (0 until 100).toList.map { i =>
      val node = f"i-$i%05d"
      Query.And(Query.Equal("nf.node", node), diskUsage)
    }

    val index = QueryIndex(cpuUsage :: diskUsage :: diskUsagePerNode)

    // Not all tags are present
    assert(index.matchingEntries(Map.empty).isEmpty)
    assert(index.matchingEntries(Map("a" -> "1")).isEmpty)

    // matchingEntries
    assert(index.matchingEntries(Map("name" -> "cpuUsage", "nf.node" -> "unknown")) === List(cpuUsage))
    assert(index.matchingEntries(Map("name" -> "cpuUsage", "nf.node" -> "i-00099")) === List(cpuUsage))
    assert(index.matchingEntries(Map("name" -> "diskUsage", "nf.node" -> "i-00099")) === List(diskUsagePerNode.last, diskUsage))
    assert(index.matchingEntries(Map("name" -> "diskUsage", "nf.node" -> "unknown")) === List(diskUsage))

    // shouldn't match
    assert(index.matchingEntries(Map("name" -> "memoryUsage", "nf.node" -> "i-00099")).isEmpty)
  }
}
