/*
 * Copyright 2014-2023 Netflix, Inc.
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

import java.net.URI
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.MathExpr
import com.netflix.atlas.core.model.ModelExtractors
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.StyleVocabulary
import com.netflix.atlas.core.model.TimeSeriesExpr
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.util.SmallHashMap
import com.netflix.atlas.core.util.Streams
import org.openjdk.jol.info.GraphLayout
import munit.FunSuite

import scala.util.Using

class QueryIndexSuite extends FunSuite {

  private def matches[T](index: QueryIndex[T], tags: Map[String, String]): Boolean = {
    val r1 = index.matches(tags)
    val r2 = index.matches(SmallHashMap(tags))
    assertEquals(r1, r2)
    r1
  }

  private def matchingEntries[T](index: QueryIndex[T], tags: Map[String, String]): List[T] = {
    val r1 = index.matchingEntries(tags).sortWith(_.toString < _.toString)
    val r2 = index.matchingEntries(SmallHashMap(tags)).sortWith(_.toString < _.toString)
    assertEquals(r1, r2)
    r1
  }

  test("empty") {
    val index = QueryIndex(Nil)
    assert(!matches(index, Map.empty))
    assert(!matches(index, Map("a" -> "1")))
  }

  test("matchingEntries empty") {
    val index = QueryIndex(Nil)
    assert(matchingEntries(index, Map.empty).isEmpty)
    assert(matchingEntries(index, Map("a" -> "1")).isEmpty)
  }

  test("single query: simple") {
    val q = Query.And(Query.Equal("a", "1"), Query.Equal("b", "2"))
    val index = QueryIndex(List(q))

    // Not all tags are present
    assert(!matches(index, Map.empty))
    assert(!matches(index, Map("a" -> "1")))

    // matches
    assert(matches(index, Map("a" -> "1", "b" -> "2")))
    assert(matches(index, Map("a" -> "1", "b" -> "2", "c" -> "3")))

    // a doesn't match
    assert(!matches(index, Map("a" -> "2", "b" -> "2", "c" -> "3")))

    // b doesn't match
    assert(!matches(index, Map("a" -> "1", "b" -> "3", "c" -> "3")))
  }

  test("matchingEntries single query: simple") {
    val q = Query.And(Query.Equal("a", "1"), Query.Equal("b", "2"))
    val index = QueryIndex(List(q))

    // Not all tags are present
    assert(matchingEntries(index, Map.empty).isEmpty)
    assert(matchingEntries(index, Map("a" -> "1")).isEmpty)

    // matches
    assertEquals(matchingEntries(index, Map("a" -> "1", "b" -> "2")), List(q))
    assertEquals(matchingEntries(index, Map("a" -> "1", "b" -> "2", "c" -> "3")), List(q))

    // a doesn't match
    assert(matchingEntries(index, Map("a" -> "2", "b" -> "2", "c" -> "3")).isEmpty)

    // b doesn't match
    assert(matchingEntries(index, Map("a" -> "1", "b" -> "3", "c" -> "3")).isEmpty)
  }

  test("single query: complex") {
    val q = Query.And(Query.And(Query.Equal("a", "1"), Query.Equal("b", "2")), Query.HasKey("c"))
    val index = QueryIndex(List(q))

    // Not all tags are present
    assert(!matches(index, Map.empty))
    assert(!matches(index, Map("a" -> "1")))
    assert(!matches(index, Map("a" -> "1", "b" -> "2")))

    // matches
    assert(matches(index, Map("a" -> "1", "b" -> "2", "c" -> "3")))

    // a doesn't match
    assert(!matches(index, Map("a" -> "2", "b" -> "2", "c" -> "3")))

    // b doesn't match
    assert(!matches(index, Map("a" -> "1", "b" -> "3", "c" -> "3")))
  }

  test("single query: in expansion is limited") {
    // If the :in clauses are fully expanded, then this will cause an OOM error because
    // of the combinatorial explosion of simple queries (10k * 10k * 10k).
    val q1 = Query.In("a", (0 until 10000).map(_.toString).toList)
    val q2 = Query.In("b", (0 until 10000).map(_.toString).toList)
    val q3 = Query.In("c", (0 until 10000).map(_.toString).toList)
    val q = Query.And(Query.And(q1, q2), q3)
    val index = QueryIndex(List(q))

    assert(matches(index, Map("a" -> "1", "b" -> "9999", "c" -> "727")))
    assert(!matches(index, Map("a" -> "1", "b" -> "10000", "c" -> "727")))
  }

  test("matchingEntries single query: complex") {
    val q = Query.And(Query.And(Query.Equal("a", "1"), Query.Equal("b", "2")), Query.HasKey("c"))
    val index = QueryIndex(List(q))

    // Not all tags are present
    assert(matchingEntries(index, Map.empty).isEmpty)
    assert(matchingEntries(index, Map("a" -> "1")).isEmpty)
    assert(matchingEntries(index, Map("a" -> "1", "b" -> "2")).isEmpty)

    // matchingEntries
    assertEquals(matchingEntries(index, Map("a" -> "1", "b" -> "2", "c" -> "3")), List(q))

    // a doesn't match
    assert(matchingEntries(index, Map("a" -> "2", "b" -> "2", "c" -> "3")).isEmpty)

    // b doesn't match
    assert(matchingEntries(index, Map("a" -> "1", "b" -> "3", "c" -> "3")).isEmpty)
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
    assert(!matches(index, Map.empty))
    assert(!matches(index, Map("a" -> "1")))

    // matches
    assert(matches(index, Map("name" -> "cpuUsage", "nf.node" -> "unknown")))
    assert(matches(index, Map("name" -> "cpuUsage", "nf.node" -> "i-00099")))
    assert(matches(index, Map("name" -> "diskUsage", "nf.node" -> "i-00099")))

    // shouldn't match
    assert(!matches(index, Map("name" -> "diskUsage", "nf.node" -> "unknown")))
    assert(!matches(index, Map("name" -> "memoryUsage", "nf.node" -> "i-00099")))
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
    assert(matchingEntries(index, Map.empty).isEmpty)
    assert(matchingEntries(index, Map("a" -> "1")).isEmpty)

    // matchingEntries
    assertEquals(
      matchingEntries(index, Map("name" -> "cpuUsage", "nf.node" -> "unknown")),
      List(cpuUsage)
    )
    assertEquals(
      matchingEntries(index, Map("name" -> "cpuUsage", "nf.node" -> "i-00099")),
      List(cpuUsage)
    )
    assertEquals(
      matchingEntries(index, Map("name" -> "diskUsage", "nf.node" -> "i-00099")),
      List(
        diskUsage,
        diskUsagePerNode.last
      )
    )
    assertEquals(
      matchingEntries(index, Map("name" -> "diskUsage", "nf.node" -> "unknown")),
      List(diskUsage)
    )

    // shouldn't match
    assert(matchingEntries(index, Map("name" -> "memoryUsage", "nf.node" -> "i-00099")).isEmpty)
  }

  test("from list of exprs") {
    val expr1: TimeSeriesExpr = DataExpr.Sum(Query.Equal("name", "cpuUsage"))
    val expr2: TimeSeriesExpr =
      MathExpr.Divide(expr1, DataExpr.Sum(Query.Equal("name", "numCores")))
    val entries = List(expr1, expr2).flatMap { expr =>
      expr.dataExprs.map { d =>
        QueryIndex.Entry(d.query, expr)
      }
    }
    val index = QueryIndex.create(entries)

    assertEquals(matchingEntries(index, Map("name" -> "cpuUsage")).toSet, Set(expr1, expr2))
    assertEquals(matchingEntries(index, Map("name" -> "numCores")).toSet, Set(expr2))
  }

  test("queries for both nf.app and nf.cluster") {
    val appQuery = Query.Equal("nf.app", "testapp")
    val clusterQuery = Query.Equal("nf.cluster", "testapp-test")
    val queries = List(appQuery, clusterQuery)
    val index = QueryIndex(queries)

    val tags = Map("nf.app" -> "testapp", "nf.cluster" -> "testapp-test")
    assert(matches(index, tags))
    assertEquals(matchingEntries(index, tags), queries)
  }

  test("queries for both nf.app w/ nf.cluster miss and nf.cluster") {
    val appQuery =
      Query.And(Query.Equal("nf.app", "testapp"), Query.Equal("nf.cluster", "testapp-miss"))
    val clusterQuery = Query.Equal("nf.cluster", "testapp-test")
    val queries = List(appQuery, clusterQuery)
    val index = QueryIndex(queries)

    val tags = Map("nf.app" -> "testapp", "nf.cluster" -> "testapp-test")
    assert(matches(index, tags))
    assertEquals(matchingEntries(index, tags), List(clusterQuery))
  }

  type QueryInterner = scala.collection.mutable.AnyRefMap[Query, Query]

  private def intern(interner: QueryInterner, query: Query): Query = {
    query match {
      case Query.True =>
        query
      case Query.False =>
        query
      case q: Query.Equal =>
        interner.getOrElseUpdate(q, Query.Equal(q.k.intern(), q.v.intern()))
      case q: Query.LessThan =>
        interner.getOrElseUpdate(q, Query.LessThan(q.k.intern(), q.v.intern()))
      case q: Query.LessThanEqual =>
        interner.getOrElseUpdate(q, Query.LessThanEqual(q.k.intern(), q.v.intern()))
      case q: Query.GreaterThan =>
        interner.getOrElseUpdate(q, Query.GreaterThan(q.k.intern(), q.v.intern()))
      case q: Query.GreaterThanEqual =>
        interner.getOrElseUpdate(q, Query.GreaterThanEqual(q.k.intern(), q.v.intern()))
      case q: Query.Regex =>
        interner.getOrElseUpdate(q, Query.Regex(q.k.intern(), q.v.intern()))
      case q: Query.RegexIgnoreCase =>
        interner.getOrElseUpdate(q, Query.RegexIgnoreCase(q.k.intern(), q.v.intern()))
      case q: Query.In =>
        interner.getOrElseUpdate(q, Query.In(q.k.intern(), q.vs.map(_.intern())))
      case q: Query.HasKey =>
        interner.getOrElseUpdate(q, Query.HasKey(q.k.intern()))
      case q: Query.And =>
        interner.getOrElseUpdate(q, Query.And(intern(interner, q.q1), intern(interner, q.q2)))
      case q: Query.Or =>
        interner.getOrElseUpdate(q, Query.Or(intern(interner, q.q1), intern(interner, q.q2)))
      case q: Query.Not =>
        interner.getOrElseUpdate(q, Query.Not(intern(interner, q.q)))
    }
  }

  private def parse(interner: QueryInterner, s: String): List[Query] = {
    try {
      val interpreter = Interpreter(StyleVocabulary.allWords)
      val queries = interpreter.execute(s).stack.collect {
        case ModelExtractors.PresentationType(t) =>
          t.expr.dataExprs.map(e => intern(interner, e.query))
      }
      queries.flatten.distinct
    } catch {
      case _: Exception => Nil
    }
  }

  test("memory".ignore) {
    val interner = new QueryInterner
    val queries = Using.resource(Streams.resource("queries.txt")) { in =>
      Streams.lines(in).toList.flatMap { u =>
        val uri = URI.create(u.replace("|", "%7C").replace("^", "%5E"))
        val qstring = uri.getRawQuery
        if (qstring == null) Nil
        else {
          qstring
            .split("&")
            .filter(_.startsWith("q="))
            .map(s => parse(interner, s.substring(2)))
        }
      }
    }

    val inputLayout = GraphLayout.parseInstance(queries)
    println("INPUT:")
    println(inputLayout.toFootprint)

    val index = QueryIndex(queries.flatten)
    val idxLayout = GraphLayout.parseInstance(index)
    println("INDEX:")
    println(idxLayout.toFootprint)
  }
}
