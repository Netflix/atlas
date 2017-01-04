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
import com.netflix.atlas.core.util.SmallHashMap

/**
 * Index for quickly matching a set of tags against many query expressions. The intended use-case
 * is for stream processing. If a stream of tagged data points are flowing through the system
 * and we have thousands of queries, then we need efficient ways to:
 *
 * 1. Check if a datapoint is a match to any of the queries. This can be used to quickly filter
 *    out data that isn't going to be needed.
 * 2. Figure out which queries or expressions match a given datapoint.
 *
 * @param indexes
 *     Map of :eq query to a sub-index. This is used to recursively search the set after finding
 *     the first match.
 * @param entries
 *     Entries that remain after checking all the simple :eq queries. This list will be searched
 *     using a linear scan to get final matching with regex or other more complicated query
 *     clauses.
 */
case class QueryIndex[T](
    indexes: Map[Query.Equal, QueryIndex[T]],
    entries: List[QueryIndex.Entry[T]]) {

  /** Returns true if the tags match any of the queries in the index. */
  def matches(tags: Map[String, String]): Boolean = {
    val qs = tags.map(t => Query.Equal(t._1, t._2)).toList
    matches(tags, qs)
  }

  private def matches(tags: Map[String, String], queries: List[Query.Equal]): Boolean = {
    queries match {
      case q :: qs =>
        val children = indexes.get(q) match {
          case Some(qt) => qt.matches(tags, qs)
          case None     => false
        }
        children || entries.exists(_.query.matches(tags)) || matches(tags, qs)
      case Nil =>
        entries.exists(_.query.matches(tags))
    }
  }

  /** Finds the set of items that match the provided tags. */
  def matchingEntries(tags: Map[String, String]): List[T] = {
    val qs = tags.map(t => Query.Equal(t._1, t._2)).toList
    matchingEntries(tags, qs).distinct
  }

  private def matchingEntries(tags: Map[String, String], queries: List[Query.Equal]): List[T] = {
    queries match {
      case q :: qs =>
        val children = indexes.get(q) match {
          case Some(qt) => qt.matchingEntries(tags, qs)
          case None     => Nil
        }
        children ::: entries.filter(_.query.matches(tags)).map(_.value) ::: matchingEntries(tags, qs)
      case Nil =>
        entries.filter(_.query.matches(tags)).map(_.value)
    }
  }

  /**
   * Creates a string representation of the index tree. Warning: this can be large if many queries
   * are indexed.
   */
  override def toString: String = {
    val buf = new StringBuilder
    append(buf, 0)
    buf.toString()
  }

  private def append(buf: StringBuilder, indent: Int): Unit = {
    val pad1 = "    " * indent
    val pad2 = "    " * (indent + 1)
    buf.append(pad1).append("children\n")
    indexes.foreach { case (k, child) =>
      buf.append(pad2).append(k).append('\n')
      child.append(buf, indent + 2)
    }
    buf.append(pad1).append("queries\n")
    entries.foreach { e => buf.append(pad2).append(e.query).append('\n') }
  }
}

/**
  * Helper for building an index.
  */
object QueryIndex {

  type IndexMap[T <: Any] = scala.collection.mutable.AnyRefMap[AnyRef, QueryIndex[T]]

  case class Entry[T](query: Query, value: T)

  private case class AnnotatedEntry[T](entry: Entry[T], filters: Set[Query.Equal]) {
    def toList: List[(Query.Equal, AnnotatedEntry[T])] = {
      filters.toList.map(q => q -> AnnotatedEntry(entry, filters - q))
    }
  }

  /**
   * Create an index based on a list of queries. The value for the entry will be the raw input
   * query.
   */
  def apply(queries: List[Query]): QueryIndex[Query] = {
    create(queries.map(q => Entry(q, q)))
  }

  /**
   * Create an index based on a list of entries.
   */
  def create[T](entries: List[Entry[T]]): QueryIndex[T] = {
    val annotated = entries.flatMap { entry =>
      val qs = Query.dnfList(entry.query).flatMap(split)
      qs.map(q => annotate(Entry(q, entry.value)))
    }
    val idxMap = new IndexMap[T]
    createImpl(idxMap, annotated)
  }

  /**
   * Recursively build the index.
   */
  private def createImpl[T](idxMap: IndexMap[T], entries: List[AnnotatedEntry[T]]): QueryIndex[T] = {
    idxMap.get(entries) match {
      case Some(idx) => idx
      case None =>
        val (children, leaf) = entries.partition(_.filters.nonEmpty)
        val trees = children.flatMap(_.toList).groupBy(_._1).map { case (q, ts) =>
          q -> createImpl(idxMap, ts.map(_._2))
        }
        val idx = QueryIndex(smallMap(trees), leaf.map(_.entry))
        idxMap += entries -> idx
        idx
    }
  }

  /**
    * Convert to a SmallHashMap to get a more compact memory representation.
    */
  private def smallMap[T](m: Map[Query.Equal, QueryIndex[T]]): Map[Query.Equal, QueryIndex[T]] = {
    // Scala special cases immutable maps with size <= 4, so go ahead and keep those
    if (m.size <= 4) m else {
      // Otherwise, convert to a SmallHashMap. Note that default apply will create a
      // map with the exact size of the input to optimize for memory use. This results
      // in terrible performance for lookups of items that are not in the map because
      // the entire array has to be scanned.
      //
      // In this case we use the builder and give 2x the size of the input so there
      // will be 50% unused entries. Since we expect many misses this gives us better
      // performance and memory overhead isn't too bad. It is still much lower than
      // default immutable map since we don't need entry objects.
      val size = m.size * 2
      val builder = new SmallHashMap.Builder[Query.Equal, QueryIndex[T]](size)
      builder.addAll(m)
      val sm = builder.result
      sm
    }
  }

  /**
   * Split :in queries into a list of queries using :eq.
   */
  private def split(query: Query): List[Query] = {
    query match {
      case Query.And(q1, q2) => for (a <- split(q1); b <- split(q2)) yield { Query.And(a, b) }
      case Query.In(k, vs)   => vs.map { v => Query.Equal(k, v) }
      case _                 => List(query)
    }
  }

  /**
   * Convert a query into a list of query clauses that are ANDd together.
   */
  private def conjunctionList(query: Query): List[Query] = {
    query match {
      case Query.And(q1, q2)     => conjunctionList(q1) ::: conjunctionList(q2)
      case q                     => List(q)
    }
  }

  /**
   * Annotate an entry with a set of :eq queries that should filter in the input before checking
   * against the final remaining query. Ideally if the query is only using :eq and :and the final
   * remainder will be :true.
   */
  private def annotate[T](entry: Entry[T]): AnnotatedEntry[T] = {
    val distinct = conjunctionList(entry.query).distinct
    val filters   = distinct.collect { case q: Query.Equal => q }
    val remainder = distinct.collect { case q if !q.isInstanceOf[Query.Equal] => q }
    val remainderQ = if (remainder.isEmpty) Query.True else remainder.reduce { (a, b) => Query.And(a, b) }
    AnnotatedEntry(Entry(remainderQ, entry.value), filters.toSet)
  }
}
