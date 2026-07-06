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

/**
  * Immutable map view over the entries of `underlying` whose key matches the predicate `p`,
  * equivalent to `underlying.filter(e => p(e._1))` but without materializing a new map. A
  * predicate is used rather than a key set so the test is pluggable (e.g. a cheaper matcher)
  * and to avoid capturing a collection.
  *
  * Intended for filtering tag maps during evaluation, where the filtered map is typically
  * accessed a bounded number of times. Replacing the allocation of a new map with a thin
  * wrapper is a net win in that case, at the cost of slightly more expensive `get`/`foreach`.
  *
  * The predicate must be a pure, deterministic function of the key. The size is computed once
  * and cached, so a predicate whose result changes between calls would make `size` disagree
  * with `iterator`/`foreachEntry`.
  *
  * The view implements enough of the Map contract to be correct for those access patterns.
  * In particular `equals`, `hashCode`, and the id computed via `TaggedItem.computeId` match
  * the materialized filtered map. `updated` and `removed` materialize a concrete map since
  * they are not on the hot path.
  */
final class FilteredMap(underlying: Map[String, String], p: String => Boolean)
    extends scala.collection.immutable.Map[String, String] {

  override def get(key: String): Option[String] =
    if (p(key)) underlying.get(key) else None

  override def getOrElse[V1 >: String](key: String, default: => V1): V1 =
    if (p(key)) underlying.getOrElse(key, default) else default

  override def contains(key: String): Boolean =
    p(key) && underlying.contains(key)

  override def iterator: Iterator[(String, String)] =
    underlying.iterator.filter(e => p(e._1))

  override def foreachEntry[U](f: (String, String) => U): Unit =
    underlying.foreachEntry((k, v) => if (p(k)) f(k, v))

  // Cached so callers that read size more than once (e.g. ItemIdCalculator) do not recompute
  // it via a fresh closure each time. Lazy so building the view stays cheap if size is unused.
  override lazy val size: Int = {
    var n = 0
    underlying.foreachEntry((k, _) => if (p(k)) n += 1)
    n
  }

  override def removed(key: String): Map[String, String] =
    iterator.toMap.removed(key)

  override def updated[V1 >: String](key: String, value: V1): Map[String, V1] =
    iterator.toMap.updated(key, value)
}
