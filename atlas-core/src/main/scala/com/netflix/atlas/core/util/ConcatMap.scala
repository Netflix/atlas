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
  * Immutable map view over the concatenation of two maps, equivalent to `a ++ b` but without
  * materializing a new map. Entries in `b` take precedence over entries in `a` with the same
  * key.
  *
  * Intended for combining tag maps during evaluation, where the combined map is typically
  * accessed a bounded number of times. Replacing the allocation of a new map with a thin
  * wrapper is a net win in that case, at the cost of slightly more expensive `get`/`foreach`.
  *
  * The view implements enough of the Map contract to be correct for those access patterns.
  * In particular `equals`, `hashCode`, and the id computed via `TaggedItem.computeId` match
  * the materialized `a ++ b`. `updated` and `removed` materialize a concrete map since they
  * are not on the hot path.
  */
final class ConcatMap(a: Map[String, String], b: Map[String, String])
    extends scala.collection.immutable.Map[String, String] {

  override def get(key: String): Option[String] = {
    val v = b.get(key)
    if (v.isDefined) v else a.get(key)
  }

  // Avoid the Option allocation on the lookup hot path (e.g. query matching). Delegates to
  // the underlying maps' getOrElse, which is allocation free for the standard immutable Map
  // implementations. `contains` is used to decide which side wins rather than a null sentinel
  // so that a key mapped to a null value in `b` is still treated as present (matching `a ++ b`).
  override def getOrElse[V1 >: String](key: String, default: => V1): V1 = {
    if (b.contains(key)) b.getOrElse(key, default) else a.getOrElse(key, default)
  }

  override def contains(key: String): Boolean = b.contains(key) || a.contains(key)

  override def iterator: Iterator[(String, String)] = {
    b.iterator ++ a.iterator.filter(e => !b.contains(e._1))
  }

  override def foreachEntry[U](f: (String, String) => U): Unit = {
    b.foreachEntry(f)
    a.foreachEntry((k, v) => if (!b.contains(k)) f(k, v))
  }

  // Cached so callers that read size more than once (e.g. ItemIdCalculator) do not recompute
  // it via a fresh closure each time. Lazy so building the view stays cheap if size is unused.
  override lazy val size: Int = {
    var n = b.size
    a.foreachEntry((k, _) => if (!b.contains(k)) n += 1)
    n
  }

  override def removed(key: String): Map[String, String] =
    iterator.toMap.removed(key)

  override def updated[V1 >: String](key: String, value: V1): Map[String, V1] =
    iterator.toMap.updated(key, value)
}
