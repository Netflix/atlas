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

/**
  * Wraps the java IdentityHashMap as an immutable scala Map. Modifications will result
  * in a copy of the wrapped map being created and used with the new instance.
  */
class IdentityMap[K <: AnyRef, V] private (jmap: java.util.IdentityHashMap[K, V])
    extends scala.collection.immutable.Map[K, V] {

  override def get(key: K): Option[V] = Option(jmap.get(key))

  override def iterator: Iterator[(K, V)] = {
    val iter = jmap.entrySet().iterator()
    new Iterator[(K, V)] {
      override def hasNext: Boolean = iter.hasNext
      override def next(): (K, V) = {
        val entry = iter.next()
        entry.getKey -> entry.getValue
      }
    }
  }

  override def foreachEntry[U](f: (K, V) => U): Unit = {
    jmap.forEach((k, v) => f(k, v))
  }

  override def updated[V1 >: V](k: K, v: V1): IdentityMap[K, V1] = {
    val copy = new java.util.IdentityHashMap[K, V1](jmap)
    copy.put(k, v)
    new IdentityMap(copy)
  }

  override def removed(key: K): IdentityMap[K, V] = {
    val copy = new java.util.IdentityHashMap[K, V](jmap)
    copy.remove(key)
    new IdentityMap(copy)
  }

  override def concat[V2 >: V](suffix: IterableOnce[(K, V2)]): IdentityMap[K, V2] = {
    val copy = new java.util.IdentityHashMap[K, V2](jmap)
    suffix.iterator.foreach {
      case (k, v) => copy.put(k, v)
    }
    new IdentityMap(copy)
  }

  override def toString: String = {
    iterator.mkString("IdentityMap(", ", ", ")")
  }
}

object IdentityMap {

  private lazy val emptyMap = {
    new IdentityMap[AnyRef, Any](new java.util.IdentityHashMap[AnyRef, Any]())
  }

  def empty[K <: AnyRef, V]: IdentityMap[K, V] = emptyMap.asInstanceOf[IdentityMap[K, V]]

  def apply[K <: AnyRef, V](jmap: java.util.IdentityHashMap[K, V]): IdentityMap[K, V] = {
    val copy = new java.util.IdentityHashMap[K, V](jmap)
    new IdentityMap[K, V](copy)
  }

  def apply[K <: AnyRef, V](map: Map[K, V]): IdentityMap[K, V] = {
    val copy = new java.util.IdentityHashMap[K, V]()
    map.foreachEntry(copy.put)
    new IdentityMap[K, V](copy)
  }

  def apply[K <: AnyRef, V](vs: (K, V)*): IdentityMap[K, V] = {
    val copy = new java.util.IdentityHashMap[K, V]()
    vs.foreach { t =>
      copy.put(t._1, t._2)
    }
    new IdentityMap[K, V](copy)
  }
}
