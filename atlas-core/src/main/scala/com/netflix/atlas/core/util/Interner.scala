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

import com.github.benmanes.caffeine.cache.Caffeine

import java.util.concurrent.atomic.AtomicReference

/** Helper functions for interners. */
object Interner {

  private val stringInternerRef = new AtomicReference[Interner[String]](StringInterner)

  /** Set the default interner to use for strings. */
  def setStringInterner(interner: Interner[String]): Unit = {
    stringInternerRef.set(interner)
  }

  /** Returns the default interner to use for strings. */
  def forStrings: Interner[String] = {
    stringInternerRef.get
  }
}

/**
  * Keeps track of canonical references for a type of object. Typically used to reduce memory
  * overhead if an application potentially creates many copies of equal objects and will need to
  * keep them around for some period of time.
  */
trait Interner[T] {

  /**
    * Returns a representative instance that is equal to the specified instance. Check the
    * particular implementation before making any assumptions about the ability to just use
    * reference equality on interned instances.
    */
  def intern(value: T): T

  /** Returns the number of items that are currently interned. */
  def size: Int
}

/**
  * Does nothing, the original value will always be returned.
  */
class NoopInterner[T] extends Interner[T] {

  def intern(value: T): T = value

  def size: Int = 0
}

/**
  * Interner that starts with a predefined set of values. If a request is made to intern a value
  * that is not in the predefined list, then it will be forwarded to the specified fallback
  * interner.
  */
class PredefinedInterner[T](predefValues: Seq[T], fallback: Interner[T]) extends Interner[T] {

  // Java hash map seems to be a bit faster on average for our tested use-cases, need to
  // re-evaluate more thoroughly at some point...
  private val predef = {
    val m = new java.util.HashMap[T, T](predefValues.size)
    predefValues.foreach { v =>
      m.put(v, v)
    }
    m
  }

  def isPredefined(value: T): Boolean = predef.containsKey(value)

  def intern(value: T): T = {
    val v = predef.get(value)
    if (v == null) fallback.intern(value) else v
  }

  def size: Int = predef.size + fallback.size
}

/** Delegate to `String.intern()`. */
object StringInterner extends Interner[String] {

  def intern(value: String): String = value.intern

  def size: Int = 0
}

/**
  * Interner based on a Caffeine cache. This implementation should only be used
  * for memory reduction and not reference equality. If the cache is full, then
  * some values will not be deduped.
  *
  * @param maxSize
  *     Maximum number of strings to intern.
  */
class CaffeineInterner[T](maxSize: Int) extends Interner[T] {

  private val cache = Caffeine
    .newBuilder()
    .maximumSize(maxSize)
    .build[T, T]()

  def intern(value: T): T = {
    cache.get(value, v => v)
  }

  def size: Int = cache.estimatedSize().toInt
}
