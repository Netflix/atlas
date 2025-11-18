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
package com.netflix.atlas.lwc.events

import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.util.Strings

import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime

/**
  * Event filter that supports typed dimensions with custom comparison logic.
  * Splits queries into indexed and post-filter stages, where typed dimensions
  * are evaluated in the post-filter stage using type-specific matchers.
  */
abstract class TypedLwcEventFilter extends LwcEventFilter {

  /**
    * Map of dimension names to their type matchers. Dimensions in this map
    * will be filtered in the post-filter stage using the associated type matcher.
    */
  def typedDimensions: Map[String, TypedLwcEventFilter.TypeMatcher]

  /**
    * Indicates the subclass will handle the matching logic with `customMatches`. */
  def isCustom(key: String): Boolean = false

  private def isSpecial(key: String): Boolean = {
    typedDimensions.contains(key) || isCustom(key)
  }

  private def removeValueKey(query: Query): Query = {
    val q = query
      .rewrite {
        case kq: Query.KeyQuery if kq.k == valueDimension => Query.True
      }
      .asInstanceOf[Query]
    Query.simplify(q, ignore = true)
  }

  private def computeIndexQuery(query: Query): Query = {
    val q = query
      .rewrite {
        case kq: Query.KeyValueQuery if isSpecial(kq.k) => Query.True
      }
      .asInstanceOf[Query]
    Query.simplify(q, ignore = true)
  }

  private def computePostFilterQuery(query: Query): Query = {
    val q = query
      .rewrite {
        case kq: Query.KeyValueQuery if !isSpecial(kq.k) => Query.True
        case q: Query.Or                                 => q
        case q: Query.Not                                => q
      }
      .asInstanceOf[Query]
    Query.simplify(q, ignore = true)
  }

  override def splitQuery(query: Query): LwcEventFilter.Queries = {
    val q = removeValueKey(query)
    LwcEventFilter.Queries(computeIndexQuery(q), computePostFilterQuery(q))
  }

  private def checkKV(event: LwcEvent, k: String, v: String, cmp: Int => Boolean): Boolean = {
    typedDimensions.get(k) match {
      case Some(matcher) =>
        val v1 = matcher.cast(event.extractValueSafe(k))
        val v2 = matcher.cast(v)
        v1 != null && v2 != null && cmp(matcher.compare(v1, v2))
      case None =>
        val v1 = event.tagValue(k)
        val v2 = v
        v1 != null && v2 != null && cmp(v1.compareTo(v2))
    }
  }

  private def checkIn(event: LwcEvent, k: String, vs: List[String]): Boolean = {
    typedDimensions.get(k) match {
      case Some(matcher) =>
        val v1 = matcher.cast(event.extractValueSafe(k))
        val vs2 = vs.map(matcher.cast).filterNot(_ == null)
        v1 != null && vs2.exists(v2 => matcher.compare(v1, v2) == 0)
      case None =>
        val v1 = event.tagValue(k)
        v1 != null && vs.contains(v1)
    }
  }

  private def checkPattern(event: LwcEvent, q: Query.PatternQuery): Boolean = {
    typedDimensions.get(q.k) match {
      case Some(_) => false
      case None    => q.check(event.tagValue(q.k))
    }
  }

  /**
    * Overridden by a subclass to provide custom matching logic for some keys.
    *
    * @param event
    *     The event to check
    * @param query
    *     Query to check against the event
    * @return
    *     True if the event matches the query
    */
  def customMatches(event: LwcEvent, query: Query.KeyQuery): Boolean = false

  override def matches(event: LwcEvent, postFilterQuery: Query): Boolean = {
    postFilterQuery match {
      case Query.False                        => false
      case Query.True                         => true
      case q: Query.KeyQuery if isCustom(q.k) => customMatches(event, q)
      case q: Query.HasKey                    => event.extractValueSafe(q.k) != null
      case q: Query.Equal                     => checkKV(event, q.k, q.v, c => c == 0)
      case q: Query.LessThan                  => checkKV(event, q.k, q.v, c => c < 0)
      case q: Query.LessThanEqual             => checkKV(event, q.k, q.v, c => c <= 0)
      case q: Query.GreaterThan               => checkKV(event, q.k, q.v, c => c > 0)
      case q: Query.GreaterThanEqual          => checkKV(event, q.k, q.v, c => c >= 0)
      case q: Query.In                        => checkIn(event, q.k, q.vs)
      case q: Query.Regex                     => checkPattern(event, q)
      case q: Query.RegexIgnoreCase           => checkPattern(event, q)
      case q: Query.And                       => matches(event, q.q1) && matches(event, q.q2)
      case q: Query.Or                        => matches(event, q.q1) || matches(event, q.q2)
      case q: Query.Not                       => !matches(event, q.q)
    }
  }
}

object TypedLwcEventFilter {

  /**
    * Creates a TypedLwcEventFilter with the specified typed dimensions.
    *
    * @param dimensions
    *     Map of dimension names to type matchers
    * @param valueKey
    *     The dimension to use for projecting values from the event
    * @return
    *     A new TypedLwcEventFilter instance
    */
  def apply(
    dimensions: Map[String, TypeMatcher],
    valueKey: String = "value"
  ): TypedLwcEventFilter = {
    new TypedLwcEventFilter {
      override def valueDimension: String = valueKey

      override def typedDimensions: Map[String, TypeMatcher] = dimensions
    }
  }

  /**
    * Trait for matching and comparing typed dimension values.
    */
  trait TypeMatcher {

    /**
      * Cast a value to the appropriate type for comparison.
      *
      * @param value
      *     Value to cast
      * @return
      *     Casted value, or null if the value cannot be cast
      */
    def cast(value: Any): Any

    /**
      * Compare two values of the same type.
      *
      * @param v1
      *     First value to compare
      * @param v2
      *     Second value to compare
      * @return
      *     Negative if v1 < v2, zero if v1 == v2, positive if v1 > v2
      */
    def compare(v1: Any, v2: Any): Int
  }

  /**
    * Type matcher for long integer values.
    */
  object LongMatcher extends TypeMatcher {

    override def cast(value: Any): Any = value match {
      case v: String => v.toLong
      case v: Byte   => v.toLong
      case v: Short  => v.toLong
      case v: Int    => v.toLong
      case v: Long   => v
      case v: Number => v.longValue()
      case _         => null
    }

    override def compare(v1: Any, v2: Any): Int = {
      val i1 = v1.asInstanceOf[Long]
      val i2 = v2.asInstanceOf[Long]
      java.lang.Long.compare(i1, i2)
    }
  }

  /**
    * Type matcher for double-precision floating point values.
    */
  object DoubleMatcher extends TypeMatcher {

    override def cast(value: Any): Any = value match {
      case v: String => v.toDouble
      case v: Byte   => v.toDouble
      case v: Short  => v.toDouble
      case v: Int    => v.toDouble
      case v: Long   => v.toDouble
      case v: Float  => v.toDouble
      case v: Double => v
      case v: Number => v.doubleValue()
      case _         => null
    }

    override def compare(v1: Any, v2: Any): Int = {
      val d1 = v1.asInstanceOf[Double]
      val d2 = v2.asInstanceOf[Double]
      java.lang.Double.compare(d1, d2)
    }
  }

  /**
    * Type matcher for duration values. Supports parsing duration strings and
    * converting numeric values as nanoseconds.
    */
  object DurationMatcher extends TypeMatcher {

    override def cast(value: Any): Any = value match {
      case v: String   => Strings.parseDuration(v)
      case v: Int      => Duration.ofNanos(v)
      case v: Long     => Duration.ofNanos(v)
      case v: Number   => Duration.ofNanos(v.longValue())
      case v: Duration => v
      case _           => null
    }

    override def compare(v1: Any, v2: Any): Int = {
      val d1 = v1.asInstanceOf[Duration]
      val d2 = v2.asInstanceOf[Duration]
      d1.compareTo(d2)
    }
  }

  /**
    * Type matcher for instant (timestamp) values. Supports parsing date strings
    * and converting numeric values as epoch milliseconds.
    */
  object InstantMatcher extends TypeMatcher {

    override def cast(value: Any): Any = value match {
      case v: String        => Strings.parseDate(v).toInstant
      case v: ZonedDateTime => v.toInstant
      case v: Instant       => v
      case v: Int           => Strings.ofEpoch(v, ZoneOffset.UTC).toInstant
      case v: Long          => Strings.ofEpoch(v, ZoneOffset.UTC).toInstant
      case v: Number        => Strings.ofEpoch(v.longValue(), ZoneOffset.UTC).toInstant
      case _                => null
    }

    override def compare(v1: Any, v2: Any): Int = {
      val i1 = v1.asInstanceOf[Instant]
      val i2 = v2.asInstanceOf[Instant]
      i1.compareTo(i2)
    }
  }
}
