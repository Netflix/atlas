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
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.SpanContext
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.TraceFlags
import io.opentelemetry.api.trace.TraceState
import io.opentelemetry.sdk.testing.trace.TestSpanData
import io.opentelemetry.sdk.trace.data.SpanData
import io.opentelemetry.sdk.trace.data.StatusData

import java.time.Duration
import scala.util.Random

/**
  * Helper to convert OTEL spans to LwcEvent for tests.
  */
class TraceLwcEvent(spans: Seq[SpanData]) extends LwcEvent {

  import TraceLwcEvent.*

  require(spans.size > 1, "must have at least one span")

  // Map to lookup parent by id
  private val spanIdMap: Map[String, SpanData] = {
    spans.map(s => s.getSpanId -> s).toMap
  }

  // Root span is the first where the parentId is not present in the set of
  // spans. For full traces it should be "0000000000000000", but for partial traces
  // may be a valid span id for a span that wasn't sampled.
  private val root: SpanData = {
    spans.find(s => !spanIdMap.contains(s.getParentSpanId)).getOrElse {
      spans.head
    }
  }

  /** Invoke the function for each span in the trace as an LwcEvent. */
  def foreach(f: LwcEvent => Unit): Unit = {
    val spanEvent = new SpanLwcEvent(this, null)
    spans.foreach { span =>
      spanEvent.current = span
      f(spanEvent)
    }
  }

  /** Checks if there exists an span in the trace that matches the predicate. */
  def exists(f: LwcEvent => Boolean): Boolean = {
    val spanEvent = new SpanLwcEvent(this, null)
    spans.exists { span =>
      spanEvent.current = span
      f(spanEvent)
    }
  }

  override def rawEvent: Any = root

  override def timestamp: Long = root.getEndEpochNanos / 1_000_000L

  override def extractValue(key: String): Any = extractValue(root, key)

  private def extractValue(span: SpanData, key: String): Any = {
    if (key.startsWith("root."))
      extractValueImpl(root, key.substring("root.".length))
    else if (key.startsWith("parent."))
      extractFromParent(span, key.substring("parent.".length))
    else
      extractValueImpl(span, key)
  }

  @scala.annotation.tailrec
  private def extractFromParent(span: SpanData, key: String): Any = {
    spanIdMap.get(span.getParentSpanId) match {
      case Some(s) if key.startsWith("parent.") =>
        extractFromParent(s, key.substring("parent.".length))
      case Some(s) =>
        extractValueImpl(s, key)
      case None =>
        null
    }
  }

  private def extractValueImpl(span: SpanData, key: String): Any = {
    key match {
      case "name"     => span.getName
      case "spanId"   => span.getSpanId
      case "traceId"  => span.getTraceId
      case "kind"     => span.getKind
      case "status"   => span.getStatus.getStatusCode
      case "duration" => Duration.ofNanos(span.getEndEpochNanos - span.getStartEpochNanos)
      case k          => span.getAttributes.get(AttributeKey.stringKey(k))
    }
  }
}

object TraceLwcEvent {

  /**
    * Helper that wraps a trace event to represent an event for a span. Used as part
    * of operations that iterate over the spans in the trace.
    */
  private class SpanLwcEvent(val traceEvent: TraceLwcEvent, var current: SpanData)
      extends LwcEvent {

    override def rawEvent: Any = current

    override def timestamp: Long = current.getEndEpochNanos / 1_000_000L

    override def extractValue(key: String): Any = traceEvent.extractValue(current, key)
  }

  /**
    * Sample event filter to allow some custom post filter handling. Supports duration
    * match on the spans and adds support for "any." prefix. Note, the "any." prefix
    * would require a linear scan over all the spans for each index query that matches
    * and thus could be quite expensive.
    */
  object TraceLwcEventFilter extends TypedLwcEventFilter {

    override val typedDimensions: Map[String, TypedLwcEventFilter.TypeMatcher] =
      Map("duration" -> TypedLwcEventFilter.DurationMatcher)

    override def valueDimension: String = "value"

    override def isCustom(key: String): Boolean = key.startsWith("any.")

    private def fixKey(query: Query.KeyQuery): Query.KeyQuery = {
      val k = query.k.substring("any.".length)
      query match {
        case Query.HasKey(_)              => Query.HasKey(k)
        case Query.Equal(_, v)            => Query.Equal(k, v)
        case Query.LessThan(_, v)         => Query.LessThan(k, v)
        case Query.LessThanEqual(_, v)    => Query.LessThanEqual(k, v)
        case Query.GreaterThan(_, v)      => Query.GreaterThan(k, v)
        case Query.GreaterThanEqual(_, v) => Query.GreaterThanEqual(k, v)
        case Query.In(_, vs)              => Query.In(k, vs)
        case Query.Regex(_, v)            => Query.Regex(k, v)
        case Query.RegexIgnoreCase(_, v)  => Query.RegexIgnoreCase(k, v)
      }
    }

    private def check(event: TraceLwcEvent, query: Query.KeyQuery): Boolean = {
      val q = fixKey(query)
      event.exists(e => matches(e, q))
    }

    override def customMatches(event: LwcEvent, query: Query.KeyQuery): Boolean = {
      event match {
        case e: TraceLwcEvent if isCustom(query.k) => check(e, query)
        case e: SpanLwcEvent if isCustom(query.k)  => check(e.traceEvent, query)
        case _                                     => false
      }
    }
  }

  /**
    * Rewrite keys for the query to use parent instead of child. Child is just a convenience
    * but can be rewritten to use parent that can be quickly looked up from the span.
    */
  def rewriteToParentQuery(query: Query): Query = {
    val keys = Query.allKeys(query)
    val depths = keys.map(k => k -> computeDepth(k)).toMap
    val min = depths.values.min
    val newKeys = depths.map(t => t._1 -> computeNewKey(t._1, t._2 - min))
    query
      .rewrite {
        case q: Query.Equal            => q.copy(k = newKeys(q.k))
        case q: Query.In               => q.copy(k = newKeys(q.k))
        case q: Query.Regex            => q.copy(k = newKeys(q.k))
        case q: Query.RegexIgnoreCase  => q.copy(k = newKeys(q.k))
        case q: Query.LessThan         => q.copy(k = newKeys(q.k))
        case q: Query.LessThanEqual    => q.copy(k = newKeys(q.k))
        case q: Query.GreaterThan      => q.copy(k = newKeys(q.k))
        case q: Query.GreaterThanEqual => q.copy(k = newKeys(q.k))
        case q: Query.HasKey           => q.copy(k = newKeys(q.k))
      }
      .asInstanceOf[Query]
  }

  private def computeDepth(key: String): Int = {
    if (key.startsWith("child."))
      computeDepth(key.substring("child.".length)) - 1
    else if (key.startsWith("parent."))
      computeDepth(key.substring("parent.".length)) + 1
    else
      0
  }

  @scala.annotation.tailrec
  private def stripPrefix(key: String): String = {
    if (key.startsWith("child."))
      stripPrefix(key.substring("child.".length))
    else if (key.startsWith("parent."))
      stripPrefix(key.substring("parent.".length))
    else
      key
  }

  private def computeNewKey(key: String, depth: Int): String = {
    val prefix = "parent." * depth
    s"$prefix${stripPrefix(key)}"
  }

  private def attrs(tags: (String, String)*): Attributes = {
    val builder = Attributes.builder()
    tags.foreach {
      case (k, v) => builder.put(k, v)
    }
    builder.build()
  }

  private def spanContext(id: String): SpanContext = {
    SpanContext.create("12345678" * 4, id, TraceFlags.getDefault, TraceState.getDefault)
  }

  // A
  // ├── B
  // │   ├── E
  // │   ├── E
  // │   ├── E (error)
  // │   └── F
  // ├── C
  // │   ├── E
  // │   └── E
  // └── D
  def sampleTrace: TraceLwcEvent = {
    val spans = List(
      TestSpanData
        .builder()
        .setName("A")
        .setKind(SpanKind.SERVER)
        .setSpanContext(spanContext("a" * 16))
        .setStartEpochNanos(0L)
        .setEndEpochNanos(100L)
        .setHasEnded(true)
        .setStatus(StatusData.ok())
        .setAttributes(attrs("app" -> "a"))
        .build(),
      TestSpanData
        .builder()
        .setName("B")
        .setKind(SpanKind.SERVER)
        .setSpanContext(spanContext("b" * 16))
        .setParentSpanContext(spanContext("a" * 16))
        .setStartEpochNanos(5L)
        .setEndEpochNanos(80L)
        .setHasEnded(true)
        .setStatus(StatusData.ok())
        .setAttributes(attrs("app" -> "b"))
        .build(),
      TestSpanData
        .builder()
        .setName("C")
        .setKind(SpanKind.SERVER)
        .setSpanContext(spanContext("c" * 16))
        .setParentSpanContext(spanContext("a" * 16))
        .setStartEpochNanos(5L)
        .setEndEpochNanos(10L)
        .setHasEnded(true)
        .setStatus(StatusData.ok())
        .setAttributes(attrs("app" -> "c"))
        .build(),
      TestSpanData
        .builder()
        .setName("D")
        .setKind(SpanKind.SERVER)
        .setSpanContext(spanContext("d" * 16))
        .setParentSpanContext(spanContext("a" * 16))
        .setStartEpochNanos(10L)
        .setEndEpochNanos(20L)
        .setHasEnded(true)
        .setStatus(StatusData.ok())
        .setAttributes(attrs("app" -> "d"))
        .build(),
      TestSpanData
        .builder()
        .setName("E")
        .setKind(SpanKind.SERVER)
        .setSpanContext(spanContext("e" * 16))
        .setParentSpanContext(spanContext("b" * 16))
        .setStartEpochNanos(6L)
        .setEndEpochNanos(15L)
        .setHasEnded(true)
        .setStatus(StatusData.ok())
        .setAttributes(attrs("app" -> "e", "part" -> "1"))
        .build(),
      TestSpanData
        .builder()
        .setName("E")
        .setKind(SpanKind.SERVER)
        .setSpanContext(spanContext("e" * 16))
        .setParentSpanContext(spanContext("b" * 16))
        .setStartEpochNanos(6L)
        .setEndEpochNanos(15L)
        .setHasEnded(true)
        .setStatus(StatusData.ok())
        .setAttributes(attrs("app" -> "e", "part" -> "2"))
        .build(),
      TestSpanData
        .builder()
        .setName("E")
        .setKind(SpanKind.SERVER)
        .setSpanContext(spanContext("e" * 16))
        .setParentSpanContext(spanContext("b" * 16))
        .setStartEpochNanos(6L)
        .setEndEpochNanos(75L)
        .setHasEnded(true)
        .setStatus(StatusData.error())
        .setAttributes(attrs("app" -> "e", "part" -> "3"))
        .build(),
      TestSpanData
        .builder()
        .setName("E")
        .setKind(SpanKind.SERVER)
        .setSpanContext(spanContext("e" * 16))
        .setParentSpanContext(spanContext("c" * 16))
        .setStartEpochNanos(6L)
        .setEndEpochNanos(15L)
        .setHasEnded(true)
        .setStatus(StatusData.ok())
        .setAttributes(attrs("app" -> "e", "part" -> "4"))
        .build(),
      TestSpanData
        .builder()
        .setName("E")
        .setKind(SpanKind.SERVER)
        .setSpanContext(spanContext("e" * 16))
        .setParentSpanContext(spanContext("c" * 16))
        .setStartEpochNanos(6L)
        .setEndEpochNanos(15L)
        .setHasEnded(true)
        .setStatus(StatusData.ok())
        .setAttributes(attrs("app" -> "e", "part" -> "5"))
        .build(),
      TestSpanData
        .builder()
        .setName("F")
        .setKind(SpanKind.SERVER)
        .setSpanContext(spanContext("f" * 16))
        .setParentSpanContext(spanContext("b" * 16))
        .setStartEpochNanos(6L)
        .setEndEpochNanos(15L)
        .setHasEnded(true)
        .setStatus(StatusData.ok())
        .setAttributes(attrs("app" -> "f"))
        .build()
    )
    // Randomly order the list to check it constructs the trace graph correctly
    // regardless of order
    new TraceLwcEvent(Random.shuffle(spans))
  }
}
