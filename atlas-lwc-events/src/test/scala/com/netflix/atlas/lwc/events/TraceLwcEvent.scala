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

  // Current span to be considered, defaults to the root span
  private var current: SpanData = root

  /** Invoke the function for each span in the trace as an LwcEvent. */
  def foreach(f: LwcEvent => Unit): Unit = {
    spans.foreach { span =>
      current = span
      f(this)
    }
  }

  override def rawEvent: Any = current

  override def timestamp: Long = current.getEndEpochNanos / 1_000_000L

  override def extractValue(key: String): Any = {
    if (key.startsWith("root."))
      extractValue(root, key.substring("root.".length))
    else if (key.startsWith("parent."))
      extractFromParent(current, key.substring("parent.".length))
    else
      extractValue(current, key)
  }

  @scala.annotation.tailrec
  private def extractFromParent(span: SpanData, key: String): Any = {
    spanIdMap.get(span.getParentSpanId) match {
      case Some(s) if key.startsWith("parent.") =>
        extractFromParent(s, key.substring("parent.".length))
      case Some(s) =>
        extractValue(s, key)
      case None =>
        null
    }
  }

  private def extractValue(span: SpanData, key: String): Any = {
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
