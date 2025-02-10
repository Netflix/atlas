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

import com.fasterxml.jackson.core.JsonGenerator
import com.netflix.atlas.json.Json
import com.netflix.spectator.api.Spectator
import org.slf4j.LoggerFactory

import java.io.StringWriter
import scala.util.Using

/**
  * Represents an event that should be published via an LWC stream. Defines how to
  * extract values and encode the raw event object.
  */
trait LwcEvent {

  /** Raw event object that is being considered. */
  def rawEvent: Any

  /**
    * Timestamp for the event in Unix epoch milliseconds. If it is an event such as
    * a span that has a start and end, the timestamp should be for the end of the event.
    */
  def timestamp: Long

  /**
    * Value to use for the event when mapping to a time series. By default it will be
    * 1.0 same as incrementing a counter by 1.
    */
  def value: Any = 1.0

  /**
    * Extract a tag value for a given key. Returns `null` if there is no value for
    * the key or the value is not a string. By default it will delegate to `extractValue`
    * to ensure the two are consistent.
    */
  def tagValue(key: String): String = {
    extractValueSafe(key) match {
      case v: String  => v
      case e: Enum[_] => e.name()
      case _          => null
    }
  }

  /**
    * Extract a value from the raw event for a given key. This method should be consistent
    * with the `tagValue` method for keys that can be considered tags.
    */
  def extractValue(key: String): Any

  /**
    * Internal method for extracting the value that handles exceptions if any. If an exception
    * is thrown, then the value will be treated as `null`. The underlying exception will be
    * logged at trace level as it can be quite noisy if it is a common pattern across events.
    */
  private[events] final def extractValueSafe(key: String): Any = {
    try {
      extractValue(key)
    } catch {
      case e: Exception =>
        LoggerFactory.getLogger(getClass).trace(s"failed to extract value for key: $key", e)
        Spectator
          .globalRegistry()
          .counter("lwc.extractValueFailures", "error", e.getClass.getSimpleName)
          .increment()
        null
    }
  }

  /** Encode the raw event as JSON. */
  def encode(gen: JsonGenerator): Unit = {
    Json.encode(gen, rawEvent)
  }

  /**
    * Encode the raw event as an array representing a row in a table.
    *
    * @param columns
    *     Keys to use with `extractValue` for selecting the value of the
    *     column.
    * @param gen
    *     Generator for the JSON output.
    */
  def encodeAsRow(columns: List[String], gen: JsonGenerator): Unit = {
    gen.writeStartArray()
    columns.foreach { key =>
      Json.encode(gen, extractValueSafe(key))
    }
    gen.writeEndArray()
  }

  /** Return a JSON representation of the raw event. */
  def toJson: String = {
    Using.resource(new StringWriter) { w =>
      Using.resource(Json.newJsonGenerator(w)) { gen =>
        encode(gen)
      }
      w.toString
    }
  }

  /** Return a JSON representation of a row generated from the raw event. */
  def toJson(columns: List[String]): String = {
    Using.resource(new StringWriter) { w =>
      Using.resource(Json.newJsonGenerator(w)) { gen =>
        encodeAsRow(columns, gen)
      }
      w.toString
    }
  }

  /**
    * Estimates the size of the events in bytes. This is used for batching to ensure that
    * the payload is not too big for the backend. The default implementation just encodes
    * to a JSON string. If possible, override with a more efficient implementation.
    */
  def estimatedSizeInBytes: Int = {
    toJson.length
  }
}

object LwcEvent {

  /**
    * Wrap a Map as an LWC event. The timestamp will be the time when the event is
    * wrapped.
    */
  def apply(event: Map[String, Any]): LwcEvent = {
    apply(event, k => event.getOrElse(k, null))
  }

  /**
    * Wrap an object as an LWC event. The timestamp will be the time when the event is
    * wrapped.
    *
    * @param rawEvent
    *     Raw event object to wrap.
    * @param extractor
    *     Function to extract a value from the raw event. Returns null if there is no
    *     value associated with the key.
    * @return
    *     Wrapped event to process with LWC.
    */
  def apply(rawEvent: Any, extractor: String => Any): LwcEvent = {
    BasicLwcEvent(rawEvent, extractor)
  }

  /**
    * Event used to indicate a heartbeat. It just consists of a timestamp and can be used to
    * ensure regular traffic. Some activity such as flushing analytic data points or cleanup
    * may be triggered by the heartbeat.
    */
  case class HeartbeatLwcEvent(timestamp: Long) extends LwcEvent {

    /** Raw event object that is being considered. */
    override def rawEvent: Any = timestamp

    /**
      * Extract a value from the raw event for a given key. This method should be consistent
      * with the `tagValue` method for keys that can be considered tags.
      */
    override def extractValue(key: String): Any = null
  }

  private case class BasicLwcEvent(rawEvent: Any, extractor: String => Any) extends LwcEvent {

    override val timestamp: Long = System.currentTimeMillis()

    override def extractValue(key: String): Any = extractor(key)
  }

  /**
    * Wraps an event and converts it to a row.
    *
    * @param event
    *     Event to wrap.
    * @param columns
    *     Columsn to project into the rwo.
    */
  case class Row(event: LwcEvent, columns: List[String]) extends LwcEvent {

    override def rawEvent: Any = event.rawEvent

    override def timestamp: Long = event.timestamp

    override def tagValue(key: String): String = event.tagValue(key)

    override def extractValue(key: String): Any = event.extractValue(key)

    override def encode(gen: JsonGenerator): Unit = {
      event.encodeAsRow(columns, gen)
    }

    override def encodeAsRow(columns: List[String], gen: JsonGenerator): Unit = {
      event.encodeAsRow(columns, gen)
    }
  }

  /** Wraps a sequence of events into an event that can be submitted. */
  case class Events(seq: Seq[LwcEvent]) extends LwcEvent {

    require(seq.nonEmpty, "event sequence cannot be empty")

    override def rawEvent: Any = seq

    override def timestamp: Long = seq.head.timestamp

    override def tagValue(key: String): String = null

    override def extractValue(key: String): Any = null

    override def encode(gen: JsonGenerator): Unit = {
      gen.writeStartArray()
      seq.foreach(_.encode(gen))
      gen.writeEndArray()
    }

    override def encodeAsRow(columns: List[String], gen: JsonGenerator): Unit = {
      throw new UnsupportedOperationException()
    }
  }

  /** Represents a span event that makes up a trace. */
  trait Span extends LwcEvent {

    /** Return the id for this span. */
    def spanId: String

    /** Returns the id for the parent or `null` if it is the root span. */
    def parentId: String
  }
}
