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
package com.netflix.atlas.lwc.events

import com.fasterxml.jackson.core.JsonGenerator
import com.netflix.atlas.json.Json

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
    * Extract a tag value for a given key. Returns `null` if there is no value for
    * the key or the value is not a string. By default it will delegate to `extractValue`
    * to ensure the two are consistent.
    */
  def tagValue(key: String): String = {
    extractValue(key) match {
      case v: String => v
      case _         => null
    }
  }

  /**
    * Extract a value from the raw event for a given key. This method should be consistent
    * with the `tagValue` method for keys that can be considered tags.
    */
  def extractValue(key: String): Any

  /** Encode the raw event as JSON. */
  def encode(gen: JsonGenerator): Unit

  /**
    * Encode the raw event as an array representing a row in a table.
    *
    * @param columns
    *     Keys to use with `extractValue` for selecting the value of the
    *     column.
    * @param gen
    *     Generator for the JSON output.
    */
  def encodeAsRow(columns: List[String], gen: JsonGenerator): Unit

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
}

object LwcEvent {

  /**
    * Wrap an object as an LWC event.
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

  private case class BasicLwcEvent(rawEvent: Any, extractor: String => Any) extends LwcEvent {

    override def extractValue(key: String): Any = extractor(key)

    override def encode(gen: JsonGenerator): Unit = {
      Json.encode(gen, rawEvent)
    }

    override def encodeAsRow(columns: List[String], gen: JsonGenerator): Unit = {
      gen.writeStartArray()
      encodeColumns(columns, gen)
      gen.writeEndArray()
    }

    @scala.annotation.tailrec
    private def encodeColumns(columns: List[String], gen: JsonGenerator): Unit = {
      if (columns.nonEmpty) {
        Json.encode(gen, extractValue(columns.head))
        encodeColumns(columns.tail, gen)
      }
    }
  }
}
