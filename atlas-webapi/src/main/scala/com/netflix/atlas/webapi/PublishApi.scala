/*
 * Copyright 2015 Netflix, Inc.
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
package com.netflix.atlas.webapi

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.core.model.DefaultSettings
import com.netflix.atlas.core.model.TaggedItem
import com.netflix.atlas.core.util.Interner
import com.netflix.atlas.core.util.SmallHashMap
import com.netflix.atlas.core.util.Streams
import com.netflix.atlas.json.Json

object PublishApi {
  private final val step = DefaultSettings.stepSize

  import com.netflix.atlas.json.JsonParserHelper._

  type TagMap = Map[String, String]

  private final val maxPermittedTags = 30

  private def decodeTags(parser: JsonParser, commonTags: TagMap, intern: Boolean): TagMap = {
    val strInterner = Interner.forStrings
    val b = new SmallHashMap.Builder[String, String](2 * maxPermittedTags)
    if (commonTags != null) b.addAll(commonTags)
    foreachField(parser) { case key =>
      val value = parser.nextTextValue()
      if (value == null || value.isEmpty) {
        val loc = parser.getCurrentLocation
        val line = loc.getLineNr
        val col = loc.getColumnNr
        val msg = s"tag value cannot be null or empty (key=$key, line=$line, col=$col)"
        throw new IllegalArgumentException(msg)
      }
      if (intern)
        b.add(strInterner.intern(key), strInterner.intern(value))
      else
        b.add(key, value)
    }
    if (intern) TaggedItem.internTagsShallow(b.compact) else b.result
  }

  private def getValue(parser: JsonParser): Double = {
    import com.fasterxml.jackson.core.JsonToken._
    parser.nextToken() match {
      case START_ARRAY        => nextDouble(parser)
      case VALUE_NUMBER_FLOAT => parser.getValueAsDouble()
      case VALUE_STRING       => java.lang.Double.valueOf(parser.getText())
      case t                  => fail(parser, s"expected VALUE_NUMBER_FLOAT but received $t")
    }
  }

  private def decode(parser: JsonParser, commonTags: TagMap, intern: Boolean): Datapoint = {
    var tags: TagMap = null
    var timestamp: Long = -1L
    var value: Double = Double.NaN
    foreachField(parser) {
      case "tags"      => tags = decodeTags(parser, commonTags, intern)
      case "timestamp" => timestamp = nextLong(parser)
      case "value"     => value = nextDouble(parser)
      case "start"     => timestamp = nextLong(parser) // Legacy support
      case "values"    => value = getValue(parser)
      case _           => // Ignore unknown fields
    }
    Datapoint(tags, timestamp, value)
  }

  def decodeDatapoint(parser: JsonParser, intern: Boolean = false): Datapoint = {
    decode(parser, null, intern)
  }

  def decodeDatapoint(json: String): Datapoint = {
    val parser = Json.newJsonParser(json)
    try decodeDatapoint(parser, false) finally parser.close()
  }

  def decodeBatch(parser: JsonParser, intern: Boolean = false): List[Datapoint] = {
    var tags: Map[String, String] = null
    var metrics: List[Datapoint] = null
    var tagsLoadedFirst = false
    foreachField(parser) {
      case "tags"    => tags = decodeTags(parser, null, intern)
      case "metrics" =>
        tagsLoadedFirst = (tags != null)
        val builder = List.newBuilder[Datapoint]
        foreachItem(parser) { builder += decode(parser, tags, intern) }
        metrics = builder.result
    }
    if (tagsLoadedFirst || tags == null) metrics else metrics.map(d => d.copy(tags = d.tags ++ tags))
  }

  def decodeBatch(json: String): List[Datapoint] = {
    val parser = Json.newJsonParser(json)
    try decodeBatch(parser, false) finally parser.close()
  }

  def decodeList(parser: JsonParser, intern: Boolean = false): List[Datapoint] = {
    val builder = List.newBuilder[Datapoint]
    foreachItem(parser) {
      builder += decode(parser, null, intern)
    }
    builder.result
  }

  def decodeList(json: String): List[Datapoint] = {
    val parser = Json.newJsonParser(json)
    try decodeList(parser, false) finally parser.close()
  }

  private def encodeTags(gen: JsonGenerator, tags: Map[String, String]) {
    gen.writeObjectFieldStart("tags")
    tags match {
      case m: SmallHashMap[String, String] => m.foreachItem { (k, v) => gen.writeStringField(k, v) }
      case m: Map[String, String]          => m.foreach { t => gen.writeStringField(t._1, t._2) }
    }
    gen.writeEndObject()
  }

  def encodeDatapoint(gen: JsonGenerator, d: Datapoint) {
    gen.writeStartObject()
    encodeTags(gen, d.tags)
    gen.writeNumberField("timestamp", d.timestamp)
    gen.writeNumberField("value", d.value)
    gen.writeEndObject()
  }

  def encodeDatapoint(d: Datapoint): String = {
    Streams.string { w =>
      Streams.scope(Json.newJsonGenerator(w)) { gen => encodeDatapoint(gen, d) }
    }
  }

  def encodeBatch(gen: JsonGenerator, tags: Map[String, String], values: List[Datapoint]) {
    gen.writeStartObject()
    encodeTags(gen, tags)
    gen.writeArrayFieldStart("metrics")
    values.foreach(v => encodeDatapoint(gen, v))
    gen.writeEndArray()
    gen.writeEndObject()
  }

  def encodeBatch(tags: Map[String, String], values: List[Datapoint]): String = {
    Streams.string { w =>
      Streams.scope(Json.newJsonGenerator(w)) { gen => encodeBatch(gen, tags, values) }
    }
  }
}
