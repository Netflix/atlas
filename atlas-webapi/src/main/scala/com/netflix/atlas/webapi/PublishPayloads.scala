/*
 * Copyright 2014-2021 Netflix, Inc.
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
import com.fasterxml.jackson.core.JsonToken
import com.netflix.atlas.core.model.DatapointTuple
import com.netflix.atlas.core.model.ItemId
import com.netflix.atlas.core.model.TaggedItem
import com.netflix.atlas.core.util.Interner
import com.netflix.atlas.core.util.RefIntHashMap
import com.netflix.atlas.core.util.SmallHashMap
import com.netflix.atlas.core.util.SortedTagMap
import com.netflix.atlas.core.util.Streams
import com.netflix.atlas.json.Json

import scala.util.Using

object PublishPayloads {

  import com.netflix.atlas.json.JsonParserHelper._

  type TagMap = Map[String, String]

  // Maximum size of arrays allocated based on input data. This is used as a sanity
  // check in case a bad payload comes in with a really large size for an array that
  // could exhaust memory.
  private final val maxArraySize = 100_000

  // Maximum number of tags allowed. This will be used to help pre-size buffers when
  // processing tag data.
  private final val maxPermittedTags = ApiSettings.maxPermittedTags

  private def decodeTags(parser: JsonParser, commonTags: TagMap, intern: Boolean): TagMap = {
    val strInterner = Interner.forStrings
    val b = new SmallHashMap.Builder[String, String](2 * maxPermittedTags)
    if (commonTags != null) b.addAll(commonTags)
    foreachField(parser) {
      case key =>
        val value = parser.nextTextValue()
        if (value != null) {
          if (intern)
            b.add(strInterner.intern(key), strInterner.intern(value))
          else
            b.add(key, value)
        }
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

  private def decode(parser: JsonParser, commonTags: TagMap, intern: Boolean): DatapointTuple = {
    var tags: TagMap = null
    var timestamp: Long = -1L
    var value: Double = Double.NaN
    foreachField(parser) {
      case "tags"      => tags = decodeTags(parser, commonTags, intern)
      case "timestamp" => timestamp = nextLong(parser)
      case "value"     => value = nextDouble(parser)
      case "start"     => timestamp = nextLong(parser) // Legacy support
      case "values"    => value = getValue(parser)
      case _ => // Ignore unknown fields
        parser.nextToken()
        parser.skipChildren()
    }
    val id = if (intern) TaggedItem.createId(tags) else TaggedItem.computeId(tags)
    DatapointTuple(id, tags, timestamp, value)
  }

  /**
    * Parse a single datapoint.
    *
    * @param parser
    *     Parser for JSON input.
    * @param intern
    *     If true, then strings and the final tag map will be interned as the data is being
    *     parsed.
    */
  def decodeDatapoint(parser: JsonParser, intern: Boolean = false): DatapointTuple = {
    decode(parser, null, intern)
  }

  /**
    * Parse a single datapoint.
    */
  def decodeDatapoint(json: String): DatapointTuple = {
    val parser = Json.newJsonParser(json)
    try decodeDatapoint(parser)
    finally parser.close()
  }

  /**
    * Parse batch of datapoints encoded as an object. Common tags to all datapoints can be
    * placed at the top level to avoid repetition.
    *
    * @param parser
    *     Parser for JSON input.
    * @param intern
    *     If true, then strings and the final tag map will be interned as the data is being
    *     parsed.
    */
  def decodeBatch(parser: JsonParser, intern: Boolean = false): List[DatapointTuple] = {
    var tags: Map[String, String] = null
    var metrics: List[DatapointTuple] = null
    var tagsLoadedFirst = false
    foreachField(parser) {
      case "tags" => tags = decodeTags(parser, null, intern)
      case "metrics" =>
        tagsLoadedFirst = (tags != null)
        val builder = List.newBuilder[DatapointTuple]
        foreachItem(parser) { builder += decode(parser, tags, intern) }
        metrics = builder.result()
    }

    // If the tags were loaded first they got merged with the datapoints while parsing. Otherwise
    // they need to be merged here.
    if (tagsLoadedFirst || tags == null) {
      if (metrics == null) Nil else metrics
    } else {
      metrics.map(d => d.copy(tags = d.tags ++ tags))
    }
  }

  /**
    * Parse batch of datapoints encoded as an object. Common tags to all datapoints can be
    * placed at the top level to avoid repetition.
    */
  def decodeBatch(json: String): List[DatapointTuple] = {
    val parser = Json.newJsonParser(json)
    try decodeBatch(parser)
    finally parser.close()
  }

  private def nextArraySize(parser: JsonParser): Int = {
    val size = nextInt(parser)
    if (size > maxArraySize) {
      throw new IllegalArgumentException(
        s"requested buffer size exceeds limit ($size > $maxArraySize)"
      )
    }
    size
  }

  /**
    * Batch format that is less repetitive for string data and more efficient to process
    * than the normal batch format encoded as an object. Data is encoded as a flattened
    * array with the following structure:
    *
    * ```
    * [
    *   size of string table,
    *   ... strings in table...,
    *
    *   number of datapoints,
    *   foreach datapoint:
    *     id computed based on tags,
    *     number of tags,
    *     foreach tag:
    *       int for key position in string table,
    *       int for value position in string table,
    *     timestamp,
    *     value,
    * ]
    * ```
    *
    * @param parser
    *     Parser for JSON input.
    * @param consumer
    *     Consumer that will be called with each datapoint that is extracted from the input.
    * @param intern
    *     If true, then strings and the final tag map will be interned as the data is being
    *     parsed.
    */
  def decodeCompactBatch(
    parser: JsonParser,
    consumer: PublishConsumer,
    intern: Boolean = false
  ): Unit = {
    val strInterner = Interner.forStrings

    requireNextToken(parser, JsonToken.START_ARRAY)
    val table = new Array[String](nextArraySize(parser))
    var i = 0
    while (i < table.length) {
      val s = nextString(parser)
      table(i) = if (intern) strInterner.intern(s) else s
      i += 1
    }

    val numDatapointTuples = nextInt(parser)
    i = 0
    while (i < numDatapointTuples) {
      val idRaw = ItemId(nextString(parser))
      val id = if (intern) TaggedItem.internId(idRaw) else idRaw

      val numTags = nextArraySize(parser)
      var j = 0
      val builder = SortedTagMap.builder(numTags)
      while (j < numTags) {
        val k = table(nextInt(parser))
        val v = table(nextInt(parser))
        builder.add(k, v)
        j += 1
      }
      val tags = if (intern) TaggedItem.internTagsShallow(builder.result()) else builder.result()

      val timestamp = nextLong(parser)
      val value = getValue(parser)

      consumer.consume(id, tags, timestamp, value)
      i += 1
    }
  }

  /**
    * Batch format that is less repetitive for string data and more efficient to process
    * than the normal batch format encoded as an object.
    */
  def decodeCompactBatch(json: String): List[DatapointTuple] = {
    val parser = Json.newJsonParser(json)
    val consumer = PublishConsumer.datapointList
    try decodeCompactBatch(parser, consumer)
    finally parser.close()
    consumer.toList
  }

  /**
    * Parse batch of datapoints encoded as a list.
    *
    * @param parser
    *     Parser for JSON input.
    * @param intern
    *     If true, then strings and the final tag map will be interned as the data is being
    *     parsed.
    */
  def decodeList(parser: JsonParser, intern: Boolean = false): List[DatapointTuple] = {
    val builder = List.newBuilder[DatapointTuple]
    foreachItem(parser) {
      builder += decode(parser, null, intern)
    }
    builder.result()
  }

  /**
    * Parse batch of datapoints encoded as a list.
    */
  def decodeList(json: String): List[DatapointTuple] = {
    val parser = Json.newJsonParser(json)
    try decodeList(parser)
    finally parser.close()
  }

  private def encodeTags(gen: JsonGenerator, tags: Map[String, String]): Unit = {
    gen.writeObjectFieldStart("tags")
    tags.foreachEntry(gen.writeStringField)
    gen.writeEndObject()
  }

  def encodeDatapoint(gen: JsonGenerator, d: DatapointTuple): Unit = {
    gen.writeStartObject()
    encodeTags(gen, d.tags)
    gen.writeNumberField("timestamp", d.timestamp)
    gen.writeNumberField("value", d.value)
    gen.writeEndObject()
  }

  def encodeDatapoint(d: DatapointTuple): String = {
    Streams.string { w =>
      Using.resource(Json.newJsonGenerator(w)) { gen =>
        encodeDatapoint(gen, d)
      }
    }
  }

  /**
    * Encode batch of datapoints.
    */
  def encodeBatch(gen: JsonGenerator, tags: TagMap, values: List[DatapointTuple]): Unit = {
    gen.writeStartObject()
    encodeTags(gen, tags)
    gen.writeArrayFieldStart("metrics")
    values.foreach(v => encodeDatapoint(gen, v))
    gen.writeEndArray()
    gen.writeEndObject()
  }

  /**
    * Encode batch of datapoints.
    */
  def encodeBatch(tags: TagMap, values: List[DatapointTuple]): String = {
    Streams.string { w =>
      Using.resource(Json.newJsonGenerator(w)) { gen =>
        encodeBatch(gen, tags, values)
      }
    }
  }

  private def encodeStringTable(
    gen: JsonGenerator,
    values: List[DatapointTuple]
  ): RefIntHashMap[String] = {
    val stringPositions = new RefIntHashMap[String](100)
    values.foreach { value =>
      value.tags.foreachEntry { (k, v) =>
        stringPositions.putIfAbsent(k, stringPositions.size)
        stringPositions.putIfAbsent(v, stringPositions.size)
      }
    }

    val tmp = new Array[String](stringPositions.size)
    stringPositions.foreach { (s, i) =>
      tmp(i) = s
    }
    gen.writeNumber(tmp.length)
    var i = 0
    while (i < tmp.length) {
      gen.writeString(tmp(i))
      i += 1
    }

    stringPositions
  }

  /**
    * Encode batch of datapoints as using compact format. See decodeCompactBatch method for
    * details.
    */
  def encodeCompactBatch(gen: JsonGenerator, values: List[DatapointTuple]): Unit = {
    gen.writeStartArray()

    val table = encodeStringTable(gen, values)

    gen.writeNumber(values.size)
    values.foreach { value =>
      // id
      gen.writeString(value.id.toString)

      // tags
      gen.writeNumber(value.tags.size)
      value.tags.foreachEntry { (k, v) =>
        gen.writeNumber(table.get(k, -1))
        gen.writeNumber(table.get(v, -1))
      }

      // timestamp
      gen.writeNumber(value.timestamp)

      // value
      gen.writeNumber(value.value)
    }

    gen.writeEndArray()
  }

  /**
    * Encode batch of datapoints as using compact format. See decodeCompactBatch method for
    * details.
    */
  def encodeCompactBatch(values: List[DatapointTuple]): String = {
    Streams.string { w =>
      Using.resource(Json.newJsonGenerator(w)) { gen =>
        encodeCompactBatch(gen, values)
      }
    }
  }

  /**
    * Encode batch of datapoints as a list.
    */
  def encodeList(gen: JsonGenerator, values: List[DatapointTuple]): Unit = {
    gen.writeStartArray()
    values.foreach(v => encodeDatapoint(gen, v))
    gen.writeEndArray()
  }

  /**
    * Encode batch of datapoints as a list.
    */
  def encodeList(values: List[DatapointTuple]): String = {
    Streams.string { w =>
      Using.resource(Json.newJsonGenerator(w)) { gen =>
        encodeList(gen, values)
      }
    }
  }
}
