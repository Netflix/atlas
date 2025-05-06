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
package com.netflix.atlas.eval.model

import org.apache.pekko.util.ByteString
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.NullNode
import com.netflix.atlas.core.util.SortedTagMap
import com.netflix.atlas.json.Json
import com.netflix.atlas.json.JsonParserHelper.*
import com.netflix.atlas.pekko.ByteStringInputStream
import com.netflix.atlas.pekko.DiagnosticMessage

import java.io.ByteArrayOutputStream
import java.io.OutputStream
import java.util.zip.GZIPOutputStream

/**
  * Helpers for working with messages coming back from the LWCAPI service.
  */
object LwcMessages {

  // For reading arbitrary json structures for events
  private val mapper = Json.newMapperBuilder.build()

  /**
    * Parse the message string into an internal model object based on the type.
    */
  def parse(msg: ByteString): AnyRef = {
    parse(Json.newJsonParser(ByteStringInputStream.create(msg)))
  }

  /**
    * Parse the message string into an internal model object based on the type.
    */
  def parse(msg: String): AnyRef = {
    parse(Json.newJsonParser(msg))
  }

  private def parse(parser: JsonParser): AnyRef = {
    // This is a performance critical part of the code so the parsing is done by
    // hand rather than using ObjectMapper to minimize allocations and get peak
    // performance.
    try {
      // All
      var typeDesc: String = null

      // LwcExpression
      var expression: String = null
      var exprType: ExprType = ExprType.TIME_SERIES
      var step: Long = -1L

      // LwcSubscription
      // - expression
      // - metrics
      // LwcSubscriptionV2
      // - expression
      // - exprType
      // - subExprs
      var subExprs: List[LwcDataExpr] = Nil

      // LwcDatapoint
      var timestamp: Long = -1L
      var id: String = null
      var tags: Map[String, String] = Map.empty
      var value: Double = Double.NaN
      var samples: List[List[Any]] = Nil

      // LwcEvent
      var payload: JsonNode = NullNode.instance

      // LwcDiagnosticMessage
      // - id
      // - message: DiagnosticMessage
      var diagnosticMessage: DiagnosticMessage = null

      // LwcHeartbeat
      // - timestamp
      // - step

      // DiagnosticMessage
      // - message: String
      var message: String = null

      // Actually do the parsing work
      foreachField(parser) {
        case "type" => typeDesc = nextString(parser)

        case "expression" => expression = nextString(parser)
        case "exprType"   => exprType = ExprType.valueOf(nextString(parser))
        case "step"       => step = nextLong(parser)
        case "metrics"    => subExprs = parseDataExprs(parser)
        case "subExprs"   => subExprs = parseDataExprs(parser)

        case "timestamp" => timestamp = nextLong(parser)
        case "id"        => id = nextString(parser)
        case "tags"      => tags = parseTags(parser)
        case "value"     => value = nextDouble(parser)
        case "samples"   => samples = parseSamples(parser)

        case "payload" => payload = nextTree(parser)

        case "message" =>
          val t = parser.nextToken()
          if (t == JsonToken.VALUE_STRING)
            message = parser.getText
          else
            diagnosticMessage = parseDiagnosticMessage(parser)

        case _ => skipNext(parser)
      }

      typeDesc match {
        case "expression"      => LwcExpression(expression, exprType, step)
        case "subscription"    => LwcSubscription(expression, subExprs)
        case "subscription-v2" => LwcSubscriptionV2(expression, exprType, subExprs)
        case "datapoint"       => LwcDatapoint(timestamp, id, tags, value, samples)
        case "event"           => LwcEvent(id, payload)
        case "diagnostic"      => LwcDiagnosticMessage(id, diagnosticMessage)
        case "heartbeat"       => LwcHeartbeat(timestamp, step)
        case _                 => DiagnosticMessage(typeDesc, message, None)
      }
    } finally {
      parser.close()
    }
  }

  private def nextTree(parser: JsonParser): JsonNode = {
    parser.nextToken()
    mapper.readTree[JsonNode](parser)
  }

  private[model] def parseDataExprs(parser: JsonParser): List[LwcDataExpr] = {
    val builder = List.newBuilder[LwcDataExpr]
    foreachItem(parser) {
      var id: String = null
      var expression: String = null
      var step: Long = -1L

      foreachField(parser) {
        case "id"                 => id = nextString(parser)
        case "expression"         => expression = nextString(parser)
        case "step" | "frequency" => step = nextLong(parser)
        case _                    => skipNext(parser)
      }

      builder += LwcDataExpr(id, expression, step)
    }
    builder.result()
  }

  private def parseSamples(parser: JsonParser): List[List[Any]] = {
    val samples = List.newBuilder[List[Any]]
    foreachItem(parser) {
      val row = List.newBuilder[Any]
      var t = parser.nextToken()
      while (t != null && t != JsonToken.END_ARRAY) {
        row += parser.readValueAsTree[JsonNode]()
        t = parser.nextToken()
      }
      samples += row.result()
    }
    samples.result()
  }

  private def parseDiagnosticMessage(parser: JsonParser): DiagnosticMessage = {
    var typeDesc: String = null
    var message: String = null
    foreachField(parser) {
      case "type"    => typeDesc = nextString(parser)
      case "message" => message = nextString(parser)
      case _         => skipNext(parser)
    }
    DiagnosticMessage(typeDesc, message, None)
  }

  private def parseTags(parser: JsonParser): Map[String, String] = {
    val builder = new SortedTagMap.Builder(30)
    foreachField(parser) {
      case k => builder.add(k, nextString(parser))
    }
    builder.result()
  }

  private val Expression = 0
  private val Subscription = 1
  private val Datapoint = 2
  private val LwcDiagnostic = 3
  private val Diagnostic = 4
  private val Heartbeat = 5
  private val Event = 6
  private val SubscriptionV2 = 7
  private val DatapointV2 = 8

  /**
    * Encode messages using Jackson's smile format into a ByteString.
    */
  def encodeBatch(msgs: Seq[AnyRef], compress: Boolean = false): ByteString = {
    val baos = new ByteArrayOutputStream()
    if (compress)
      encodeBatchImpl(msgs, new GZIPOutputStream(baos))
    else
      encodeBatchImpl(msgs, baos)
    ByteString.fromArrayUnsafe(baos.toByteArray)
  }

  /**
    * Encode messages using Jackson's smile format into a ByteString. The
    * `ByteArrayOutputStream` will be reset and used as a buffer for encoding
    * the data.
    */
  def encodeBatch(msgs: Seq[AnyRef], baos: ByteArrayOutputStream): ByteString = {
    baos.reset()
    encodeBatchImpl(msgs, baos)
    ByteString.fromArrayUnsafe(baos.toByteArray)
  }

  private def encodeBatchImpl(msgs: Seq[AnyRef], out: OutputStream): Unit = {
    try {
      val gen = Json.newSmileGenerator(out)
      try {
        gen.writeStartArray()
        msgs.foreach {
          case msg: LwcExpression =>
            gen.writeNumber(Expression)
            gen.writeString(msg.expression)
            if (msg.exprType != ExprType.TIME_SERIES)
              gen.writeString(msg.exprType.name())
            gen.writeNumber(msg.step)
          case msg: LwcSubscription =>
            gen.writeNumber(Subscription)
            gen.writeString(msg.expression)
            gen.writeStartArray()
            msg.metrics.foreach { m =>
              gen.writeString(m.id)
              gen.writeString(m.expression)
              gen.writeNumber(m.step)
            }
            gen.writeEndArray()
          case msg: LwcSubscriptionV2 =>
            gen.writeNumber(SubscriptionV2)
            gen.writeString(msg.expression)
            gen.writeString(msg.exprType.name())
            gen.writeStartArray()
            msg.subExprs.foreach { s =>
              gen.writeString(s.id)
              gen.writeString(s.expression)
              gen.writeNumber(s.step)
            }
            gen.writeEndArray()
          case msg: LwcDatapoint if msg.samples.isEmpty =>
            gen.writeNumber(Datapoint)
            gen.writeNumber(msg.timestamp)
            gen.writeString(msg.id)
            // Should already be sorted, but convert if needed to ensure we can rely on
            // the order. It will be a noop if already a SortedTagMap.
            val tags = SortedTagMap(msg.tags)
            gen.writeNumber(tags.size)
            tags.foreachEntry { (k, v) =>
              gen.writeString(k)
              gen.writeString(v)
            }
            gen.writeNumber(msg.value)
          case msg: LwcDatapoint =>
            gen.writeNumber(DatapointV2)
            gen.writeNumber(msg.timestamp)
            gen.writeString(msg.id)
            // Should already be sorted, but convert if needed to ensure we can rely on
            // the order. It will be a noop if already a SortedTagMap.
            val tags = SortedTagMap(msg.tags)
            gen.writeNumber(tags.size)
            tags.foreachEntry { (k, v) =>
              gen.writeString(k)
              gen.writeString(v)
            }
            gen.writeNumber(msg.value)
            Json.encode(gen, msg.samples)
          case msg: LwcDiagnosticMessage =>
            gen.writeNumber(LwcDiagnostic)
            gen.writeString(msg.id)
            gen.writeString(msg.message.typeName)
            gen.writeString(msg.message.message)
          case msg: DiagnosticMessage =>
            gen.writeNumber(Diagnostic)
            gen.writeString(msg.typeName)
            gen.writeString(msg.message)
          case msg: LwcHeartbeat =>
            gen.writeNumber(Heartbeat)
            gen.writeNumber(msg.timestamp)
            gen.writeNumber(msg.step)
          case msg: LwcEvent =>
            gen.writeNumber(Event)
            gen.writeString(msg.id)
            mapper.writeTree(gen, msg.payload)
          case msg =>
            throw new MatchError(s"$msg")
        }
        gen.writeEndArray()
      } finally {
        gen.close()
      }
    } finally {
      out.close()
    }
  }

  /**
    * Parse a set of messages that were encoded with `encodeBatch`.
    */
  def parseBatch(msgs: ByteString): List[AnyRef] = {
    parseBatch(Json.newSmileParser(ByteStringInputStream.create(msgs)))
  }

  private def parseBatch(parser: JsonParser): List[AnyRef] = {
    val builder = List.newBuilder[AnyRef]
    try {
      foreachItem(parser) {
        parser.getIntValue match {
          case Expression =>
            val expression = parser.nextTextValue()
            if (parser.nextToken() == JsonToken.VALUE_STRING) {
              val exprType = ExprType.valueOf(parser.getText)
              val step = parser.nextLongValue(-1L)
              builder += LwcExpression(expression, exprType, step)
            } else {
              val step = parser.getLongValue
              builder += LwcExpression(expression, ExprType.TIME_SERIES, step)
            }
          case Subscription =>
            val expression = parser.nextTextValue()
            val dataExprs = List.newBuilder[LwcDataExpr]
            foreachItem(parser) {
              dataExprs += LwcDataExpr(
                parser.getText,
                parser.nextTextValue(),
                parser.nextLongValue(-1L)
              )
            }
            builder += LwcSubscription(expression, dataExprs.result())
          case SubscriptionV2 =>
            val expression = parser.nextTextValue()
            val exprType = ExprType.valueOf(parser.nextTextValue())
            val subExprs = List.newBuilder[LwcDataExpr]
            foreachItem(parser) {
              subExprs += LwcDataExpr(
                parser.getText,
                parser.nextTextValue(),
                parser.nextLongValue(-1L)
              )
            }
            builder += LwcSubscriptionV2(expression, exprType, subExprs.result())
          case Datapoint =>
            val timestamp = parser.nextLongValue(-1L)
            val id = parser.nextTextValue()
            val tags = parseTags(parser, parser.nextIntValue(0))
            val value = nextDouble(parser)
            builder += LwcDatapoint(timestamp, id, tags, value)
          case DatapointV2 =>
            val timestamp = parser.nextLongValue(-1L)
            val id = parser.nextTextValue()
            val tags = parseTags(parser, parser.nextIntValue(0))
            val value = nextDouble(parser)
            val samples = parseSamples(parser)
            builder += LwcDatapoint(timestamp, id, tags, value, samples)
          case LwcDiagnostic =>
            val id = parser.nextTextValue()
            val typeName = parser.nextTextValue()
            val message = parser.nextTextValue()
            builder += LwcDiagnosticMessage(id, DiagnosticMessage(typeName, message, None))
          case Diagnostic =>
            val typeName = parser.nextTextValue()
            val message = parser.nextTextValue()
            builder += DiagnosticMessage(typeName, message, None)
          case Heartbeat =>
            val timestamp = parser.nextLongValue(-1L)
            val step = parser.nextLongValue(-1L)
            builder += LwcHeartbeat(timestamp, step)
          case Event =>
            val id = parser.nextTextValue()
            builder += LwcEvent(id, nextTree(parser))
          case v =>
            throw new MatchError(s"invalid type id: $v")
        }
      }
    } finally {
      parser.close()
    }
    builder.result()
  }

  private def parseTags(parser: JsonParser, n: Int): Map[String, String] = {
    if (n == 0) {
      SortedTagMap.empty
    } else {
      val data = new Array[String](2 * n)
      var i = 0
      while (i < data.length) {
        data(i) = parser.nextTextValue()
        data(i + 1) = parser.nextTextValue()
        i += 2
      }
      // The map should be sorted from the server side, so we can avoid resorting
      // here. Force the hash to be computed and cached as soon as possible so it
      // can be done on the compute pool for parsing.
      val tags = SortedTagMap.createUnsafe(data, data.length)
      tags.hashCode
      tags
    }
  }
}
