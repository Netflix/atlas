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
package com.netflix.atlas.eval.model

import akka.util.ByteString
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.netflix.atlas.akka.ByteStringInputStream
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.core.util.SmallHashMap
import com.netflix.atlas.json.Json
import com.netflix.atlas.json.JsonParserHelper._
import com.netflix.atlas.json.JsonSupport

import java.io.ByteArrayOutputStream

/**
  * Helpers for working with messages coming back from the LWCAPI service.
  */
object LwcMessages {

  /**
    * Parse the message string into an internal model object based on the type.
    */
  def parse(msg: ByteString): AnyRef = {
    parse(Json.newJsonParser(new ByteStringInputStream(msg)))
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
      var step: Long = -1L

      // LwcSubscription
      // - expression
      var metrics: List[LwcDataExpr] = Nil

      // LwcDatapoint
      var timestamp: Long = -1L
      var id: String = null
      var tags: Map[String, String] = Map.empty
      var value: Double = Double.NaN

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
        case "step"       => step = nextLong(parser)
        case "metrics"    => metrics = parseDataExprs(parser)

        case "timestamp" => timestamp = nextLong(parser)
        case "id"        => id = nextString(parser)
        case "tags"      => tags = parseTags(parser)
        case "value"     => value = nextDouble(parser)

        case "message" =>
          val t = parser.nextToken()
          if (t == JsonToken.VALUE_STRING)
            message = parser.getText
          else
            diagnosticMessage = parseDiagnosticMessage(parser)

        case _ => skipNext(parser)
      }

      typeDesc match {
        case "expression"   => LwcExpression(expression, step)
        case "subscription" => LwcSubscription(expression, metrics)
        case "datapoint"    => LwcDatapoint(timestamp, id, tags, value)
        case "diagnostic"   => LwcDiagnosticMessage(id, diagnosticMessage)
        case "heartbeat"    => LwcHeartbeat(timestamp, step)
        case _              => DiagnosticMessage(typeDesc, message, None)
      }
    } finally {
      parser.close()
    }
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
    val builder = new SmallHashMap.Builder[String, String](30)
    foreachField(parser) {
      case k => builder.add(k, nextString(parser))
    }
    builder.result
  }

  private val Expression = 0
  private val Subscription = 1
  private val Datapoint = 2
  private val LwcDiagnostic = 3
  private val Diagnostic = 4
  private val Heartbeat = 5

  /**
    * Encode messages using Jackson's smile format into a ByteString.
    */
  def encodeBatch(msgs: Seq[AnyRef]): ByteString = {
    encodeBatch(msgs, new ByteArrayOutputStream())
  }

  /**
    * Encode messages using Jackson's smile format into a ByteString. The
    * `ByteArrayOutputStream` will be reset and used as a buffer for encoding
    * the data.
    */
  def encodeBatch(msgs: Seq[AnyRef], baos: ByteArrayOutputStream): ByteString = {
    baos.reset()
    val gen = Json.newSmileGenerator(baos)
    try {
      gen.writeStartArray()
      msgs.foreach {
        case msg: LwcExpression =>
          gen.writeNumber(Expression)
          gen.writeString(msg.expression)
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
        case msg: LwcDatapoint =>
          gen.writeNumber(Datapoint)
          gen.writeNumber(msg.timestamp)
          gen.writeString(msg.id)
          gen.writeNumber(msg.tags.size)
          msg.tags.foreachEntry { (k, v) =>
            gen.writeString(k)
            gen.writeString(v)
          }
          gen.writeNumber(msg.value)
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
        case _ =>
          throw new MatchError("foo")
      }
      gen.writeEndArray()
    } finally {
      gen.close()
    }
    ByteString.fromArrayUnsafe(baos.toByteArray)
  }

  /**
    * Parse a set of messages that were encoded with `encodeBatch`.
    */
  def parseBatch(msgs: ByteString): List[AnyRef] = {
    parseBatch(Json.newSmileParser(new ByteStringInputStream(msgs)))
  }

  private def parseBatch(parser: JsonParser): List[AnyRef] = {
    val builder = List.newBuilder[AnyRef]
    try {
      foreachItem(parser) {
        parser.getIntValue match {
          case Expression =>
            builder += LwcExpression(parser.nextTextValue(), parser.nextLongValue(-1L))
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
          case Datapoint =>
            val timestamp = parser.nextLongValue(-1L)
            val id = parser.nextTextValue()
            val tags = parseTags(parser, parser.nextIntValue(0))
            val value = nextDouble(parser)
            builder += LwcDatapoint(timestamp, id, tags, value)
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
      SmallHashMap.empty[String, String]
    } else {
      val builder = new SmallHashMap.Builder[String, String](2 * n)
      var i = 0
      while (i < n) {
        val k = parser.nextTextValue()
        val v = parser.nextTextValue()
        builder.add(k, v)
        i += 1
      }
      builder.result
    }
  }

  def toSSE(msg: JsonSupport): ByteString = {
    val prefix = msg match {
      case _: LwcSubscription      => subscribePrefix
      case _: LwcDatapoint         => metricDataPrefix
      case _: LwcDiagnosticMessage => diagnosticPrefix
      case _: LwcHeartbeat         => heartbeatPrefix
      case _                       => defaultPrefix
    }
    prefix ++ ByteString(msg.toJson) ++ suffix
  }

  private val subscribePrefix = ByteString("info: subscribe ")
  private val metricDataPrefix = ByteString("data: metric ")
  private val diagnosticPrefix = ByteString("data: diagnostic ")
  private val heartbeatPrefix = ByteString("data: heartbeat ")
  private val defaultPrefix = ByteString("data: ")

  private val suffix = ByteString("\r\n\r\n")
}
