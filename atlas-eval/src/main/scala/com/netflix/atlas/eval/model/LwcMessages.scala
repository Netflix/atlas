/*
 * Copyright 2014-2020 Netflix, Inc.
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
