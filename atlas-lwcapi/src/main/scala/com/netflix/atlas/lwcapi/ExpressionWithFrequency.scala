/*
 * Copyright 2014-2016 Netflix, Inc.
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
package com.netflix.atlas.lwcapi

import java.util.Base64

import com.fasterxml.jackson.core.{JsonParser, Version}
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, JsonNode}
import com.fasterxml.jackson.databind.module.SimpleModule
import com.netflix.atlas.json.{Json, JsonSupport}

case class ExpressionWithFrequency(expression: String, frequency: Int, id: String) extends JsonSupport {
  if (expression == null || expression.isEmpty) {
    throw new IllegalArgumentException("expression is empty or null")
  }

  if (id == null || id.isEmpty) {
    throw new IllegalArgumentException("id is empty or null")
  }
}

object ExpressionWithFrequency {
  val module = new SimpleModule("CustomJson", Version.unknownVersion())
  module.addDeserializer(classOf[ExpressionWithFrequency], new ExpressionWithFrequencyDeserializer)
  Json.registerModule(module)

  def fromJson(json: String): ExpressionWithFrequency = Json.decode[ExpressionWithFrequency](json)

  def apply(expression: String, frequency: Int) = {
    val f = if (frequency > 0) frequency else ApiSettings.defaultFrequency
    new ExpressionWithFrequency(expression, f, computeId(expression, f))
  }

  def apply(expression: String) = new ExpressionWithFrequency(expression, ApiSettings.defaultFrequency, computeId(expression, ApiSettings.defaultFrequency))

  def computeId(e: String, f: Int): String = {
    val key = s"$f~$e"
    val md = java.security.MessageDigest.getInstance("SHA-1")
    Base64.getUrlEncoder.withoutPadding.encodeToString(md.digest(key.getBytes("UTF-8")))
  }
}

class ExpressionWithFrequencyDeserializer extends JsonDeserializer[ExpressionWithFrequency] {
  def deserialize(parser: JsonParser, context: DeserializationContext) = {
    val node: JsonNode = parser.getCodec.readTree(parser)

    val expression = if (node.hasNonNull("expression")) node.get("expression").asText("") else null
    if (expression == null || expression.isEmpty)
      throw new IllegalArgumentException("expression is empty or null")

    val frequency = if (node.hasNonNull("frequency")) {
      val value = node.get("frequency")
      if (!value.canConvertToInt) {
        throw new IllegalArgumentException("frequency is not an integer")
      }
      value.asInt(ApiSettings.defaultFrequency)
    } else {
      ApiSettings.defaultFrequency
    }

    if (node.hasNonNull("id")) {
      val id = node.get("id").asText
      if (id == null || id.isEmpty)
        throw new IllegalArgumentException("id is empty or null")
      ExpressionWithFrequency(expression, frequency, id)
    } else {
      ExpressionWithFrequency(expression, frequency)
    }
  }
}
