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

import com.netflix.atlas.json.{Json, JsonSupport}

case class ExpressionWithFrequency(expression: String,
                                   frequency: Int = ApiSettings.defaultFrequency,
                                   id: String = "") extends JsonSupport {
  //require(expression != null && expression.nonEmpty)
}

object ExpressionWithFrequency {
  def fromJson(json: String): ExpressionWithFrequency = Json.decode[ExpressionWithFrequency](json)

  def apply(expression: String, frequency: Int) = {
    val f = if (frequency > 0) frequency else ApiSettings.defaultFrequency
    new ExpressionWithFrequency(expression, f, computeId(expression, f))
  }

  def apply(expression: String) = {
    new ExpressionWithFrequency(expression, id = computeId(expression, ApiSettings.defaultFrequency))
  }

  def computeId(e: String, f: Int): String = {
    val key = s"$f~$e"
    val md = java.security.MessageDigest.getInstance("SHA-1")
    Base64.getUrlEncoder.withoutPadding.encodeToString(md.digest(key.getBytes("UTF-8")))
  }
}
