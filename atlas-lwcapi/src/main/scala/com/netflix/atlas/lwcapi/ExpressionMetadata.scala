/*
 * Copyright 2014-2017 Netflix, Inc.
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

import com.netflix.atlas.core.util.Hash
import com.netflix.atlas.core.util.Strings
import com.netflix.atlas.json.JsonSupport

case class ExpressionMetadata(
    expression: String,
    frequency: Long = ApiSettings.defaultFrequency,
    id: String = "")
  extends JsonSupport with Ordered[ExpressionMetadata] {

  require(expression != null && expression.nonEmpty, "expression cannot be null")

  def compare(that: ExpressionMetadata): Int = {
    val expressionMatch = expression.compare(that.expression)
    if (expressionMatch == 0) frequency.compare(that.frequency) else expressionMatch
  }
}

object ExpressionMetadata {

  def apply(expression: String, frequency: Long): ExpressionMetadata = {
    val f = if (frequency > 0) frequency else ApiSettings.defaultFrequency
    new ExpressionMetadata(expression, f, computeId(expression, f))
  }

  def apply(expression: String): ExpressionMetadata = {
    apply(expression, ApiSettings.defaultFrequency)
  }

  def computeId(e: String, f: Long): String = {
    Strings.zeroPad(Hash.sha1(s"$f~$e"), 40)
  }
}
