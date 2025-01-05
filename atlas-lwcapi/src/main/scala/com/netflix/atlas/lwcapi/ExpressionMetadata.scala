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
package com.netflix.atlas.lwcapi

import com.fasterxml.jackson.annotation.JsonAlias
import com.netflix.atlas.core.util.Hash
import com.netflix.atlas.core.util.Strings
import com.netflix.atlas.eval.model.ExprType
import com.netflix.atlas.json.JsonSupport

case class ExpressionMetadata(
  expression: String,
  exprType: ExprType,
  @JsonAlias(Array("step")) frequency: Long = ApiSettings.defaultStep,
  id: String = ""
) extends JsonSupport
    with Ordered[ExpressionMetadata] {

  require(expression != null && expression.nonEmpty, "expression cannot be null")

  def compare(that: ExpressionMetadata): Int = {
    val expressionMatch = expression.compare(that.expression)
    if (expressionMatch == 0) frequency.compare(that.frequency) else expressionMatch
  }
}

object ExpressionMetadata {

  def apply(expression: String, exprType: ExprType, step: Long): ExpressionMetadata = {
    val f = if (step > 0) step else ApiSettings.defaultStep
    new ExpressionMetadata(expression, exprType, f, computeId(expression, exprType, f))
  }

  def apply(expression: String): ExpressionMetadata = {
    apply(expression, ExprType.TIME_SERIES, ApiSettings.defaultStep)
  }

  def computeId(e: String, t: ExprType, f: Long): String = {
    Strings.zeroPad(Hash.sha1bytes(s"$f~$t~$e"), 40)
  }
}
