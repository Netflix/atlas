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
package com.netflix.atlas.core.validation

import com.typesafe.config.Config

/**
  * Verifies that the values are within the specified length bounds. Sample config:
  *
  * ```
  * min-length = 2
  * max-length = 60
  * ```
  */
case class ValueLengthRule(minLength: Int, maxLength: Int) extends TagRule {

  def validate(k: String, v: String): String = {
    v.length match {
      case len if len > maxLength =>
        s"value too long: $k = [$v] ($len > $maxLength)"
      case len if len < minLength =>
        s"value too short: $k = [$v] ($len < $minLength)"
      case _ =>
        TagRule.Pass
    }
  }
}

object ValueLengthRule {

  def apply(config: Config): ValueLengthRule = {
    val minLength = config.getInt("min-length")
    val maxLength = config.getInt("max-length")

    new ValueLengthRule(minLength, maxLength)
  }
}
