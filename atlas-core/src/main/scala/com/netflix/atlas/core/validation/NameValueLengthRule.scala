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
  * Verifies that the name and values are within the specified length bounds. The limits for
  * the value of the name key can be set independently from the values of other keys. Sample
  * config:
  *
  * ```
  * name {
  *   min-length = 2
  *   max-length = 80
  * }
  * others {
  *   min-length = 2
  *   max-length = 60
  * }
  * ```
  */
case class NameValueLengthRule(nameRule: ValueLengthRule, valueRule: ValueLengthRule)
    extends TagRule {

  override def validate(k: String, v: String): String = {
    if (k == "name") nameRule.validate(k, v) else valueRule.validate(k, v)
  }
}

object NameValueLengthRule {

  def apply(config: Config): NameValueLengthRule = {
    val nameRule = ValueLengthRule(config.getConfig("name"))
    val valueRule = ValueLengthRule(config.getConfig("others"))
    apply(nameRule, valueRule)
  }
}
