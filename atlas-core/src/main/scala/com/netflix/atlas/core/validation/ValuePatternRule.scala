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

import com.netflix.spectator.impl.PatternMatcher
import com.typesafe.config.Config

/**
  * Verifies that the values match a specified pattern. Sample config:
  *
  * ```
  * pattern = "^[-_.a-zA-Z0-9]{4,60}$"
  * ```
  */
case class ValuePatternRule(pattern: PatternMatcher) extends TagRule {

  def validate(k: String, v: String): String = {
    if (pattern.matches(v)) TagRule.Pass
    else {
      s"value doesn't match pattern '$pattern': [$v]"
    }
  }
}

object ValuePatternRule {

  def apply(config: Config): ValuePatternRule = {
    val pattern = PatternMatcher.compile(config.getString("pattern"))

    new ValuePatternRule(pattern)
  }
}
