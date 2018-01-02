/*
 * Copyright 2014-2018 Netflix, Inc.
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

import java.util.regex.Pattern

import com.typesafe.config.Config

/**
 * Verifies that the values match a specified pattern. Sample config:
 *
 * ```
 * pattern = "^[-_.a-zA-Z0-9]{4,60}$"
 * ```
 */
class ValuePatternRule(config: Config) extends TagRule {

  private val pattern = Pattern.compile(config.getString("pattern"))

  def validate(k: String, v: String): ValidationResult = {
    if (pattern.matcher(v).matches()) ValidationResult.Pass else {
      failure(s"value doesn't match pattern '$pattern': [$v]")
    }
  }
}
