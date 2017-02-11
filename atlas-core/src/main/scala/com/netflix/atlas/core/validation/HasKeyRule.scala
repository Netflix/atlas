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
package com.netflix.atlas.core.validation

import com.netflix.atlas.core.util.SmallHashMap
import com.typesafe.config.Config

/**
 * Verifies that the tags contain a specified key. Sample config:
 *
 * ```
 * key = name
 * ```
 */
class HasKeyRule(config: Config) extends Rule {
  private val key = config.getString("key")

  def validate(tags: SmallHashMap[String, String]): ValidationResult = {
    if (tags.contains(key)) ValidationResult.Pass else failure(s"missing '$key': ${tags.keys}")
  }
}

