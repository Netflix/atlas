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

import com.netflix.atlas.core.util.IdMap
import com.netflix.atlas.core.util.SortedTagMap
import com.netflix.spectator.api.Id
import com.typesafe.config.Config

/**
  * Verifies that the tags contain a specified key. Sample config:
  *
  * ```
  * key = name
  * ```
  */
case class HasKeyRule(key: String) extends Rule {

  override def validate(tags: SortedTagMap): ValidationResult = {
    if (tags.contains(key))
      ValidationResult.Pass
    else
      failure(s"missing key '$key'", tags)
  }

  private def containsKey(id: Id): Boolean = {
    val size = id.size()
    var i = 1 // skip name
    while (i < size) {
      val k = id.getKey(i)
      // The id tags are sorted by key, so if the search key is less
      // than the key from the id, then it will not be present and we
      // can short circuit the check.
      if (key <= k) return key == k
      i += 1
    }
    false
  }

  override def validate(id: Id): ValidationResult = {
    if (key == "name" || containsKey(id))
      ValidationResult.Pass
    else
      failure(s"missing key '$key'", IdMap(id))
  }
}

object HasKeyRule {

  def apply(config: Config): HasKeyRule = {
    val key = config.getString("key")

    new HasKeyRule(key)
  }
}
