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

/**
  * Helper for rules that can be checked using a single key and value pair.
  */
trait TagRule extends Rule {

  override def validate(tags: SortedTagMap): ValidationResult = {
    val size = tags.size
    var i = 0
    while (i < size) {
      val result = validate(tags.key(i), tags.value(i))
      if (result != TagRule.Pass) return failure(result, tags)
      i += 1
    }
    ValidationResult.Pass
  }

  override def validate(id: Id): ValidationResult = {
    val size = id.size()
    var i = 0
    var result = TagRule.Pass
    while (i < size && result == TagRule.Pass) {
      result = validate(id.getKey(i), id.getValue(i))
      i += 1
    }
    if (result == TagRule.Pass)
      ValidationResult.Pass
    else
      failure(result, IdMap(id))
  }

  /**
    * Check the key/value pair and return `null` if it is valid or a reason string if
    * there is a validation failure. The `null` type for success is used to avoid allocations
    * or other overhead since the validation checks tend to be a hot path.
    */
  def validate(k: String, v: String): String
}

object TagRule {

  /** Null string to make the code easier to follow when using null for a passing result. */
  val Pass: String = null
}
