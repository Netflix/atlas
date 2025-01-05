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

import com.netflix.atlas.core.model.TagKey
import com.netflix.atlas.core.util.IdMap
import com.netflix.atlas.core.util.SortedTagMap
import com.netflix.spectator.api.Id
import com.typesafe.config.Config

/**
  * Verifies that the number of custom user tags are within a specified limit. Sample config:
  *
  * ```
  * limit = 10
  * ```
  */
case class MaxUserTagsRule(limit: Int) extends Rule {

  override def validate(tags: SortedTagMap): ValidationResult = {
    val size = tags.size
    var i = 0
    var count = 0
    while (i < size) {
      if (!TagKey.isRestricted(tags.key(i))) count += 1
      i += 1
    }
    if (count <= limit) ValidationResult.Pass
    else {
      failure(s"too many user tags: $count > $limit", tags)
    }
  }

  override def validate(id: Id): ValidationResult = {
    val size = id.size()
    var i = 1 // skip name
    var count = 1 // name is counted
    while (i < size) {
      if (!TagKey.isRestricted(id.getKey(i))) count += 1
      i += 1
    }
    if (count <= limit) ValidationResult.Pass
    else {
      failure(s"too many user tags: $count > $limit", IdMap(id))
    }
  }
}

object MaxUserTagsRule {

  def apply(config: Config): MaxUserTagsRule = {
    val limit = config.getInt("limit")

    new MaxUserTagsRule(limit)
  }
}
