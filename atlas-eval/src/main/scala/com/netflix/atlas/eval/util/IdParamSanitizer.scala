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
package com.netflix.atlas.eval.util

import java.util.Locale
import java.util.regex.Pattern

/** Helper to sanitize the id parameter value. */
object IdParamSanitizer {

  private val pattern = Pattern.compile("[0-9a-f]{8}|[0-9]{3}|[0-9][.][0-9]|:")

  /**
    * Sanitize id parameter value.
    *
    * @param id
    *     Value for the parameter.
    * @return
    *     Sanitized value. If the input contains something that looks like UUIDs, IP addresses,
    *     instance ids, or arbitrary numbers, then it will return the value `default` instead.
    */
  def sanitize(id: String): String = {
    val lowerId = id.toLowerCase(Locale.US)
    if (pattern.matcher(lowerId).find())
      "default"
    else
      lowerId
  }
}
