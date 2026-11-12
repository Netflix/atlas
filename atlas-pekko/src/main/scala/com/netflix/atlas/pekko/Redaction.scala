/*
 * Copyright 2014-2026 Netflix, Inc.
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
package com.netflix.atlas.pekko

import java.util.Locale

/** Helpers shared by features that redact sensitive values. */
private[pekko] object Redaction {

  /** Placeholder used in place of a redacted value. */
  val placeholder: String = "<redacted>"

  /**
    * Normalize a list of names or patterns from the config so they can be compared ignoring
    * case. Surrounding whitespace is removed and empty entries are dropped.
    */
  def normalize(values: java.util.List[String]): List[String] = {
    import scala.jdk.CollectionConverters.*
    values.asScala.toList.map(_.trim.toLowerCase(Locale.ROOT)).filter(_.nonEmpty)
  }
}
