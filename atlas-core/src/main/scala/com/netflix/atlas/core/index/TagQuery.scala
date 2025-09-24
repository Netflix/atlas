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
package com.netflix.atlas.core.index

import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.Tag

case class TagQuery(
  query: Option[Query],
  key: Option[String] = None,
  offset: String = "",
  limit: Int = Integer.MAX_VALUE
) {

  /** Parse the offset string to a tag object. */
  lazy val offsetTag: Tag = {
    val comma = offset.indexOf(",")
    if (comma == -1)
      Tag(offset, null, Integer.MAX_VALUE)
    else
      Tag(offset.substring(0, comma), offset.substring(comma + 1), Integer.MAX_VALUE)
  }
}
