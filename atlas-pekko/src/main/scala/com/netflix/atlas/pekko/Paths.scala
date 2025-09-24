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
package com.netflix.atlas.pekko

import java.util.regex.Pattern

import org.apache.pekko.actor.ActorPath
import com.typesafe.config.Config

/** Helper for creating mapping functions to extract a tag value based on an actor path. */
object Paths {

  type Mapper = ActorPath => String

  /** Create a mapping function from a config object using the `path-pattern` field. */
  def createMapper(config: Config): Mapper = createMapper(config.getString("path-pattern"))

  /** Create a mapper from a regex string. */
  def createMapper(pattern: String): Mapper = createMapper(Pattern.compile(pattern))

  /**
    * Creates a mapping function that extracts a tag value based on the
    * pattern. The pattern should have a single capturing group that contains
    * the string to use for the tag value.
    */
  def createMapper(pattern: Pattern): Mapper = { path =>
    {
      val matcher = pattern.matcher(path.toString)
      if (matcher.matches() && matcher.groupCount() == 1) matcher.group(1) else "uncategorized"
    }
  }
}
