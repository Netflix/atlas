/*
 * Copyright 2015 Netflix, Inc.
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
package com.netflix.atlas.akka

import akka.actor.ActorPath
import com.typesafe.config.ConfigFactory


object Paths {
  private val config = ConfigFactory.load()
  private val Path = config.getString("atlas.akka.pathPattern").r

  /**
   * Summarizes a path for use in a metric tag.
   */
  def tagValue(path: ActorPath): String = {
    path.toString match {
      case Path(v) => v
      case _       => "uncategorized"
    }
  }
}
