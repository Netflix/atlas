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
package com.netflix.atlas.config

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

/**
 * Keeps reference to global config object that can updated during application initialization.
 */
object ConfigManager {

  private val logger = LoggerFactory.getLogger(getClass)

  // Copy of the unresolved config object so it can be re-resolved if later updates are applied.
  private var unresolvedRef: Config = ConfigFactory.load(pickClassLoader)

  @volatile private var configRef: Config = unresolvedRef.resolve()

  /** Return the current global config object. */
  def current: Config = configRef

  /** Set the global config to `c`. */
  def set(c: Config): Unit = synchronized {
    unresolvedRef = c
    configRef = unresolvedRef.resolve()
  }

  /** Update the global config object setting it to `c.withFallback(current)`. */
  def update(c: Config): Unit = synchronized {
    unresolvedRef = c.withFallback(unresolvedRef)
    configRef = unresolvedRef.resolve()
  }

  private def pickClassLoader: ClassLoader = {
    val cl = Thread.currentThread().getContextClassLoader
    if (cl != null) cl else {
      val cname = getClass.getName
      logger.warn(s"Thread.currentThread().getContextClassLoader() is null, using loader for $cname")
      getClass.getClassLoader
    }
  }
}
