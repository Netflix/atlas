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
package com.netflix.atlas.config

import java.util.concurrent.atomic.AtomicReference

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

/**
 * Keeps reference to global config object that can updated during application initialization.
 */
object ConfigManager {

  private val configRef: AtomicReference[Config] = new AtomicReference(ConfigFactory.load())

  /** Return the current global config object. */
  def current: Config = configRef.get

  /** Set the global config to `c`. */
  def set(c: Config): Unit = configRef.set(c)

  /** Update the global config object setting it to `c.withFallback(current)`. */
  def update(c: Config): Unit = {
    var tmp = configRef.get
    while (!configRef.compareAndSet(tmp, c.withFallback(tmp).resolve())) {
      tmp = configRef.get
    }
  }
}
