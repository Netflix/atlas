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
package com.netflix.atlas.core.util

import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

object TimeWave {

  private val cache = new ConcurrentHashMap[(Long, Long), TimeWave]

  /**
    * Get a cached instance of the time wave function for a given wavelength and step. This should
    * be used where possible to avoid using lots of memory for the precomputed sine values.
    */
  def get(wavelength: Duration, step: Long): TimeWave = {
    cache.computeIfAbsent(wavelength.toMillis -> step, _ => TimeWave(wavelength, step))
  }
}

/**
  * Sine function based on timestamps. The sine values will be pre-computed for a single wavelength
  * and then looked up for all others. This can be significantly faster than using the sine function
  * directly for longer spans.
  *
  * @param wavelength
  *     Span of time for the repeating pattern of the wave.
  * @param step
  *     How often to compute the sine value within the wavelength.
  */
case class TimeWave(wavelength: Duration, step: Long) extends Function1[Long, Double] {

  private val length = (wavelength.toMillis / step).toInt

  private val sineValues = {
    val lambda = 2 * scala.math.Pi / wavelength.toMillis
    val values = new Array[Double](length)
    var i = 0
    while (i < length) {
      values(i) = scala.math.sin(i * step * lambda)
      i += 1
    }
    values
  }

  def apply(t: Long): Double = {
    val i = ((t / step) % length).toInt
    sineValues(i)
  }
}
