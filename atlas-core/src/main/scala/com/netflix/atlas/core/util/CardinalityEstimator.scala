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

import org.apache.datasketches.cpc.CpcSketch

import java.util.concurrent.atomic.AtomicLong

/**
  * Utility for cheaply estimating the number of distinct values for a set of objects.
  */
trait CardinalityEstimator {

  /**
    * Update the estimate with the provided value. The object should have a good hash code
    * implementation that is likely to be distinct for different values of the object.
    */
  def update(obj: AnyRef): Unit

  /** Return the current estimate for the number of distinct values seen. */
  def cardinality: Long

  override def toString: String = cardinality.toString
}

object CardinalityEstimator {

  /**
    * Create a new estimator instance using the [CPC] algorithm.
    *
    * [CPC]: https://datasketches.apache.org/docs/CPC/CPC.html
    *
    * @param lgK
    *     Higher lgK means higher accuracy but higher space (`2^lgK` bytes), can be in range
    *     of [4,26]. For lgk=9, accuracy is more than 97% for SHA1 hashed tags.
    * @return estimator instance
    */
  def newEstimator(lgK: Int = 9): CardinalityEstimator = {
    new CpcEstimator(lgK)
  }

  // It's expected to be updated by a single thread, while reading cardinality is thread-safe.
  private class CpcEstimator(val lgK: Int) extends CardinalityEstimator {

    private val sketch = new CpcSketch(lgK)
    private val _cardinality = new AtomicLong()

    override def update(obj: AnyRef): Unit = {
      sketch.update(obj.hashCode())
      _cardinality.set(sketch.getEstimate.longValue())
    }

    override def cardinality: Long = _cardinality.get()
  }
}
