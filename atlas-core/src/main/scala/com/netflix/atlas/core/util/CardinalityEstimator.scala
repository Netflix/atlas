/*
 * Copyright 2014-2020 Netflix, Inc.
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

trait CardinalityEstimator {
  def update(obj: AnyRef): Unit
  def cardinality: Int
}

private class CpcEstimator(val lgK: Int) extends CardinalityEstimator {
  private val sketch = new CpcSketch(lgK)

  override def update(obj: AnyRef): Unit = {
    sketch.update(obj.hashCode())
  }

  override def cardinality: Int = sketch.getEstimate.toInt
}

object CardinalityEstimator {

  /**
    * Create a new estimator instance.
    * @param lgK  Higher lgK means higher accuracy but higher space (2^lgK bytes), can be in range
    *             of [4,26]. For lgk=9, accuracy is more than 97% for SHA1 hashed tags.
    * @return estimator instance
    */
  def newEstimator(lgK: Int = 9): CardinalityEstimator = {
    new CpcEstimator(lgK)
  }

}
