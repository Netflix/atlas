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
package com.netflix.atlas.core.util


/**
 * Utilities for computing and rounding times based on the step size for a dataset.
 */
object Step {

  private final val oneSecond = 1000L
  private final val oneMinute = 60000L
  private final val oneHour = 60 * oneMinute
  private final val oneDay = 24 * oneHour

  private final val allowedStepSizes = {
    val div60 = List(1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30)
    val subMinute = div60.map(_ * oneSecond)
    val subHour = div60.map(_ * oneMinute)
    val subDay = List(1, 2, 3, 6, 8, 12).map(_ * oneHour)
    val subMonth = List(1, 7).map(_ * oneDay)
    subMinute ::: subHour ::: subDay ::: subMonth
  }

  private final val autoStepSizes = {
    val div60 = List(1, 5, 10, 30)
    val subMinute = div60.map(_ * oneSecond)
    val subHour = div60.map(_ * oneMinute)
    val subDay = List(1, 6, 12).map(_ * oneHour)
    val subMonth = List(1, 7).map(_ * oneDay)
    subMinute ::: subHour ::: subDay ::: subMonth
  }

  private final val largestStep = allowedStepSizes.last

  private final val stepSet = allowedStepSizes.toSet

  private def validate(s: Long) {
    require(stepSet.contains(s), s"step size must a member of the set: $allowedStepSizes")
  }

  private def datapointsPerPixel(datapoints: Long, width: Int): Long = {
    val v = datapoints / width
    if (datapoints % width == 0) v else v + 1
  }

  /**
   * Round an arbitrary step to the next largest allowed step size.
   */
  def round(primary: Long, step: Long): Long = {
    val max = math.max(primary, step)
    allowedStepSizes.find(_ >= max).getOrElse(largestStep)
  }

  /**
   * Compute an appropriate step size so that each datapoint shown for the graph has at least one
   * pixel. The computed step must be a multiple of the primary step that is passed in.
   *
   * @param primary  step size that the input data is stored with
   * @param width    width in pixels available for rendering the line
   * @param start    start time for the graph
   * @param end      end time for the graph
   */
  def compute(primary: Long, width: Int, start: Long, end: Long): Long = {
    validate(primary)
    val datapoints = (end - start) / primary
    val minStep = datapointsPerPixel(datapoints, width) * primary
    val max = math.max(primary, minStep)
    autoStepSizes.find(_ >= max).getOrElse(largestStep)
  }
}
