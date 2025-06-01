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

/**
  * Utilities for computing and rounding times based on the step size for a dataset.
  */
class Step private (allowedStepSizesForBlock: List[Long]) {

  import Step.*

  /**
    * Round an arbitrary step to the next largest allowed step size.
    */
  def round(primary: Long, step: Long): Long = {
    val max = math.max(primary, step)
    allowedStepSizesForBlock.find(_ >= max).getOrElse(roundToDayBoundary(step))
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
    val datapoints = (end - start) / primary
    val minStep = datapointsPerPixel(datapoints, width) * primary
    round(primary, minStep)
  }
}

object Step {

  private final val oneSecond = 1000L
  private final val oneMinute = 60000L
  private final val oneHour = 60 * oneMinute
  private final val oneDay = 24 * oneHour

  private final val allowedStepSizes = {
    val subSecond = List(1L, 5L, 10L, 50L, 100L, 500L)
    val div60 = List(1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30)
    val subMinute = div60.map(_ * oneSecond)
    val subHour = div60.map(_ * oneMinute)
    val subDay = List(1, 2, 3, 4, 6, 8, 12).map(_ * oneHour)
    subSecond ::: subMinute ::: subHour ::: subDay
  }

  private def datapointsPerPixel(datapoints: Long, width: Int): Long = {
    val v = datapoints / width
    if (datapoints % width == 0) v else v + 1
  }

  private def roundToDayBoundary(step: Long): Long = {
    if (step % oneDay == 0)
      step
    else
      step / oneDay * oneDay + oneDay
  }

  /**
    * Create a helper instance for a given underlying block size. The allowed steps must
    * evenly divide or be even multiples of the block step. The block step is the number
    * of milliseconds for a block of data in the underlying storage.
    */
  def forBlockStep(blockStep: Long): Step = {
    val stepsForBlock = allowedStepSizes.filter { s =>
      if (s <= blockStep)
        blockStep % s == 0
      else
        s % blockStep == 0
    }
    new Step(stepsForBlock)
  }
}
