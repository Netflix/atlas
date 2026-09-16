/*
 * Copyright 2014-2026 Netflix, Inc.
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
package com.netflix.atlas.core.model

import java.time.Duration

import com.netflix.atlas.core.stacklang.StackItem

/**
  * Window size for the rolling operators. It can either be a fixed number of datapoints or
  * an amount of time that is converted to a number of datapoints using the step size for the
  * evaluation.
  */
sealed trait RollingWindow extends StackItem {

  /**
    * Number of datapoints to include in the window, including the current value, when
    * evaluating with a given step size.
    *
    * @param step
    *     Step size for the evaluation in milliseconds.
    * @return
    *     Number of datapoints for the window, always at least 1.
    */
  def period(step: Long): Int

  override def toString: String = {
    val builder = new java.lang.StringBuilder()
    append(builder)
    builder.toString
  }
}

object RollingWindow {

  private val MaxSeconds = Long.MaxValue / 1000L

  /** Create a window for a number of datapoints, or `None` if the size is not positive. */
  def steps(n: Int): Option[RollingWindow] = if (n > 0) Some(Steps(n)) else None

  /** Create a window for an amount of time, or `None` if the duration is not positive. */
  def time(duration: Duration): Option[RollingWindow] = {
    if (duration.isZero || duration.isNegative) None else Some(Time(duration))
  }

  /**
    * Number of datapoints covered by a duration for a given step size, rounded down to the
    * nearest step boundary. The result can be zero or negative if the duration is smaller than
    * the step or negative, callers are expected to handle that based on the desired behavior
    * for a window that does not cover a full interval.
    *
    * Absurdly large durations are clamped rather than allowed to overflow. Such a window is
    * not usable in practice, the buffer for it could not be allocated, but it should fail
    * based on the size, which is reported as a user error, rather than an arithmetic error
    * that would show up as a server failure.
    */
  def datapoints(duration: Duration, step: Long): Int = {
    // Duration.toMillis can overflow in either the multiply or the add, so the bounds are
    // checked inclusively rather than allowing a duration with the maximum number of seconds
    // plus a fractional part through.
    val seconds = duration.getSeconds
    val millis =
      if (seconds >= MaxSeconds) Long.MaxValue
      else if (seconds <= -MaxSeconds) Long.MinValue
      else duration.toMillis
    val n = millis / step
    if (n > Int.MaxValue) Int.MaxValue else if (n < Int.MinValue) Int.MinValue else n.toInt
  }

  /**
    * Window based on a fixed number of datapoints. The amount of time covered by the window
    * will change if the step size changes, for example when zooming out on a graph.
    */
  case class Steps(n: Int) extends RollingWindow {

    require(n > 0, s"window size must be positive (n=$n)")

    override def period(step: Long): Int = n

    override def append(builder: java.lang.StringBuilder): Unit = builder.append(n)
  }

  /**
    * Window based on an amount of time. The duration is rounded down to the nearest step
    * boundary, so a 5m window with a 2m step will result in a 4m window with two datapoints.
    * If the step size is larger than the window, then a single datapoint is used. A single
    * interval already covers at least the requested amount of time, so that is preferred over
    * rounding down to an empty window.
    */
  case class Time(duration: Duration) extends RollingWindow {

    require(
      !duration.isZero && !duration.isNegative,
      s"window size must be positive (duration=$duration)"
    )

    override def period(step: Long): Int = math.max(1, datapoints(duration, step))

    override def append(builder: java.lang.StringBuilder): Unit = builder.append(duration)
  }
}
