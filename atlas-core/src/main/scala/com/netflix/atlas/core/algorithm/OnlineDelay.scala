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
package com.netflix.atlas.core.algorithm

/**
  * Delays the values by the window size. This is similar to the `:offset` operator
  * except that it can be applied to any input line instead of just changing the time
  * window fetched with a DataExpr. Short delays can be useful for alerting to detect
  * changes in slightly shifted trend lines.
  */
case class OnlineDelay(buf: RollingBuffer) extends OnlineAlgorithm {

  override def next(v: Double): Double = buf.add(v)

  override def reset(): Unit = buf.clear()

  override def isEmpty: Boolean = buf.isEmpty

  override def state: AlgoState = {
    AlgoState("delay", "buffer" -> buf.state)
  }
}

object OnlineDelay {

  def apply(n: Int): OnlineDelay = new OnlineDelay(RollingBuffer(n))

  def apply(state: AlgoState): OnlineDelay = {
    apply(RollingBuffer(state.getState("buffer")))
  }
}
