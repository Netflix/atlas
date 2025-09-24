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
  * Push a value through a sequence of online algorithms and return the result.
  */
case class Pipeline(stages: List[OnlineAlgorithm]) extends OnlineAlgorithm {

  override def next(v: Double): Double = {
    stages.foldLeft(v) {
      case (acc, stage) => stage.next(acc)
    }
  }

  override def reset(): Unit = {
    stages.foreach(_.reset())
  }

  override def isEmpty: Boolean = stages.forall(_.isEmpty)

  override def state: AlgoState = {
    AlgoState("pipeline", "stages" -> stages.map(_.state))
  }
}

object Pipeline {

  def apply(state: AlgoState): Pipeline = {
    apply(state.getStateList("stages").map(OnlineAlgorithm.apply))
  }

  def apply(stages: OnlineAlgorithm*): Pipeline = apply(stages.toList)
}
