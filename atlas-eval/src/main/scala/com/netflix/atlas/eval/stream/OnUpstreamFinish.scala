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
package com.netflix.atlas.eval.stream

import org.apache.pekko.stream.Attributes
import org.apache.pekko.stream.FlowShape
import org.apache.pekko.stream.Inlet
import org.apache.pekko.stream.Outlet
import org.apache.pekko.stream.stage.GraphStage
import org.apache.pekko.stream.stage.GraphStageLogic
import org.apache.pekko.stream.stage.InHandler
import org.apache.pekko.stream.stage.OutHandler

/**
  * Performs the specified action when the upstream has finished. This is used as an
  * alternative to `watchTermination` which seems to get invoked as soon as the source
  * completes. In some cases that is not desirable and the attached action should not
  * run until data from the source has passed through some of the other steps.
  */
private[stream] class OnUpstreamFinish[T](action: => Unit) extends GraphStage[FlowShape[T, T]] {

  private val in = Inlet[T]("OnUpstreamFinish.in")
  private val out = Outlet[T]("OnUpstreamFinish.out")

  override val shape: FlowShape[T, T] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler with OutHandler {

      override def onPush(): Unit = {
        push(out, grab(in))
      }

      override def onPull(): Unit = {
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        completeStage()
        action
      }

      setHandlers(in, out, this)
    }
  }
}
