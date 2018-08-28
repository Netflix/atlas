/*
 * Copyright 2014-2018 Netflix, Inc.
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

import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import com.netflix.atlas.eval.model.AggrDatapoint
import com.netflix.atlas.eval.model.LwcDataExpr
import com.netflix.atlas.eval.model.LwcDatapoint
import com.netflix.atlas.eval.model.LwcSubscription
import com.netflix.atlas.json.Json

/**
  * Process the SSE output from an LWC service and convert it into a stream of
  * [[AggrDatapoint]]s that can be used for evaluation.
  */
private[stream] class LwcToAggrDatapoint extends GraphStage[FlowShape[String, AggrDatapoint]] {

  private val in = Inlet[String]("LwcToAggrDatapoint.in")
  private val out = Outlet[AggrDatapoint]("LwcToAggrDatapoint.out")

  override val shape: FlowShape[String, AggrDatapoint] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler with OutHandler {
      import LwcToAggrDatapoint._

      private var state: Map[String, LwcDataExpr] = Map.empty

      // HACK: needed until we can plumb the actual source through the system
      private var nextSource: Int = 0

      override def onPush(): Unit = {
        grab(in).trim match {
          case msg: String if msg.startsWith(subscribePrefix)  => updateState(msg)
          case msg: String if msg.startsWith(metricDataPrefix) => pushDatapoint(msg)
          case msg: String                                     => ignoreMessage(msg)
        }
      }

      private def updateState(msg: String): Unit = {
        val json = msg.substring(subscribePrefix.length)
        val sub = Json.decode[LwcSubscription](json)
        state ++= sub.metrics.map(m => m.id -> m).toMap
        pull(in)
      }

      private def pushDatapoint(msg: String): Unit = {
        val json = msg.substring(metricDataPrefix.length)
        val d = Json.decode[LwcDatapoint](json)
        state.get(d.id) match {
          case Some(sub) =>
            // TODO, put in source, for now make it random to avoid dedup
            nextSource += 1
            val expr = sub.expr
            val step = sub.frequency
            push(out, AggrDatapoint(d.timestamp, step, expr, nextSource.toString, d.tags, d.value))
          case None =>
            pull(in)
        }
      }

      private def ignoreMessage(msg: String): Unit = {
        pull(in)
      }

      override def onPull(): Unit = {
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        completeStage()
      }

      setHandlers(in, out, this)
    }
  }
}

object LwcToAggrDatapoint {
  private val subscribePrefix = "info: subscribe "
  private val metricDataPrefix = "data: metric "
}
