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
package com.netflix.atlas.eval.stream

import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import akka.util.ByteString
import com.netflix.atlas.eval.model.AggrDatapoint
import com.netflix.atlas.eval.model.LwcDataExpr
import com.netflix.atlas.eval.model.LwcDatapoint
import com.netflix.atlas.eval.model.LwcDiagnosticMessage
import com.netflix.atlas.eval.model.LwcHeartbeat
import com.netflix.atlas.eval.model.LwcMessages
import com.netflix.atlas.eval.model.LwcSubscription
import com.typesafe.scalalogging.Logger

/**
  * Process the SSE output from an LWC service and convert it into a stream of
  * [[AggrDatapoint]]s that can be used for evaluation.
  */
private[stream] class LwcToAggrDatapoint(context: StreamContext)
    extends GraphStage[FlowShape[ByteString, AggrDatapoint]] {

  private val logger = Logger(getClass)

  private val badMessages = context.registry.counter("atlas.eval.badMessages")

  private val in = Inlet[ByteString]("LwcToAggrDatapoint.in")
  private val out = Outlet[AggrDatapoint]("LwcToAggrDatapoint.out")

  override val shape: FlowShape[ByteString, AggrDatapoint] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler with OutHandler {

      private[this] var state: Map[String, LwcDataExpr] = Map.empty

      // HACK: needed until we can plumb the actual source through the system
      private var nextSource: Int = 0

      override def onPush(): Unit = {
        val message = grab(in)
        try {
          val parsedMsg = LwcMessages.parse(message.utf8String)
          parsedMsg match {
            case sb: LwcSubscription      => updateState(sb)
            case dp: LwcDatapoint         => pushDatapoint(dp)
            case dg: LwcDiagnosticMessage => pushDiagnosticMessage(dg)
            case hb: LwcHeartbeat         => pushHeartbeat(hb)
            case _                        => pull(in)
          }
        } catch {
          case e: Exception =>
            val messageString = toString(message)
            logger.warn(s"failed to process message [$messageString]", e)
            badMessages.increment()
            pull(in)
        }
      }

      private def toString(bytes: ByteString): String = {
        val builder = new StringBuilder()
        bytes.foreach { b =>
          val c = b & 0xFF
          if (isPrintable(c))
            builder.append(c.asInstanceOf[Char])
          else if (c <= 0xF)
            builder.append("\\x0").append(Integer.toHexString(c))
          else
            builder.append("\\x").append(Integer.toHexString(c))
        }
        builder.toString()
      }

      private def isPrintable(c: Int): Boolean = {
        c >= 32 && c < 127
      }

      private def updateState(sub: LwcSubscription): Unit = {
        state ++= sub.metrics.map(m => m.id -> m).toMap
        pull(in)
      }

      private def pushDatapoint(dp: LwcDatapoint): Unit = {
        state.get(dp.id) match {
          case Some(sub) =>
            // TODO, put in source, for now make it random to avoid dedup
            nextSource += 1
            val expr = sub.expr
            val step = sub.step
            push(
              out,
              AggrDatapoint(dp.timestamp, step, expr, nextSource.toString, dp.tags, dp.value)
            )
          case None =>
            pull(in)
        }
      }

      private def pushDiagnosticMessage(diagMsg: LwcDiagnosticMessage): Unit = {
        state.get(diagMsg.id).foreach { sub =>
          context.log(sub.expr, diagMsg.message)
        }
        pull(in)
      }

      private def pushHeartbeat(hb: LwcHeartbeat): Unit = {
        push(out, AggrDatapoint.heartbeat(hb.timestamp, hb.step))
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
