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
package com.netflix.atlas.lwcapi

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Attributes
import org.apache.pekko.stream.FlowShape
import org.apache.pekko.stream.Inlet
import org.apache.pekko.stream.Outlet
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.stage.GraphStage
import org.apache.pekko.stream.stage.GraphStageLogic
import org.apache.pekko.stream.stage.InHandler
import org.apache.pekko.stream.stage.OutHandler
import org.apache.pekko.util.ByteString
import com.netflix.atlas.eval.model.LwcExpression
import com.netflix.atlas.eval.model.LwcMessages
import com.netflix.atlas.json.JsonSupport
import com.netflix.atlas.pekko.DiagnosticMessage
import com.typesafe.scalalogging.StrictLogging

import scala.util.control.NonFatal

/**
  * Operator that register a WebSocket Stream, subscribe/update Expressions on demand as they
  * flow in, and produce output message stream a single "Source".
  */
private[lwcapi] class WebSocketSessionManager(
  val streamMeta: StreamMetadata,
  val registerFunc: StreamMetadata => Source[JsonSupport, NotUsed],
  val subscribeFunc: (String, List[ExpressionMetadata]) => List[JsonSupport]
) extends GraphStage[FlowShape[AnyRef, Source[JsonSupport, NotUsed]]]
    with StrictLogging {

  private val in = Inlet[AnyRef]("WebSocketSessionManager.in")
  private val out = Outlet[Source[JsonSupport, NotUsed]]("WebSocketSessionManager.out")

  override val shape: FlowShape[AnyRef, Source[JsonSupport, NotUsed]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler with OutHandler {

      private var dataSourcePushed = false
      private var dataSource: Source[JsonSupport, NotUsed] = _

      // Messages that shouldn't get dropped and thus shouldn't go through the queue
      // with other data
      private var highPriorityMessages: List[JsonSupport] = Nil

      setHandlers(in, out, this)

      override def preStart(): Unit = {
        dataSource = registerFunc(streamMeta)
      }

      override def onPush(): Unit = {
        try {
          val lwcExpressions = grab(in) match {
            case str: ByteString => LwcMessages.parseBatch(str).asInstanceOf[List[LwcExpression]]
            case v               => throw new MatchError(s"invalid type: ${v.getClass.getName}")
          }
          val metadata =
            lwcExpressions.map(v => ExpressionMetadata(v.expression, v.exprType, v.step))
          // Update subscription here
          val messages = subscribeFunc(streamMeta.streamId, metadata)
          highPriorityMessages = highPriorityMessages ::: messages
        } catch {
          case NonFatal(t) =>
            highPriorityMessages = DiagnosticMessage.error(t) :: highPriorityMessages
        } finally {
          // Push out dataSource only once
          if (!dataSourcePushed) {
            push(out, dataSource)
            dataSourcePushed = true
          } else {
            // Only pull when no push happened, because push should have triggered a pull
            // from downstream
            onPull()
          }
        }
      }

      override def onPull(): Unit = {
        if (highPriorityMessages.nonEmpty) {
          push(out, Source(highPriorityMessages))
          highPriorityMessages = Nil
        } else {
          pull(in)
        }
      }
    }
  }
}
