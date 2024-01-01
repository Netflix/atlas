/*
 * Copyright 2014-2024 Netflix, Inc.
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
import com.netflix.atlas.lwcapi.SubscribeApi.ErrorMsg
import com.netflix.atlas.pekko.DiagnosticMessage
import com.typesafe.scalalogging.StrictLogging

import scala.util.control.NonFatal

/**
  * Operator that register a WebSocket Stream, subscribe/update Expressions on demand as they
  * flow in, and produce output message stream a single "Source".
  */
private[lwcapi] class WebSocketSessionManager(
  val streamMeta: StreamMetadata,
  val registerFunc: StreamMetadata => (QueueHandler, Source[JsonSupport, NotUsed]),
  val subscribeFunc: (String, List[ExpressionMetadata]) => List[ErrorMsg]
) extends GraphStage[FlowShape[AnyRef, Source[JsonSupport, NotUsed]]]
    with StrictLogging {

  private val in = Inlet[AnyRef]("WebSocketSessionManager.in")
  private val out = Outlet[Source[JsonSupport, NotUsed]]("WebSocketSessionManager.out")

  override val shape: FlowShape[AnyRef, Source[JsonSupport, NotUsed]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler with OutHandler {

      var dataSourcePushed = false
      var queueHandler: QueueHandler = _
      var dataSource: Source[JsonSupport, NotUsed] = _

      setHandlers(in, out, this)

      override def preStart(): Unit = {
        val (_queueHandler, _dataSource) = registerFunc(streamMeta)
        queueHandler = _queueHandler
        dataSource = _dataSource
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
          val errors = subscribeFunc(streamMeta.streamId, metadata).map { error =>
            DiagnosticMessage.error(s"[${error.expression}] ${error.message}")
          }
          queueHandler.offer(errors)
        } catch {
          case NonFatal(t) => queueHandler.offer(Seq(DiagnosticMessage.error(t)))
        } finally {
          // Push out dataSource only once
          if (!dataSourcePushed) {
            push(out, dataSource)
            dataSourcePushed = true
          } else {
            // Only pull when no push happened, because push should have triggered a pull
            // from downstream
            pull(in)
          }
        }
      }

      override def onPull(): Unit = {
        pull(in)
      }
    }
  }
}
