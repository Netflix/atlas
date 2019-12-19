/*
 * Copyright 2014-2019 Netflix, Inc.
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

import akka.http.scaladsl.model.ws.Message
import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.eval.model.LwcExpression
import com.netflix.atlas.json.Json
import com.netflix.atlas.lwcapi.SubscribeApi.ErrorMsg
import com.typesafe.scalalogging.StrictLogging

import scala.util.control.NonFatal

/**
  * Operator that register a WebSocket Stream, subscribe/update Expressions on demand as they flow in,
  * and produce output message stream a single "Source".
  */
private[lwcapi] class WebSocketSessionManager(
  val streamId: String,
  val registerFunc: String => (QueueHandler, Source[Message, Unit]),
  val subscribeFunc: (String, List[ExpressionMetadata]) => List[ErrorMsg]
) extends GraphStage[FlowShape[String, Source[Message, Unit]]]
    with StrictLogging {
  private val in = Inlet[String]("WebSocketSession.in")
  private val out = Outlet[Source[Message, Unit]]("WebSocketSession.out")

  override val shape: FlowShape[String, Source[Message, Unit]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler with OutHandler {

      var dataSourcePushed = false
      var queueHandler: QueueHandler = _
      var dataSource: Source[Message, Unit] = _

      setHandlers(in, out, this)

      override def preStart(): Unit = {
        val (_queueHandler, _dataSource) = registerFunc(streamId)
        queueHandler = _queueHandler
        dataSource = _dataSource
      }

      override def onPush(): Unit = {
        val exprStr = grab(in)

        var newExpressions: List[ExpressionMetadata] = null
        try {
          newExpressions = Json
            .decode[List[LwcExpression]](exprStr)
            .map(v => ExpressionMetadata(v.expression, v.step))
        } catch {
          case NonFatal(e) => {
            queueHandler.offer(
              DiagnosticMessage.error(s"cannot decode json: ${exprStr}, error: ${e.getMessage}")
            )
          }
          //need to pull here: no push out so no pull from downstream
          pull(in)
          return // stop here on json parsing error
        }

        val errors = subscribeFunc(streamId, newExpressions) //update subscription here
        errors.foreach { error =>
          val msg = DiagnosticMessage.error(s"[${error.expression}] ${error.message}")
          queueHandler.offer(msg)
        }

        //only push out dataSource once
        if (!dataSourcePushed) {
          push(out, dataSource)
          dataSourcePushed = true
        } else {
          //need to pull here: no push out so no pull from downstream
          pull(in)
        }
      }

      override def onPull(): Unit = {
        pull(in)
      }
    }
  }
}
