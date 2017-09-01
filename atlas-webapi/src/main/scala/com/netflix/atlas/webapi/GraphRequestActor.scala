/*
 * Copyright 2014-2017 Netflix, Inc.
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
package com.netflix.atlas.webapi

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCodes
import akka.util.ByteString
import com.fasterxml.jackson.core.JsonProcessingException
import com.netflix.atlas.akka.ImperativeRequestContext
import com.netflix.atlas.core.model._
import com.netflix.atlas.core.util.PngImage
import com.netflix.spectator.api.Registry


class GraphRequestActor(registry: Registry) extends Actor with ActorLogging {

  import com.netflix.atlas.webapi.GraphApi._

  private val errorId = registry.createId("atlas.graph.errorImages")

  private val dbRef = context.actorSelection("/user/db")

  private var request: Request = _
  private var graphCtx: ImperativeRequestContext = _

  def receive = {
    case v =>
      try innerReceive(v) catch {
        case t: Exception if request != null && request.shouldOutputImage =>
          // When viewing a page in a browser an error response is not rendered. To make it more
          // clear to the user we return a 200 with the error information encoded into an image.
          sendErrorImage(t, request.flags.width, request.flags.height)
          context.stop(self)
        case t: Throwable =>
          graphCtx.fail(t)
          context.stop(self)
      }
  }

  def innerReceive: Receive = {
    case ctx @ ImperativeRequestContext(req: Request, _) =>
      request = req
      graphCtx = ctx
      dbRef.tell(request.toDbRequest, self)
    case DataResponse(data) =>
      sendImage(data)
  }

  private def sendErrorImage(t: Throwable, w: Int, h: Int): Unit = {
    val simpleName = t.getClass.getSimpleName
    registry.counter(errorId.withTag("error", simpleName)).increment()

    val msg = s"$simpleName: ${t.getMessage}"
    val errorImg = t match {
      case userException @ (_: IllegalArgumentException | _: IllegalStateException | _: JsonProcessingException) =>
        PngImage.userError(msg, w, h)
      case _ =>
        PngImage.systemError(msg, w, h)
    }
    val image = HttpEntity(MediaTypes.`image/png`, errorImg.toByteArray)
    graphCtx.complete(HttpResponse(status = StatusCodes.OK, entity = image))
  }

  private def sendImage(data: Map[DataExpr, List[TimeSeries]]): Unit = {
    val result = GraphEval.render(request, data)
    val entity = HttpEntity.Strict(request.contentType, ByteString(result.data))
    graphCtx.complete(HttpResponse(StatusCodes.OK, entity = entity))
    context.stop(self)
  }
}

