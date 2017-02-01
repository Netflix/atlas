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
import akka.http.scaladsl.model.ContentTypes
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.RawHeader
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.akka.ImperativeRequestContext
import com.netflix.atlas.core.model._
import com.netflix.atlas.json.Json


class TagsRequestActor extends Actor with ActorLogging {

  import com.netflix.atlas.webapi.TagsApi._

  val dbRef = context.actorSelection("/user/db")

  var request: Request = _
  var tagsCtx: ImperativeRequestContext = _

  def receive = {
    case v => try innerReceive(v) catch DiagnosticMessage.handleException(sender())
  }

  def innerReceive: Receive = {
    case rc @ ImperativeRequestContext(req: TagsApi.Request, _) =>
      request = req
      tagsCtx = rc
      dbRef.tell(request.toDbRequest, self)
    case TagListResponse(vs)   if request.useText => sendText(tagString(vs), offsetTag(vs))
    case KeyListResponse(vs)   if request.useText => sendText(vs.mkString("\n"), offsetString(vs))
    case ValueListResponse(vs) if request.useText => sendText(vs.mkString("\n"), offsetString(vs))
    case TagListResponse(vs)   if request.useJson => sendJson(vs, offsetTag(vs))
    case KeyListResponse(vs)   if request.useJson => sendJson(vs, offsetString(vs))
    case ValueListResponse(vs) if request.useJson => sendJson(vs, offsetString(vs))
  }

  private def tagString(t: Tag): String = s"${t.key}\t${t.value}\t${t.count}"

  private def tagString(ts: List[Tag]): String = ts.map(tagString).mkString("\n")

  private def offsetString(vs: List[String]): Option[String] = {
    if (vs.size < request.limit) None else Some(vs.last)
  }

  private def offsetTag(vs: List[Tag]): Option[String] = {
    if (vs.size < request.limit) None else {
      val t = vs.last
      Some(s"${t.key},${t.value}")
    }
  }

  private def sendJson(data: AnyRef, offset: Option[String]): Unit = {
    val entity = HttpEntity(MediaTypes.`application/json`, Json.encode(data))
    val headers = offset.map(v => RawHeader(offsetHeader, v)).toList
    tagsCtx.complete(HttpResponse(StatusCodes.OK, headers, entity))
    context.stop(self)
  }

  private def sendText(data: String, offset: Option[String]): Unit = {
    val entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, data)
    val headers = offset.map(v => RawHeader(offsetHeader, v)).toList
    tagsCtx.complete(HttpResponse(StatusCodes.OK, headers, entity))
    context.stop(self)
  }
}

