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
package com.netflix.atlas.webapi

import org.apache.pekko.actor.ActorRefFactory
import org.apache.pekko.http.scaladsl.model.ContentTypes
import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.MediaTypes
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.headers.RawHeader
import org.apache.pekko.pattern.ask
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.RequestContext
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.server.RouteResult
import com.netflix.atlas.core.index.TagQuery
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.QueryVocabulary
import com.netflix.atlas.core.model.Tag
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.json.Json
import com.netflix.atlas.pekko.CustomDirectives.*
import com.netflix.atlas.pekko.WebApi

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*
import scala.util.Failure

class TagsApi(implicit val actorRefFactory: ActorRefFactory) extends WebApi {

  import com.netflix.atlas.webapi.TagsApi.*
  private implicit val ec: ExecutionContext = actorRefFactory.dispatcher

  private val dbRef = actorRefFactory.actorSelection("/user/db")

  def routes: Route = {
    endpointPathPrefix("api" / "v1" / "tags") {
      pathEndOrSingleSlash {
        handleReq(None)
      } ~
      path(Remaining) { path =>
        handleReq(Some(path))
      }
    }
  }

  private def handleReq(path: Option[String]): Route = {
    get {
      extractRequestContext { ctx =>
        val req = toRequest(ctx, path)
        val limit = req.limit
        _ =>
          ask(dbRef, req.toDbRequest)(60.seconds).map {
            case TagListResponse(vs) if req.useText => asText(tagString(vs), offsetTag(limit, vs))
            case KeyListResponse(vs) if req.useText =>
              asText(vs.mkString("\n"), offsetString(limit, vs))
            case ValueListResponse(vs) if req.useText =>
              asText(vs.mkString("\n"), offsetString(limit, vs))
            case TagListResponse(vs) if req.useJson   => asJson(vs, offsetTag(limit, vs))
            case KeyListResponse(vs) if req.useJson   => asJson(vs, offsetString(limit, vs))
            case ValueListResponse(vs) if req.useJson => asJson(vs, offsetString(limit, vs))
            case Failure(t)                           => throw t
            case v                                    => throw new MatchError(v)
          }
      }
    }
  }

  private def toRequest(ctx: RequestContext, path: Option[String]): Request = {
    val params = ctx.request.uri.query()
    Request(
      key = path,
      version = params.get("v"),
      query = params.get("q"),
      format = params.get("format"),
      offset = params.get("offset"),
      verbose = params.get("verbose").contains("1"),
      limit = params.get("limit").fold(ApiSettings.maxTagLimit)(_.toInt)
    )
  }

  private def tagString(t: Tag): String = s"${t.key}\t${t.value}\t${t.count}"

  private def tagString(ts: List[Tag]): String = ts.map(tagString).mkString("\n")

  private def offsetString(limit: Int, vs: List[String]): Option[String] = {
    if (vs.size < limit) None else Some(vs.last)
  }

  private def offsetTag(limit: Int, vs: List[Tag]): Option[String] = {
    if (vs.size < limit) None
    else {
      val t = vs.last
      Some(s"${t.key},${t.value}")
    }
  }

  private def asJson(data: AnyRef, offset: Option[String]): RouteResult = {
    val entity = HttpEntity(MediaTypes.`application/json`, Json.encode(data))
    val headers = offset.map(v => RawHeader(offsetHeader, v)).toList
    RouteResult.Complete(HttpResponse(StatusCodes.OK, headers, entity))
  }

  private def asText(data: String, offset: Option[String]): RouteResult = {
    val entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, data)
    val headers = offset.map(v => RawHeader(offsetHeader, v)).toList
    RouteResult.Complete(HttpResponse(StatusCodes.OK, headers, entity))
  }
}

object TagsApi {

  val offsetHeader = "x-nflx-atlas-next-offset"

  private val queryInterpreter = new Interpreter(QueryVocabulary.allWords)

  case class Request(
    key: Option[String] = None,
    version: Option[String] = None,
    query: Option[String] = None,
    format: Option[String] = None,
    offset: Option[String] = None,
    verbose: Boolean = false,
    limit: Int = 1000
  ) {

    def actualLimit: Int = if (limit > ApiSettings.maxTagLimit) ApiSettings.maxTagLimit else limit

    def useText: Boolean = format.contains("txt")

    def useJson: Boolean = !useText

    def toDbRequest: AnyRef = {
      (key -> verbose) match {
        case (Some(k), true)  => ListTagsRequest(toTagQuery(Query.HasKey(k)))
        case (Some(k), false) => ListValuesRequest(toTagQuery(Query.HasKey(k)))
        case (None, true)     => ListTagsRequest(toTagQuery(Query.True))
        case (None, false)    => ListKeysRequest(toTagQuery(Query.True))
      }
    }

    def toTagQuery(cq: Query): TagQuery = {
      val q = Some(query.fold(cq)(s => Query.And(toQuery(s), cq)))
      TagQuery(q, key, offset.getOrElse(""), limit)
    }

    private def toQuery(s: String): Query = {
      val c = queryInterpreter.execute(s)
      c.stack match {
        case (q: Query) :: Nil => q
        case _ =>
          throw new IllegalStateException(s"expected a single query on the stack")
      }
    }
  }

  case class ListTagsRequest(q: TagQuery)

  case class ListKeysRequest(q: TagQuery)

  case class ListValuesRequest(q: TagQuery)

  case class KeyListResponse(vs: List[String])

  case class ValueListResponse(vs: List[String])

  case class TagListResponse(vs: List[Tag])

  case object EndOfResponse
}
