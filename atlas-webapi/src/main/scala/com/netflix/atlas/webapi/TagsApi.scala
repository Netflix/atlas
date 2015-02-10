/*
 * Copyright 2015 Netflix, Inc.
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

import akka.actor.ActorRefFactory
import akka.actor.Props
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.core.index.TagQuery
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.QueryVocabulary
import com.netflix.atlas.core.model.Tag
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.stacklang.StandardVocabulary
import spray.routing.RequestContext

class TagsApi(implicit val actorRefFactory: ActorRefFactory) extends WebApi {

  import com.netflix.atlas.webapi.TagsApi._

  def routes: RequestContext => Unit = {
    pathPrefix("api" / "v1" / "tags") {
      pathEndOrSingleSlash {
        get { ctx => doGet(ctx, None) }
      } ~
      path(Rest) { path =>
        get { ctx => doGet(ctx, Some(path)) }
      }
    }
  }

  private def doGet(ctx: RequestContext, path: Option[String]): Unit = {
    try {
      val reqHandler = actorRefFactory.actorOf(Props(new TagsRequestActor))
      reqHandler.tell(toRequest(ctx, path), ctx.responder)
    } catch handleException(ctx)
  }

  private def toRequest(ctx: RequestContext, path: Option[String]): Request = {
    val params = ctx.request.uri.query
    Request(
      key = path,
      version = params.get("v"),
      query = params.get("q"),
      format = params.get("format"),
      offset = params.get("offset"),
      verbose = params.get("verbose").contains("1"),
      limit = params.get("limit").fold(ApiSettings.maxTagLimit)(_.toInt))
  }

}

object TagsApi {

  val offsetHeader = "x-nflx-atlas-next-offset"

  private val queryInterpreter = new Interpreter(QueryVocabulary.allWords ::: StandardVocabulary.allWords)

  def databaseActor: Class[_] = classOf[TagsRequestActor]

  case class Request(
      key: Option[String] = None,
      version: Option[String] = None,
      query: Option[String] = None,
      format: Option[String] = None,
      offset: Option[String] = None,
      verbose: Boolean = false,
      limit: Int = 1000) {

    def actualLimit: Int = if (limit > ApiSettings.maxTagLimit) ApiSettings.maxTagLimit else limit

    def useText: Boolean = format.contains("txt")

    def useJson: Boolean = !useText

    def toDbRequest: AnyRef = {
      (key -> verbose) match {
        case (Some(k),  true) => ListTagsRequest(toTagQuery(Query.HasKey(k)))
        case (Some(k), false) => ListValuesRequest(toTagQuery(Query.HasKey(k)))
        case (None,     true) => ListTagsRequest(toTagQuery(Query.True))
        case (None,    false) => ListKeysRequest(toTagQuery(Query.True))
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
