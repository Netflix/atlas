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
package com.netflix.atlas.akka

import akka.actor.ActorRefFactory
import akka.http.scaladsl.model.ContentTypes
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpEntity.ChunkStreamPart
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.Source

class TestApi(val actorRefFactory: ActorRefFactory) extends WebApi {

  import CustomDirectives._

  def routes: Route = {
    path("jsonparse") {
      post {
        parseEntity(json[String]) { v =>
          complete(HttpResponse(status = OK, entity = v))
        }
      }
    } ~
    path("query-parsing-directive") {
      get {
        parameter("regex") { v =>
          val entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, v)
          complete(HttpResponse(status = OK, entity = entity))
        }
      }
    } ~
    path("query-parsing-explicit") {
      get {
        extractRequest { req =>
          val v = req.uri.query().get("regex").get
          val entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, v)
          complete(HttpResponse(status = OK, entity = entity))
        }
      }
    } ~
    accessLog {
      path("chunked") {
        get {
          val source = Source
            .single(ChunkStreamPart("start"))
            .concat(Source((1 until 42).map(i => ChunkStreamPart(i.toString)).toList))
          val entity = HttpEntity.Chunked(ContentTypes.`text/plain(UTF-8)`, source)
          complete(HttpResponse(status = OK, entity = entity))
        }
      }
    }
  }
}
