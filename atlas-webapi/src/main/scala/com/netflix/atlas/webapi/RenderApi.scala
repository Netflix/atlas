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

import java.io.ByteArrayOutputStream

import akka.actor.ActorRefFactory
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.netflix.atlas.akka.CustomDirectives._
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.chart.DefaultGraphEngine


class RenderApi(implicit val actorRefFactory: ActorRefFactory) extends WebApi {

  private val engine = new DefaultGraphEngine

  def routes: Route = {
    path("api" / "v1" / "render") {
      post {
        parseEntity(json[GraphApi.Response]) { data =>
          val graphDef = data.toGraphDef

          val baos = new ByteArrayOutputStream
          engine.write(graphDef, baos)
          val entity = HttpEntity(MediaTypes.`image/png`, baos.toByteArray)
          complete(HttpResponse(StatusCodes.OK, entity = entity))
        }
      }
    }
  }
}
