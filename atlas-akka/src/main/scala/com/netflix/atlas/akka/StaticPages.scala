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

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.config.Config

/**
 * Adds a directive to serve static pages from the 'www' resource directory.
 */
class StaticPages(config: Config) extends WebApi {

  private val defaultPage = config.getString("atlas.akka.static.default-page")

  def routes: Route = {
    pathEndOrSingleSlash {
      redirect(defaultPage, StatusCodes.MovedPermanently)
    } ~
    prefixRoutes
  }

  def prefixRoutes: Route = {

    val staticFiles = pathPrefix("static") {
      pathEndOrSingleSlash {
        getFromResource("www/index.html")
      } ~
      getFromResourceDirectory("www")
    }

    import scala.collection.JavaConverters._
    val singlePagePrefixes = config.getConfigList("atlas.akka.static.single-page-prefixes")
    singlePagePrefixes.asScala.foldLeft(staticFiles) { (acc, cfg) =>
      val prefix = cfg.getString("prefix")
      val resource = cfg.getString("resource")
      acc ~ pathPrefix(prefix) {
        getFromResource(resource)
      }
    }
  }
}
