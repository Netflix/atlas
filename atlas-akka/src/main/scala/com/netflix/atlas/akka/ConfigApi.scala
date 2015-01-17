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
package com.netflix.atlas.akka

import java.io.StringWriter
import java.util.Properties

import akka.actor.ActorRefFactory
import com.netflix.atlas.config.ConfigManager
import com.typesafe.config.Config
import com.typesafe.config.ConfigException
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigRenderOptions
import spray.http._
import spray.routing._


/**
 * API to browse the configuration for atlas. The endpoint listens on `/api/v2/config` and
 * returns a dump of all of the properties. A subtree can be selected by providing a dot
 * separated config path, e.g., `/api/v2/config/a.b.c`. A single query param, `format`, is
 * supported and can be used to dump the data as json, hocon, or properties. The default format
 * is json.
 */
class ConfigApi(val actorRefFactory: ActorRefFactory) extends WebApi {

  import spray.http.StatusCodes._

  private val formats: Map[String, Config => HttpResponse] = Map(
    "hocon"      -> formatHocon _,
    "json"       -> formatJson _,
    "properties" -> formatProperties _
  )

  def routes: RequestContext => Unit = {
    pathPrefix("api" / "v2" / "config") {
      pathEndOrSingleSlash {
        get { ctx => doGet(ctx, None) }
      } ~
      path(Rest) { path =>
        get { ctx => doGet(ctx, Some(path)) }
      }
    }
  }

  private def doGet(ctx: RequestContext, path: Option[String]): Unit = {
    val format = ctx.request.uri.query.get("format").getOrElse("json")
    if (formats.contains(format)) {
      val config = ConfigManager.current
      path match {
        case Some(p) if !config.hasPath(p) =>
          sendError(ctx, NotFound, s"no matching path '$p'")
        case Some(p) =>
          doGetConfig(ctx, format, getPathValue(config, p))
        case None =>
          doGetConfig(ctx, format, config)
      }
    } else {
      val fmtList = formats.keySet.toList.sortWith(_ < _).mkString(", ")
      sendError(ctx, BadRequest, s"unknown format '$format', valid formats are: $fmtList")
    }
  }

  private def getPathValue(config: Config, p: String): Config = {
    import scala.collection.JavaConversions._
    try config.getConfig(p) catch {
      case e: ConfigException.WrongType =>
        ConfigFactory.parseMap(Map("value" -> config.getString(p)))
    }
  }

  private def doGetConfig(ctx: RequestContext, format: String, config: Config): Unit = {
    try { ctx.responder ! formats(format)(config) } catch handleException(ctx)
  }

  private def formatHocon(config: Config): HttpResponse = {
    val str = config.root.render
    val entity = HttpEntity(MediaTypes.`text/plain`, str)
    HttpResponse(status = StatusCodes.OK, entity = entity)
  }

  private def formatJson(config: Config): HttpResponse = {
    val opts = ConfigRenderOptions.defaults.
      setJson(true).
      setComments(false).
      setOriginComments(false)
    val str = config.root.render(opts)
    val entity = HttpEntity(MediaTypes.`application/json`, str)
    HttpResponse(status = StatusCodes.OK, entity = entity)
  }

  private def formatProperties(config: Config): HttpResponse = {
    import scala.collection.JavaConversions._
    val props = new Properties
    config.entrySet.foreach { t => props.setProperty(t.getKey, s"${t.getValue.unwrapped}") }

    val writer = new StringWriter
    props.store(writer, null)
    val entity = HttpEntity(MediaTypes.`text/plain`, writer.toString)
    HttpResponse(status = StatusCodes.OK, entity = entity)
  }
}
