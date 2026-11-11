/*
 * Copyright 2014-2026 Netflix, Inc.
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
package com.netflix.atlas.pekko

import java.io.StringWriter
import java.util.Locale
import java.util.Properties

import scala.util.matching.Regex

import org.apache.pekko.actor.ActorRefFactory
import org.apache.pekko.http.scaladsl.model.ContentTypes
import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.MediaTypes
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.RequestContext
import org.apache.pekko.http.scaladsl.server.Route
import CustomDirectives.*
import com.typesafe.config.Config
import com.typesafe.config.ConfigException
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigList
import com.typesafe.config.ConfigObject
import com.typesafe.config.ConfigRenderOptions
import com.typesafe.config.ConfigValue
import com.typesafe.config.ConfigValueFactory
import com.typesafe.config.ConfigValueType

/**
  * API to browse the configuration for atlas. The endpoint listens on `/api/v2/config` and
  * returns a dump of all of the properties. A subtree can be selected by providing a dot
  * separated config path, e.g., `/api/v2/config/a.b.c`. A single query param, `format`, is
  * supported and can be used to dump the data as json, hocon, or properties. The default format
  * is json.
  *
  * Values that look like credentials, based on the patterns in
  * `atlas.pekko.config-api.redact-key-patterns`, are replaced with a placeholder so that
  * they are not exposed. See `ConfigApi.redact` for details.
  */
class ConfigApi(config: Config, implicit val actorRefFactory: ActorRefFactory) extends WebApi {

  private val redactedConfig = ConfigApi.redact(config)

  private val formats: Map[String, Config => HttpResponse] = Map(
    "hocon"      -> formatHocon _,
    "json"       -> formatJson _,
    "properties" -> formatProperties _
  )

  def routes: Route = {
    endpointPathPrefix("api" / "v2" / "config") {
      pathEndOrSingleSlash {
        get { ctx =>
          ctx.complete(doGet(ctx, None))
        }
      } ~
      path(Remaining) { path =>
        get { ctx =>
          ctx.complete(doGet(ctx, Some(path)))
        }
      }
    }
  }

  private def doGet(ctx: RequestContext, path: Option[String]): HttpResponse = {
    val query = ctx.request.uri.query(mode = Uri.ParsingMode.Relaxed)
    val format = query.get("format").getOrElse("json")
    if (formats.contains(format)) {
      path match {
        case Some(p) if !redactedConfig.hasPath(p) =>
          DiagnosticMessage.error(StatusCodes.NotFound, s"no matching path '$p'")
        case Some(p) =>
          formats(format)(getPathValue(redactedConfig, p))
        case None =>
          formats(format)(redactedConfig)
      }
    } else {
      val fmtList = formats.keySet.toList.sortWith(_ < _).mkString(", ")
      val msg = s"unknown format '$format', valid formats are: $fmtList"
      DiagnosticMessage.error(StatusCodes.BadRequest, msg)
    }
  }

  private def getPathValue(config: Config, p: String): Config = {
    import scala.jdk.CollectionConverters.*
    try config.getConfig(p)
    catch {
      case _: ConfigException.WrongType =>
        ConfigFactory.parseMap(Map("value" -> config.getString(p)).asJava)
    }
  }

  private def formatHocon(config: Config): HttpResponse = {
    val str = config.root.render
    val entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, str)
    HttpResponse(status = StatusCodes.OK, entity = entity)
  }

  private def formatJson(config: Config): HttpResponse = {
    val opts =
      ConfigRenderOptions.defaults.setJson(true).setComments(false).setOriginComments(false)
    val str = config.root.render(opts)
    val entity = HttpEntity(MediaTypes.`application/json`, str)
    HttpResponse(status = StatusCodes.OK, entity = entity)
  }

  private def formatProperties(config: Config): HttpResponse = {
    import scala.jdk.CollectionConverters.*
    val props = new Properties
    config.entrySet.asScala.foreach { t =>
      props.setProperty(t.getKey, s"${t.getValue.unwrapped}")
    }

    val writer = new StringWriter
    props.store(writer, null)
    val entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, writer.toString)
    HttpResponse(status = StatusCodes.OK, entity = entity)
  }
}

object ConfigApi {

  import scala.jdk.CollectionConverters.*

  private val patternsKey = "atlas.pekko.config-api.redact-key-patterns"

  /** Password portion of the user info for a URI, e.g. `postgresql://user:password@host`. */
  private val UserInfoPassword = """(://[^/?#@\s:]*):[^/?#@\s]*@""".r

  /** Query or matrix parameter, e.g. `?password=foo` or `;password=foo`. */
  private val Param = """([?&;])([^=?&;#\s]+)=([^&;#\s]*)""".r

  /**
    * Redact values that may contain credentials:
    *
    * - Values for keys where the key name contains one of the configured patterns, ignoring
    *   case, are replaced with a placeholder. If the value is an object, then it will be
    *   searched recursively rather than replaced so that non-sensitive settings are still
    *   visible. Boolean and null values are not considered sensitive.
    * - Within string values, the password portion of URI user info and the values for query
    *   parameters with a name that matches one of the patterns are replaced.
    *
    * Objects nested in lists are also searched.
    */
  private[pekko] def redact(config: Config): Config = {
    val patterns = Redaction.normalize(config.getStringList(patternsKey))

    def isSensitive(name: String): Boolean = {
      val n = name.toLowerCase(Locale.ROOT)
      patterns.exists(n.contains)
    }

    def redactString(str: String): String = {
      val s = UserInfoPassword.replaceAllIn(
        str,
        m => Regex.quoteReplacement(s"${m.group(1)}:${Redaction.placeholder}@")
      )
      Param.replaceAllIn(
        s,
        m => {
          val replacement =
            if (isSensitive(m.group(2))) s"${m.group(1)}${m.group(2)}=${Redaction.placeholder}"
            else m.matched
          Regex.quoteReplacement(replacement)
        }
      )
    }

    def redactKey(key: String, value: ConfigValue): ConfigValue = {
      value.valueType match {
        case ConfigValueType.OBJECT | ConfigValueType.BOOLEAN | ConfigValueType.NULL =>
          redactValue(value)
        case _ if isSensitive(key) =>
          ConfigValueFactory.fromAnyRef(Redaction.placeholder).withOrigin(value.origin)
        case _ =>
          redactValue(value)
      }
    }

    def redactValue(value: ConfigValue): ConfigValue = {
      value match {
        case obj: ConfigObject =>
          val redacted = obj.asScala.map { case (k, v) => k -> redactKey(k, v) }
          ConfigValueFactory.fromMap(redacted.asJava).withOrigin(obj.origin)
        case list: ConfigList =>
          ConfigValueFactory
            .fromIterable(list.asScala.map(redactValue).asJava)
            .withOrigin(list.origin)
        case _ if value.valueType == ConfigValueType.STRING =>
          val str = value.unwrapped.asInstanceOf[String]
          val redacted = redactString(str)
          if (redacted == str) value
          else ConfigValueFactory.fromAnyRef(redacted).withOrigin(value.origin)
        case _ =>
          value
      }
    }

    redactValue(config.root).asInstanceOf[ConfigObject].toConfig
  }
}
