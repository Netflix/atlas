/*
 * Copyright 2014-2022 Netflix, Inc.
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

import java.lang.reflect.Type
import java.util.zip.Deflater
import akka.http.scaladsl.coding.Coders
import akka.http.scaladsl.model.EntityStreamSizeException
import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.AuthenticationFailedRejection
import akka.http.scaladsl.server.AuthorizationFailedRejection
import akka.http.scaladsl.server.CircuitBreakerOpenRejection
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.MalformedRequestContentRejection
import akka.http.scaladsl.server.MethodRejection
import akka.http.scaladsl.server.Rejection
import akka.http.scaladsl.server.RejectionHandler
import akka.http.scaladsl.server.Route
import akka.pattern.CircuitBreakerOpenException
import com.fasterxml.jackson.core.JsonProcessingException
import com.netflix.iep.service.ClassFactory
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging

class RequestHandler(config: Config, registry: Registry, classFactory: ClassFactory)
    extends StrictLogging {

  def routes: Route = {
    val endpoints = loadRoutesFromConfig()
    if (endpoints.isEmpty) RequestHandler.notFound
    else {
      // Routes defined by the included WebApi classes from the `atlas.akka.api-endpoints`
      // config setting
      val routes = endpoints.tail.foldLeft(endpoints.head.routes) {
        case (acc, r) => acc ~ r.routes
      }
      RequestHandler.standardOptions(routes, RequestHandler.Settings(config))
    }
  }

  private def endpoints: List[String] = {
    import scala.jdk.CollectionConverters._
    config.getStringList("atlas.akka.api-endpoints").asScala.toList.distinct
  }

  /**
    * In many cases the final list will come from several config files with values getting appended
    * to the list. To avoid unnecessary duplication the class list will be deduped so that only
    * the first instance of a class will be used. The order in the list is otherwise maintained.
    */
  private def loadRoutesFromConfig(): List[WebApi] = {
    try {
      import scala.compat.java8.FunctionConverters._
      val routeClasses = endpoints
      val bindings = Map[Type, AnyRef](
        classOf[Config]   -> config,
        classOf[Registry] -> registry
      ).withDefaultValue(null)
      routeClasses.map { cls =>
        logger.info(s"loading webapi class: $cls")
        classFactory.newInstance[WebApi](cls, bindings.asJava)
      }
    } catch {
      case e: Exception =>
        logger.error("failed to instantiate api endpoints", e)
        throw e
    }
  }
}

object RequestHandler extends StrictLogging {

  import com.netflix.atlas.akka.CustomDirectives._

  private val defaultSettings = Settings(ConfigFactory.load())

  case class Settings(config: Config) {

    val corsHostPatterns: List[String] = {
      import scala.jdk.CollectionConverters._
      config.getStringList("atlas.akka.cors-host-patterns").asScala.toList.distinct
    }

    val diagnosticHeaders: List[HttpHeader] = {
      import scala.jdk.CollectionConverters._
      config
        .getConfigList("atlas.akka.diagnostic-headers")
        .asScala
        .map { c =>
          RawHeader(c.getString("name"), c.getString("value"))
        }
        .toList
        .distinct
    }

    val corsEnabled: Boolean = {
      config.getBoolean("atlas.akka.request-handler.cors")
    }

    val handleCompression: Boolean = {
      config.getBoolean("atlas.akka.request-handler.compression")
    }

    val enableAccessLog: Boolean = {
      config.getBoolean("atlas.akka.request-handler.access-log")
    }

    val closeProbability: Double = {
      config.getDouble("atlas.akka.request-handler.close-probability")
    }
  }

  // Custom set of encoders, same as the default set used with the `encodeResponse` directive
  // except that the compression level is set to best speed rather than the default to reduce
  // the computation overhead
  private val CompressedResponseEncoders = Seq(
    Coders.Gzip(compressionLevel = Deflater.BEST_SPEED),
    Coders.Deflate(compressionLevel = Deflater.BEST_SPEED)
  )

  /**
    * Wraps a route with the standard options that we typically use for error handling,
    * logging, CORS support, compression, etc.
    *
    * @param route
    *     The user route to wrap with the standard options.
    * @param settings
    *     Configuration options to adjust the behavior of the handler. See the reference.conf
    *     for more information.
    */
  def standardOptions(route: Route, settings: Settings = defaultSettings): Route = {

    // Default paths to always include
    val ok = path("ok") {
      // Default endpoint for testing that always returns 200
      extractClientIP { ip =>
        val msg = ip.toIP.map(_.toString()).getOrElse("unknown")
        complete(HttpResponse(StatusCodes.OK, entity = msg))
      }
    }

    val finalRoutes = ok ~ route

    // Automatically deal with compression
    val gzip =
      if (!settings.handleCompression) finalRoutes
      else
        encodeResponseWith(Coders.NoCoding, CompressedResponseEncoders: _*) {
          decodeRequest { finalRoutes }
        }

    // Add a default exception handler
    val error = handleExceptions(exceptionHandler) {
      handleRejections(rejectionHandler) { gzip }
    }

    // Add close directive to help balance connections
    val close = closeConnection(settings.closeProbability) {
      error
    }

    // Include all requests in the access log
    val log =
      if (!settings.enableAccessLog) close
      else
        accessLog(settings.diagnosticHeaders) { close }

    // Add CORS headers to all responses
    if (settings.corsEnabled)
      cors(settings.corsHostPatterns) { log }
    else
      log
  }

  /**
    * Wraps a route with error handling to format error messages in a consistent way.
    */
  def errorOptions(route: Route): Route = {
    handleExceptions(exceptionHandler) {
      handleRejections(rejectionHandler) { route }
    }
  }

  def errorResponse(t: Throwable): HttpResponse = {
    // Log exception to make it easier to access the full stack trace. This could be
    // high volume if there are a lot of failed requests, so it could have a performance
    // impact if enabled. The exception is logged here rather than in `exceptionHandler`
    // so that any exceptions coming from `handleRejections` will also get logged.
    logger.trace("request failed with exception", t)

    // Determine most appropriate status code to use based on the exception type
    t match {
      case e @ (_: IllegalArgumentException | _: IllegalStateException |
          _: JsonProcessingException) =>
        DiagnosticMessage.error(StatusCodes.BadRequest, e)
      case e: NoSuchElementException =>
        DiagnosticMessage.error(StatusCodes.NotFound, e)
      case e: EntityStreamSizeException =>
        DiagnosticMessage.error(StatusCodes.PayloadTooLarge, e)
      case e: CircuitBreakerOpenException =>
        DiagnosticMessage.error(StatusCodes.ServiceUnavailable, e)
      case e: Throwable =>
        DiagnosticMessage.error(StatusCodes.InternalServerError, e)
    }
  }

  private def exceptionHandler: PartialFunction[Throwable, Route] = {
    case t => complete(errorResponse(t))
  }

  private def rejectionHandler: RejectionHandler = {
    val builder = RejectionHandler
      .newBuilder()
      .handle {
        case CircuitBreakerOpenRejection(t) =>
          complete(errorResponse(t))
        case MalformedRequestContentRejection(_, t) =>
          complete(errorResponse(t))
        case MethodRejection(m) =>
          error(StatusCodes.MethodNotAllowed, s"method not allowed: ${m.name()}")
        case AuthenticationFailedRejection(_, _) =>
          error(StatusCodes.Forbidden, "not authorized")
        case AuthorizationFailedRejection =>
          error(StatusCodes.Unauthorized, "not authorized")
        case CustomRejection(status, message) =>
          error(status, message)
        case r: Rejection =>
          error(StatusCodes.BadRequest, r.toString)
      }
      .handleNotFound(notFound)
    builder.result()
  }

  private def error(status: StatusCode, msg: String): Route = {
    complete(DiagnosticMessage.error(status, msg))
  }

  private def notFound: Route = { ctx =>
    val msg = s"path not found: ${ctx.request.uri.path.toString()}"
    ctx.complete(DiagnosticMessage.error(StatusCodes.NotFound, msg))
  }
}
