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
package com.netflix.atlas.pekko

import java.io.StringWriter
import org.apache.pekko.http.scaladsl.model.HttpCharsets
import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.http.scaladsl.model.HttpHeader
import org.apache.pekko.http.scaladsl.model.HttpMethods
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.MediaType
import org.apache.pekko.http.scaladsl.model.MediaTypes
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.headers.*
import org.apache.pekko.http.scaladsl.server.Directive
import org.apache.pekko.http.scaladsl.server.Directive0
import org.apache.pekko.http.scaladsl.server.Directive1
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.MalformedRequestContentRejection
import org.apache.pekko.http.scaladsl.server.PathMatcher
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.server.RouteResult
import org.apache.pekko.http.scaladsl.server.directives.LoggingMagnet
import org.apache.pekko.http.scaladsl.unmarshalling.FromRequestUnmarshaller
import org.apache.pekko.stream.Materializer
import org.apache.pekko.util.ByteString
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.module.scala.JavaTypeable
import com.netflix.atlas.json.Json
import com.netflix.spectator.ipc.NetflixHeader
import org.apache.pekko.http.scaladsl.server.util.Tuple

import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

object CustomDirectives {

  private def isSmile(mediaType: MediaType): Boolean = {
    mediaType == CustomMediaTypes.`application/x-jackson-smile`
  }

  /**
    * Used with `parseEntity` to decode the request entity to an object of type
    * `T` using the default ObjectMapper from `atlas-json`. If the content type
    * is `application/x-jackson-smile`, then a smile parser will be used. Otherwise
    * it will be treated as `application/json` regardless of the content type.
    *
    * Note: This is kept as a separate function passed into the `parseEntity`
    * directive because adding the manifest to `T` causes problems when used
    * directly on the directive. It also makes it possible to reuse `parseEntity`
    * with a custom function.
    */
  def json[T: JavaTypeable]: MediaType => ByteString => T = { mediaType => bs =>
    {
      if (isSmile(mediaType))
        Json.smileDecode[T](ByteStringInputStream.create(bs))
      else
        Json.decode[T](ByteStringInputStream.create(bs))
    }
  }

  /**
    * Used with `parseEntity` to decode the request entity to an object of type
    * `T` using a custom decoder function. If the content type is
    * `application/x-jackson-smile`, then a smile parser will be used. Otherwise
    * it will be treated as `application/json` regardless of the content type.
    */
  def customJson[T](decoder: JsonParser => T): MediaType => ByteString => T = { mediaType => bs =>
    {
      val p =
        if (isSmile(mediaType))
          Json.newSmileParser(ByteStringInputStream.create(bs))
        else
          Json.newJsonParser(ByteStringInputStream.create(bs))
      try decoder(p)
      finally p.close()
    }
  }

  /**
    * Parses the request entity into an object of type `T`. The parsing is done by
    * passing in the complete request data to the function `f`.
    */
  def parseEntity[T](f: MediaType => ByteString => T): Directive1[T] = {
    extractRequestContext.flatMap[Tuple1[T]] { ctx =>
      import ctx.executionContext
      import ctx.materializer
      val entity = ctx.request.entity
      val future = entity.dataBytes.runReduce(_ ++ _).map(f(entity.contentType.mediaType))
      onComplete(future).flatMap {
        case Success(v) => provide[T](v)
        case Failure(t) => reject(MalformedRequestContentRejection("invalid request payload", t))
      }
    }
  }

  /**
    * Used with `entity` to decode the request entity to an object of type
    * `T` using the default ObjectMapper from `atlas-json`. If the content type
    * is `application/x-jackson-smile`, then a smile parser will be used. Otherwise
    * it will be treated as `application/json` regardless of the content type.
    */
  def jsonUnmarshaller[T: JavaTypeable]: FromRequestUnmarshaller[T] = {
    val parse = json[T]
    new FromRequestUnmarshaller[T] {
      override def apply(
        request: HttpRequest
      )(implicit ec: ExecutionContext, materializer: Materializer): Future[T] = {
        val entity = request.entity
        entity.dataBytes.runReduce(_ ++ _).map(parse(entity.contentType.mediaType))
      }
    }
  }

  // Make sure that all headers in the response will be readable by the browser
  private def exposeHeaders: Directive0 = {
    mapResponseHeaders { headers =>
      val exposed = headers.map(_.name).filterNot(_.startsWith("Access-Control"))
      if (exposed.isEmpty) headers
      else {
        headers ++ scala.collection.immutable.Seq(`Access-Control-Expose-Headers`(exposed))
      }
    }
  }

  /**
    * Filter to provide basic CORS support. By default it assumes that actual security is provided
    * elsewhere. The goal for this filter is to allow javascript UIs or other tools to access
    * the APIs and work with minimal fuss.
    *
    * The hosts param specifies a whitelist for allowing cross-origin requests. Default is no
    * requests will get CORS headers.
    */
  def respondWithCorsHeaders(hosts: List[String] = Nil): Directive0 = {

    // Add the cors headers to anything with an origin, browser behavior seems to be mixed as to
    // which request headers we can expect to receive
    optionalHeaderValueByName("Origin").flatMap {
      case None                                                 => pass
      case Some(origin) if !Cors.isOriginAllowed(hosts, origin) => pass
      case Some(origin)                                         =>
        // '*' doesn't seem to work reliably so use requested origin if provided. If running from
        // a local file we typically see 'null'.
        val allow =
          if (origin == "null") HttpOriginRange.`*` else HttpOriginRange(HttpOrigin(origin))

        // List of headers to ignore for caching. For more details see:
        // https://bugs.chromium.org/p/chromium/issues/detail?id=409090
        // https://www.fastly.com/blog/caching-cors
        val vary = RawHeader("Vary", "Origin")

        // Just allow all methods
        val headers = List(
          `Access-Control-Allow-Origin`.forRange(allow),
          `Access-Control-Allow-Methods`(
            HttpMethods.GET,
            HttpMethods.PATCH,
            HttpMethods.POST,
            HttpMethods.PUT,
            HttpMethods.DELETE
          ),
          `Access-Control-Allow-Credentials`(true),
          vary
        )

        // If specific headers are requested echo those back
        optionalHeaderValueByName("Access-Control-Request-Headers").flatMap {
          case None =>
            respondWithHeaders(headers) & exposeHeaders
          case Some(h) =>
            val finalHeaders = `Access-Control-Allow-Headers`(h) :: headers
            respondWithHeaders(finalHeaders) & exposeHeaders
        }
    }
  }

  /** Route for CORS handling pre-flight checks. */
  def corsPreflight(hosts: List[String] = Nil): Route = {
    // Set max age header to minimize the number of round-trips the browser will need
    // to make. Various browsers limit the max age that can be used. Ten minutes seems
    // to be a common number (chrome and webkit) so that is what we use here.
    val response = HttpResponse(StatusCodes.OK).withHeaders(`Access-Control-Max-Age`(600))

    // Assume all OPTIONS requests are for pre-flight checks
    options {
      extractRequestContext { ctx =>
        // For some requests the browser wants the CORS headers to be present on the
        // pre-flight response.
        respondWithCorsHeaders(hosts) {
          // Ignore request body if present
          val future = ctx.request.discardEntityBytes(ctx.materializer).future()
          onComplete(future) { _ =>
            complete(response)
          }
        }
      }
    }
  }

  /**
    * Wraps a route with support for CORS. This will handle the preflight checks as well
    * as adding the appropriate headers to the response of the inner route.
    */
  def cors(hosts: List[String])(inner: Route): Route = {
    corsPreflight(hosts) ~ respondWithCorsHeaders(hosts) { inner }
  }

  /**
    * Returns a JSONP response. This directive will always try to return a 200 response so that the
    * javascript code in the browser can better deal with errors. The actual response status code
    * and headers will be included as part of the JSON object returned.
    */
  def jsonpFilter: Directive0 = {
    import scala.language.postfixOps
    parameter("callback" ?).flatMap {
      case None => pass
      case Some(callback) =>
        mapResponse { res =>
          val writer = new StringWriter
          writer.write(callback)
          writer.append('(')
          val gen = Json.newJsonGenerator(writer)
          gen.writeStartObject()
          gen.writeNumberField("status", res.status.intValue)

          // Write out list of headers, the content-type is part of the entity so the object is
          // closed as the first part of the entity encoding
          gen.writeObjectFieldStart("headers")
          res.headers.groupBy(_.lowercaseName).foreach {
            case (n, hs) =>
              gen.writeArrayFieldStart(n)
              hs.foreach { h =>
                gen.writeString(h.value)
              }
              gen.writeEndArray()
          }

          // Write out the entity
          res.entity match {
            case entity: HttpEntity.Strict =>
              // Complete headers object
              val contentType = entity.contentType.mediaType
              gen.writeArrayFieldStart("content-type")
              if (contentType.mainType == "none")
                gen.writeString("text/plain")
              else
                gen.writeString(contentType.toString)
              gen.writeEndArray()
              gen.writeEndObject()

              gen.writeFieldName("body")

              contentType match {
                case MediaTypes.`application/json` =>
                  gen.writeRawValue(entity.data.decodeString(ByteString.UTF_8))
                case t if t.mainType == "text" =>
                  gen.writeString(entity.data.decodeString(ByteString.UTF_8))
                case _ => gen.writeBinary(entity.data.toArray)
              }
              gen.writeEndObject()
            case _ =>
              // Complete headers object
              gen.writeArrayFieldStart("content-type")
              gen.writeString("text/plain")
              gen.writeEndArray()
              gen.writeEndObject()

              // Empty, just write out an empty string. Not sure why it is this instead of null
              // but keeping it this way for backwards compatibility.
              gen.writeFieldName("body")
              gen.writeString("entity type not supported via JSONP, switch to CORS")
              gen.writeEndObject()
          }
          gen.flush()
          writer.append(')')

          val jsType = MediaTypes.`application/javascript`.withCharset(HttpCharsets.`UTF-8`)
          val entity = HttpEntity(jsType, writer.toString)
          HttpResponse(status = StatusCodes.OK, entity = entity)
        }
    }
  }

  // Helper function to finish constructing the log entry and writing to the logger.
  private def log(logger: AccessLogger)(req: HttpRequest)(result: RouteResult): Unit = {
    result match {
      case RouteResult.Complete(res) => logger.complete(req, Success(res))
      case RouteResult.Rejected(vs)  => logger.complete(req, Failure(new Exception(vs.toString)))
    }
  }

  /**
    * Generate an access log using spectator HttpLogEntry utility.
    *
    * @param headers
    *     Additional headers to apply to the response before passing to the logger. This
    *     is intended for providing additional context such as the server group and zone
    *     of the particular server instance with common IPC.
    */
  def accessLog(headers: List[HttpHeader]): Directive0 = {
    extractClientIP.flatMap { ip =>
      val addr = ip.toOption.fold("unknown")(_.getHostAddress)
      val entry = AccessLogger.ipcLogger.createServerEntry.withRemoteAddress(addr)
      val logger = AccessLogger.newServerLogger(entry)
      logRequestResult(LoggingMagnet(_ => log(logger))).tflatMap { _ =>
        respondWithDefaultHeaders(headers)
      }
    }
  }

  /**
    * Adds the `Netflix-Endpoint` header to the response if it is not already present. This
    * directive is typically not used directly, it is preferred to use one of the `endpointPath*`
    * directives instead.
    */
  def respondWithEndpointHeader: Directive0 = {
    extractMatchedPath.flatMap { path =>
      val header = RawHeader(NetflixHeader.Endpoint.headerName(), path.toString())
      respondWithDefaultHeader(header)
    }
  }

  /**
    * Alternative to the built-in `path` directive that will set the `Netflix-Endpoint` header
    * based on the currently matched path. To avoid matching paths with arbitrary values that
    * should not be a part of the header, the path matcher is restricted so that values cannot
    * be extracted. Use `endpointPathPrefix` with a nested `path` directive for those use-cases.
    */
  def endpointPath(pm: PathMatcher[Unit]): Directive0 = {
    path(pm).tflatMap { _ =>
      respondWithEndpointHeader
    }
  }

  /**
    * This is a convenience directive for the common pattern of have a fixed path with an
    * extracted portion such as an identifier at the end. It will set the endpoint header based
    * on the prefix matcher. Example:
    *
    * ```
    * endpointPathPrefix("api" / "v1" / "instances") {
    *   path(Remaining) { id =>
    *     ...
    *   }
    * }
    * ```
    *
    * Can instead be written as:
    *
    * ```
    * endpointPath("api" / "v1" / "instances", Remaining) { id =>
    *   ...
    * }
    * ```
    */
  def endpointPath[L](prefix: PathMatcher[Unit], remaining: PathMatcher[L]): Directive[L] = {
    implicit val evidence: Tuple[L] = remaining.ev
    endpointPathPrefix(prefix).tflatMap { _ =>
      path(remaining)
    }
  }

  /**
    * Alternative to the built-in `pathPrefix` directive that will set the `Netflix-Endpoint`
    * header based on the currently matched path. To avoid matching paths with arbitrary values
    * that should not be a part of the header, the path matcher is restricted so that values cannot
    * be extracted.
    */
  def endpointPathPrefix(pm: PathMatcher[Unit]): Directive0 = {
    pathPrefix(pm).tflatMap { _ =>
      respondWithEndpointHeader
    }
  }

  /**
    * Add connection close header to some responses. This can be useful when using
    * network load balancers to allow some connection reuse, but force connections
    * to close frequently enough to ensure that traffic balances across instances.
    *
    * @param probability
    *     A value of 0.0 means it will never be added, 1.0 means it will
    *     always be added.
    */
  def closeConnection(probability: Double): Directive0 = {
    val closeHeader = Connection("close")

    if (probability <= 0.0) {
      // Nothing to do
      pass
    } else if (probability >= 1.0) {
      // Always close, no need to bother checking
      respondWithHeader(closeHeader)
    } else {
      mapResponseHeaders { headers =>
        val random = ThreadLocalRandom.current()
        if (random.nextDouble() < probability)
          headers.prepended(closeHeader)
        else
          headers
      }
    }
  }
}
