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
import java.net.URI

import com.netflix.atlas.json.Json
import com.netflix.spectator.sandbox.HttpLogEntry
import spray.http.HttpMethods._
import spray.http.MediaTypes._
import spray.http._
import spray.routing._
import spray.routing.directives.BasicDirectives._
import spray.routing.directives.DebuggingDirectives._
import spray.routing.directives.HeaderDirectives._
import spray.routing.directives.LoggingMagnet
import spray.routing.directives.MiscDirectives._
import spray.routing.directives.ParameterDirectives._
import spray.routing.directives.RespondWithDirectives._

object CustomDirectives {

  // Make sure that all headers in the response will be readable by the browser
  private def exposeHeaders: Directive0 = {
    mapHttpResponseHeaders { headers =>
      if (headers.isEmpty) headers else {
        val exposed = headers.map(_.name).mkString(",")
        HttpHeaders.`Access-Control-Expose-Headers`(exposed) :: headers
      }
    }
  }

  /**
   * Filter to provide basic CORS support. By default it assumes that actual security is provided
   * elsewhere. The goal for this filter is to allow javascript UIs or other tools to access
   * the APIs and work with minimal fuss.
   */
  def corsFilter: Directive0 = {
    // Add the cors headers to anything with an origin, browser behavior seems to be mixed as to
    // which request headers we can expect to receive
    optionalHeaderValueByName("Origin").flatMap {
      case None => noop
      case Some(origin) =>
        // '*' doesn't seem to work reliably so use requested origin if provided. If running from
        // a local file we typically see 'null'.
        val allow = if (origin == "null") AllOrigins else new SomeOrigins(List(HttpOrigin(origin)))

        // Just allow all methods
        val headers = List(
          HttpHeaders.`Access-Control-Allow-Origin`(allow),
          HttpHeaders.`Access-Control-Allow-Methods`(GET, PATCH, POST, PUT, DELETE))

        // If specific headers are requested echo those back
        optionalHeaderValueByName("Access-Control-Request-Headers").flatMap {
          case None =>
            respondWithHeaders(headers) & exposeHeaders
          case Some(h) => pass
            val finalHeaders = HttpHeaders.`Access-Control-Allow-Headers`(h) :: headers
            respondWithHeaders(finalHeaders) & exposeHeaders
        }
    }
  }

  /**
   * Returns a JSONP response. This directive will always try to return a 200 response so that the
   * javascript code in the browser can better deal with errors. The actual response status code
   * and headers will be included as part of the JSON object returned.
   */
  def jsonpFilter: Directive0 = {
    import scala.language.postfixOps
    parameter("callback"?).flatMap {
      case None => noop
      case Some(callback) =>
        mapHttpResponse { res =>
          val writer = new StringWriter
          writer.write(callback)
          writer.append('(')
          val gen = Json.newJsonGenerator(writer)
          gen.writeStartObject()
          gen.writeNumberField("status", res.status.intValue)

          // Write out list of headers, the content-type is part of the entity so the object is
          // closed as the first part of the entity encoding
          gen.writeObjectFieldStart("headers")
          res.headers.groupBy(_.lowercaseName).foreach { case (n, hs) =>
            gen.writeArrayFieldStart(n)
            hs.foreach { h => gen.writeString(h.value) }
            gen.writeEndArray()
          }

          // Write out the entity
          res.entity match {
            case entity: HttpEntity.NonEmpty =>
              // Complete headers object
              val contentType = entity.contentType.mediaType
              gen.writeArrayFieldStart("content-type")
              gen.writeString(contentType.toString)
              gen.writeEndArray()
              gen.writeEndObject()

              gen.writeFieldName("body")
              contentType match {
                case `application/json`        => gen.writeRawValue(res.entity.asString)
                case t if t.mainType == "text" => gen.writeString(res.entity.asString)
                case _                         => gen.writeBinary(res.entity.data.toByteArray)
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
              gen.writeString("")
              gen.writeEndObject()
          }
          gen.flush()
          writer.append(')')
          val entity = HttpEntity(`application/javascript`, writer.toString)
          HttpResponse(status = StatusCodes.OK, entity = entity)
        }
    }
  }

  private def addRequestInfo(entry: HttpLogEntry, req: HttpRequest): Unit = {
    entry
      .mark("logging")
      .withMethod(req.method.name)
      .withRequestUri(URI.create(req.uri.render(new StringRendering).get))
      .withRequestContentLength(req.entity.data.length)
    req.headers.foreach(h => entry.withRequestHeader(h.name, h.value))
  }

  private def addResponseInfo(entry: HttpLogEntry, res: HttpResponse): Unit = {
    entry
      .withStatusCode(res.status.intValue)
      .withStatusReason(res.status.reason)
      .withResponseContentLength(res.entity.data.length)
    res.headers.foreach(h => entry.withResponseHeader(h.name, h.value))
  }

  private def log(entry: HttpLogEntry, req: HttpRequest, res: HttpResponse, step: String): Unit = {
    addRequestInfo(entry, req)
    addResponseInfo(entry, res)
    entry.mark(step)
    HttpLogEntry.logServerRequest(entry)
  }

  private def log(entry: HttpLogEntry, req: HttpRequest, t: Throwable, step: String): Unit = {
    addRequestInfo(entry, req)
    entry.withException(t)
    entry.mark(step)
    HttpLogEntry.logServerRequest(entry)
  }

  // Helper function to finish constructing the log entry and writing to the logger.
  private def log(entry: HttpLogEntry)(req: HttpRequest)(obj: Any): Unit = {
    obj match {
      case Confirmed(v, _) =>
        log(entry)(req)(v)
      case ChunkedResponseStart(res: HttpResponse) =>
        // We go ahead and log on the chunk-start so we can see how long it to start sending the
        // response in the logs. Also for cases where a TCP error occurs this logger doesn't
        // get notified so the complete message will be missing.
        log(entry, req, res, "chunked-start")
      case MessageChunk(_, _) =>
      case ChunkedMessageEnd =>
        // Log again on end with complete time marked.
        entry.mark("complete")
        HttpLogEntry.logServerRequest(entry)
      case res: HttpResponse =>
        log(entry, req, res, "complete")
      case t: Throwable =>
        log(entry, req, t, "complete")
      case _ =>
        addRequestInfo(entry, req)
        val cls = obj.getClass
        val t = new IllegalStateException(s"unexpected response type: ${cls.getName}")
        log(entry, req, t, "complete")
    }
  }

  /**
   * Generate an access log using spectator HttpLogEntry utility.
   */
  def accessLog: Directive0 = {
    val d1 = clientIP.flatMap { ip =>
      val addr = ip.toOption.fold("unknown")(_.getHostAddress)
      val entry = (new HttpLogEntry).withRemoteAddr(addr).mark("start")
      logRequestResponse(LoggingMagnet(log(entry)))
    }
    val d2 = requestInstance.flatMap { _ =>
      val entry = (new HttpLogEntry).mark("start")
      logRequestResponse(LoggingMagnet(log(entry)))
    }
    d1 | d2
  }
}
