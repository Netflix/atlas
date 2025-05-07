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
package com.netflix.atlas.lwcapi

import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.http.scaladsl.model.HttpHeader
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.MediaTypes
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.headers.*
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.util.ByteString
import com.github.benmanes.caffeine.cache.CacheLoader
import com.github.benmanes.caffeine.cache.Caffeine
import com.netflix.atlas.core.util.FastGzipOutputStream
import com.netflix.atlas.core.util.Strings
import com.netflix.atlas.json.Json
import com.netflix.atlas.json.JsonSupport
import com.netflix.atlas.pekko.CustomDirectives.*
import com.netflix.atlas.pekko.WebApi
import com.netflix.spectator.api.Registry
import com.typesafe.scalalogging.StrictLogging

import java.io.ByteArrayOutputStream
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.zip.CRC32
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Using

case class ExpressionApi(
  sm: StreamSubscriptionManager,
  registry: Registry
) extends WebApi
    with StrictLogging {

  import ExpressionApi.*

  private implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global

  private val responseCache = Caffeine
    .newBuilder()
    .expireAfterWrite(Duration.ofSeconds(10))
    .build[Option[String], EncodedExpressions](
      new CacheLoader[Option[String], EncodedExpressions] {

        override def load(key: Option[String]): EncodedExpressions = {
          val (id, expressions) = key match {
            case Some(cluster) => "overall" -> sm.subscriptionsForCluster(cluster).map(_.metadata)
            case None          => "cluster" -> sm.subscriptions.map(_.metadata)
          }
          val start = registry.clock().monotonicTime()
          val encoded = encode(expressions)
          val encodingTimeNanos = registry.clock().monotonicTime() - start
          updateStats(id, encoded.size, encodingTimeNanos)
          encoded
        }
      }
    )

  private def updateStats(id: String, size: Int, encodingTimeNanos: Long): Unit = {
    registry.distributionSummary("atlas.lwcapi.expressionsCount", "id", id).record(size)
    registry
      .timer("atlas.lwcapi.encodingTime", "id", id)
      .record(encodingTimeNanos, TimeUnit.NANOSECONDS)
  }

  /** Helper for testing to force the recomputation of encoded expressions. */
  private[lwcapi] def clearCache(): Unit = {
    responseCache.invalidateAll()
  }

  def routes: Route = {
    endpointPathPrefix("lwc" / "api" / "v1" / "expressions") {
      optionalHeaderValueByName("If-None-Match") { etags =>
        get {
          pathEndOrSingleSlash {
            complete(Future(handleList(etags)))
          } ~
          path(Segment) { cluster =>
            complete(Future(handleGet(etags, cluster)))
          }
        }
      }
    }
  }

  private def handleList(receivedETags: Option[String]): HttpResponse = {
    handle(receivedETags, responseCache.get(None))
  }

  private def handleGet(receivedETags: Option[String], cluster: String): HttpResponse = {
    handle(receivedETags, responseCache.get(Some(cluster)))
  }

  private def handle(
    receivedETags: Option[String],
    expressions: EncodedExpressions
  ): HttpResponse = {
    val tag = expressions.etag
    val headers: List[HttpHeader] = List(RawHeader("ETag", tag))
    val recvTags = receivedETags.getOrElse("")
    if (recvTags.contains(tag)) {
      HttpResponse(StatusCodes.NotModified, headers = headers)
    } else {
      val entity = HttpEntity(MediaTypes.`application/json`, expressions.data)
      HttpResponse(StatusCodes.OK, `Content-Encoding`(HttpEncodings.gzip) :: headers, entity)
    }
  }
}

object ExpressionApi {

  case class Return(expressions: List[ExpressionMetadata]) extends JsonSupport

  case class EncodedExpressions(etag: String, data: ByteString, size: Int)

  private val streams = new ThreadLocal[ByteArrayOutputStream]

  /** Use thread local to reuse byte array buffers across calls. */
  private def getOrCreateStream: ByteArrayOutputStream = {
    var baos = streams.get
    if (baos == null) {
      baos = new ByteArrayOutputStream
      streams.set(baos)
    } else {
      baos.reset()
    }
    baos
  }

  private[lwcapi] def encode(expressions: List[ExpressionMetadata]): EncodedExpressions = {
    val baos = getOrCreateStream
    Using.resource(new FastGzipOutputStream(baos)) { out =>
      Using.resource(Json.newJsonGenerator(out)) { gen =>
        Return(expressions.sorted).encode(gen)
      }
    }
    val bytes = baos.toByteArray

    val crc = new CRC32
    crc.update(bytes)
    crc.getValue
    val hash = Strings.zeroPad(crc.getValue, 16)
    val etag = s""""$hash""""

    val data = ByteString.fromArrayUnsafe(bytes)
    EncodedExpressions(etag, data, expressions.size)
  }
}
