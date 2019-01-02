/*
 * Copyright 2014-2019 Netflix, Inc.
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
package com.netflix.atlas.eval.stream

import akka.NotUsed
import akka.http.scaladsl.model.HttpMethods
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers._
import akka.stream.scaladsl.Compression
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.typesafe.scalalogging.StrictLogging

import scala.util.Failure
import scala.util.Success

/**
  * Helper for creating a stream source for a given host.
  */
private[stream] object HostSource extends StrictLogging {

  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration._

  /**
    * Create a new stream source for the response of `uri`. The URI should be a streaming
    * source such as SSE that can have the messages framed by the new line and will be
    * continuously emitting data. If the response ends for any reason, then the source
    * will attempt to reconnect. Use a kill switch to shut it down.
    *
    * @param uri
    *     URI for the remote stream. Typically this should be an endpoint that returns an
    *     SSE stream.
    * @param client
    *     Client to use for making the request. This is typically used in tests to provide
    *     responses without actually making network calls.
    * @param delay
    *     How long to delay between attempts to connect to the host.
    * @return
    *     Source that emits the response stream from the host.
    */
  def apply(
    uri: String,
    client: SimpleClient,
    delay: FiniteDuration = 1.second
  ): Source[ByteString, NotUsed] = {
    EvaluationFlows.repeat(uri, delay).flatMapConcat(singleCall(client))
  }

  private def singleCall(client: SimpleClient)(uri: String): Source[ByteString, Any] = {
    logger.info(s"subscribing to $uri")
    val headers = List(Accept(MediaTypes.`text/event-stream`))
    val request = HttpRequest(HttpMethods.GET, uri, headers)

    Source
      .single(request)
      .via(client)
      .flatMapConcat {
        case Success(res: HttpResponse) if res.status == StatusCodes.OK =>
          // Framing needs to take place on the byte stream before merging chunks
          // with other hosts
          unzipIfNeeded(res)
            .via(EvaluationFlows.sseFraming)
            .recover {
              case t: Throwable =>
                logger.warn(s"stream failed $uri", t)
                ByteString.empty
            }
            .watchTermination() { (_, f) =>
              f.onComplete {
                case Success(_) =>
                  logger.info(s"lost connection to $uri")
                case Failure(t) =>
                  logger.info(s"lost connection to $uri", t)
              }
            }
        case Success(res: HttpResponse) =>
          logger.warn(s"subscription attempt failed with status ${res.status}")
          empty
        case Failure(t) =>
          logger.warn(s"subscription attempt failed with exception", t)
          empty
      }
  }

  private def empty: Source[ByteString, NotUsed] = {
    Source.empty[ByteString]
  }

  private def unzipIfNeeded(res: HttpResponse): Source[ByteString, Any] = {
    val isCompressed = res.headers.contains(`Content-Encoding`(HttpEncodings.gzip))
    val dataBytes = res.entity.withoutSizeLimit().dataBytes
    if (isCompressed) dataBytes.via(Compression.gunzip()) else dataBytes
  }
}
