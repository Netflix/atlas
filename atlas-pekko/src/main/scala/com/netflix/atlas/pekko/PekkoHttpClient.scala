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

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.HttpsConnectionContext
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.StatusCode
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.headers.*
import org.apache.pekko.http.scaladsl.settings.ConnectionPoolSettings
import org.apache.pekko.stream.scaladsl.Compression
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.scaladsl.RetryFlow
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString

import java.net.ConnectException
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
  * A wrapper used for simple unit testing of Pekko HTTP calls.
  */
trait PekkoHttpClient {

  import PekkoHttpClient.*

  /**
    * Helper for using single request API along with common IPC instrumentation.
    */
  def singleRequest(request: HttpRequest): Future[HttpResponse]

  /**
    * Helper for using super pool along with common IPC instrumentation. It provides custom
    * handling of retries to allow for more flexible policies and being able to track the
    * attempts.
    */
  def superPool[C](
    config: ClientConfig = ClientConfig()
  ): Flow[(HttpRequest, C), (Try[HttpResponse], C), NotUsed]

  /**
    * Helper for using super pool along with common IPC instrumentation when additional context
    * is not needed. See `superPool` for more details.
    */
  def simpleFlow(
    config: ClientConfig = ClientConfig()
  ): Flow[HttpRequest, Try[HttpResponse], NotUsed] = {
    Flow[HttpRequest]
      .map(r => r -> NotUsed)
      .via(superPool(config))
      .map(_._1)
  }
}

object PekkoHttpClient {

  /**
    * Create a new client instance.
    *
    * @param name
    *     Name to use for access logging and metrics.
    * @param system
    *     Actor system to use for processing the requests.
    * @return
    *     New client instance.
    */
  def create(name: String, system: ActorSystem): PekkoHttpClient = {
    new HttpClientImpl(name)(system)
  }

  /**
    * Create a client instance that will return a fixed response for every request. Mainly
    * used for testing.
    */
  def create(response: Try[HttpResponse]): PekkoHttpClient = {
    new PekkoHttpClient {

      override def singleRequest(request: HttpRequest): Future[HttpResponse] = {
        Future.fromTry(response)
      }

      override def superPool[C](
        config: ClientConfig
      ): Flow[(HttpRequest, C), (Try[HttpResponse], C), NotUsed] = {
        Flow[(HttpRequest, C)]
          .map {
            case (_, context) => response -> context
          }
      }
    }
  }

  private[pekko] case class Context[C](accessLogger: AccessLogger, callerContext: C)

  case class ClientConfig(
    connectionContext: Option[HttpsConnectionContext] = None,
    settings: Option[ConnectionPoolSettings] = None,
    shouldRetry: Try[HttpResponse] => Boolean = retryForServerErrors
  )

  /**
    * Returns true if there request was throttled or there was a server error. Client
    * errors (4xx) will not be retried.
    */
  def retryForServerErrors(response: Try[HttpResponse]): Boolean = response match {
    case Success(r) => isThrottling(r.status) || isServerError(r.status)
    case Failure(_) => true
  }

  private def isThrottling(status: StatusCode): Boolean = status match {
    case StatusCodes.TooManyRequests    => true
    case StatusCodes.ServiceUnavailable => true
    case _                              => false
  }

  private def isServerError(status: StatusCode): Boolean = status match {
    case StatusCodes.TooManyRequests => true
    case StatusCodes.ServerError(_)  => true
    case _                           => false
  }

  /**
    * Returns true if there request was throttled or there was a connection error.
    * All other failures could have been partially processed by the server and thus
    * are considered not safe to retry.
    */
  def retryForSafeErrors(response: Try[HttpResponse]): Boolean = response match {
    case Success(r) => isThrottling(r.status)
    case Failure(t) => isConnectException(t)
  }

  private def isConnectException(t: Throwable): Boolean = t.isInstanceOf[ConnectException]

  /** Default implementation based on Pekko `Http()`. */
  private[pekko] class HttpClientImpl(name: String)(implicit val system: ActorSystem)
      extends PekkoHttpClient {

    private implicit val ec: ExecutionContext = system.dispatcher
    private val http = Http()

    protected def doSingleRequest(request: HttpRequest): Future[HttpResponse] = {
      http.singleRequest(request)
    }

    override def singleRequest(request: HttpRequest): Future[HttpResponse] = {
      val accessLogger = AccessLogger.newClientLogger(name, request)
      doSingleRequest(request).andThen { case t => accessLogger.complete(t) }
    }

    protected def superPoolFlow[C](
      connectionContext: HttpsConnectionContext,
      settings: ConnectionPoolSettings
    ): Flow[(HttpRequest, C), (Try[HttpResponse], C), NotUsed] = {
      http.superPool(connectionContext = connectionContext, settings = settings)
    }

    override def superPool[C](
      config: ClientConfig
    ): Flow[(HttpRequest, C), (Try[HttpResponse], C), NotUsed] = {
      val connectionContext = config.connectionContext.getOrElse(http.defaultClientHttpsContext)
      val settings = config.settings.getOrElse(ConnectionPoolSettings(system))

      // All retries will be handled in this flow, disable in the pekko pool
      val pekkoSettings = settings.withMaxRetries(0)
      val clientFlow =
        superPoolFlow[Context[C]](connectionContext = connectionContext, settings = pekkoSettings)
          .map {
            case (response, context) =>
              context.accessLogger.complete(response)
              response -> context
          }

      // Retry requests if needed with instrumentation via common IPC
      val retryFlow =
        RetryFlow.withBackoff[(HttpRequest, Context[C]), (Try[HttpResponse], Context[C]), NotUsed](
          minBackoff = settings.baseConnectionBackoff,
          maxBackoff = settings.maxConnectionBackoff,
          randomFactor = 0.25,
          maxRetries = settings.maxRetries,
          flow = clientFlow
        ) {
          case (request, (response, _)) if config.shouldRetry(response) => Some(request)
          case _                                                        => None
        }

      // Map caller context to internal version that includes access logger
      Flow[(HttpRequest, C)]
        .map {
          case (request, callerContext) =>
            val accessLogger = AccessLogger
              .newClientLogger(name, request)
              .withMaxAttempts(settings.maxRetries + 1)
            request -> Context(accessLogger, callerContext)
        }
        .via(if (settings.maxRetries > 0) retryFlow else clientFlow)
        .map {
          case (response, context) => response -> context.callerContext
        }
    }
  }

  def unzipIfNeeded(res: HttpResponse): Source[ByteString, Any] = {
    val isCompressed = res.headers.contains(`Content-Encoding`(HttpEncodings.gzip))
    val dataBytes = res.entity.withoutSizeLimit().dataBytes
    if (isCompressed) dataBytes.via(Compression.gunzip()) else dataBytes
  }
}
