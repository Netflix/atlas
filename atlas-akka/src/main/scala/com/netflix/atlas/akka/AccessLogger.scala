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

import java.net.URI

import com.netflix.spectator.sandbox.HttpLogEntry
import spray.http.HttpRequest
import spray.http.HttpResponse
import spray.http.StringRendering

import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
  * Logger for generating an access log.
  *
  * @param entry
  *     Spectator log entry object that formats the log and updates metrics.
  * @param client
  *     True if the logger if for a client rather than a server. Clients are
  *     typically logged based on a name given to the client by the application.
  *     Server logs based on the endpoint. They will also be marked
  *     so that they can be partitioned into separate files if needed.
  */
class AccessLogger private (entry: HttpLogEntry, client: Boolean = false) {

  /**
    * We go ahead and log on the chunk-start so we can see how long it to start sending the
    * response in the logs. Also for cases where a TCP error occurs this logger doesn't
    * get notified so the complete message will be missing.
    */
  def chunkStart(request: HttpRequest, response: HttpResponse): Unit = {
    AccessLogger.addRequestInfo(entry, request)
    AccessLogger.addResponseInfo(entry, response)
    writeLog(AccessLogger.ChunkedStart)
  }

  /**
    * Log again when the last chunk is received to mark the overall time for the request.
    */
  def chunkComplete(): Unit = {
    writeLog(AccessLogger.Complete)
  }

  /** Complete the log entry and write out the result. */
  def complete(request: HttpRequest, result: Try[HttpResponse]): Unit = {
    AccessLogger.addRequestInfo(entry, request)
    complete(result)
  }

  /** Complete the log entry and write out the result. */
  def complete(result: Try[HttpResponse]): Unit = {
    result match {
      case Success(response) => AccessLogger.addResponseInfo(entry, response)
      case Failure(t)        => entry.withException(t)
    }
    writeLog(AccessLogger.Complete)
  }

  private def writeLog(step: String): Unit = {
    entry.mark(step)
    if (client)
      HttpLogEntry.logClientRequest(entry)
    else
      HttpLogEntry.logServerRequest(entry)
  }
}

object AccessLogger {

  private val ChunkedStart = "chunked-start"
  private val Complete = "complete"

  /**
    * Create a new logger for a named client.
    *
    * @param name
    *     Used to identify a particular client in the log entry. This will
    *     typically be the name of the service it is talking to.
    * @param request
    *     Request that is being sent to the service.
    * @return
    *     Logger for the request with the start time marked.
    */
  def newClientLogger(name: String, request: HttpRequest): AccessLogger = {
    val entry = (new HttpLogEntry).withClientName(name)
    addRequestInfo(entry, request)
    entry.mark("start")
    new AccessLogger(entry, true)
  }

  /**
    * Create a new logger for a server. This will use a default entry.
    */
  def newServerLogger: AccessLogger = newServerLogger(new HttpLogEntry)

  /**
    * Create a new logger for a server based on the provided entry.
    *
    * @param entry
    *     Entry with additional information that can be extracted from the
    *     environment such as the remote ip address.
    * @return
    *     Logger for the request with the start time marked.
    */
  def newServerLogger(entry: HttpLogEntry): AccessLogger = {
    entry.mark("start")
    new AccessLogger(entry, false)
  }

  private def addRequestInfo(entry: HttpLogEntry, request: HttpRequest): Unit = {
    entry
      .withMethod(request.method.name)
      .withRequestUri(URI.create(request.uri.render(new StringRendering).get))
      .withRequestContentLength(request.entity.data.length)
    request.headers.foreach(h => entry.withRequestHeader(h.name, h.value))
  }

  private def addResponseInfo(entry: HttpLogEntry, response: HttpResponse): Unit = {
    entry
      .withStatusCode(response.status.intValue)
      .withStatusReason(response.status.reason)
      .withResponseContentLength(response.entity.data.length)
    response.headers.foreach(h => entry.withResponseHeader(h.name, h.value))
  }
}
