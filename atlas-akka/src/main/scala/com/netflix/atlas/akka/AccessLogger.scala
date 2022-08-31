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

import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import com.netflix.spectator.api.Spectator
import com.netflix.spectator.ipc.IpcLogEntry
import com.netflix.spectator.ipc.IpcLogger

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
class AccessLogger private (entry: IpcLogEntry, client: Boolean) {

  /**
    * We go ahead and log on the chunk-start so we can see how long it to start sending the
    * response in the logs. Also for cases where a TCP error occurs this logger doesn't
    * get notified so the complete message will be missing.
    */
  def chunkStart(request: HttpRequest, response: HttpResponse): Unit = {
    AccessLogger.addRequestInfo(entry, request)
    AccessLogger.addResponseInfo(entry, response)
    entry.markEnd().log()
  }

  /**
    * Log again when the last chunk is received to mark the overall time for the request.
    * This is currently ignored. The timing will always be for the arrival of the first
    * chunk.
    */
  def chunkComplete(): Unit = {}

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
    entry.markEnd().log()
  }
}

object AccessLogger {

  private val owner = "atlas-akka"

  private[akka] val ipcLogger = new IpcLogger(Spectator.globalRegistry())

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
    val entry = ipcLogger.createClientEntry().withOwner(owner).addTag("id", name)
    addRequestInfo(entry, request)
    entry.markStart()
    new AccessLogger(entry, true)
  }

  /**
    * Create a new logger for a server. This will use a default entry.
    */
  def newServerLogger: AccessLogger = newServerLogger(ipcLogger.createServerEntry())

  /**
    * Create a new logger for a server based on the provided entry.
    *
    * @param entry
    *     Entry with additional information that can be extracted from the
    *     environment such as the remote ip address.
    * @return
    *     Logger for the request with the start time marked.
    */
  def newServerLogger(entry: IpcLogEntry): AccessLogger = {
    entry.withOwner(owner).markStart()
    new AccessLogger(entry, false)
  }

  private def addRequestInfo(entry: IpcLogEntry, request: HttpRequest): Unit = {
    entry
      .withHttpMethod(request.method.name)
      .withUri(request.uri.toString(), request.uri.path.toString())
    request.headers.foreach(h => entry.addRequestHeader(h.name, h.value))
  }

  private def addResponseInfo(entry: IpcLogEntry, response: HttpResponse): Unit = {
    entry.withHttpStatus(response.status.intValue)
    response.headers.foreach(h => entry.addResponseHeader(h.name, h.value))
  }
}
