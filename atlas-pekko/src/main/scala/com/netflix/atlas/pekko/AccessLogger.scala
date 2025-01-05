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

import com.netflix.iep.config.ConfigManager
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.model.HttpResponse
import com.netflix.spectator.api.Spectator
import com.netflix.spectator.ipc.IpcLogEntry
import com.netflix.spectator.ipc.IpcLogger
import com.netflix.spectator.ipc.IpcLoggerConfig
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import org.slf4j.event.Level

import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
  * Logger for generating an access log.
  *
  * @param entry
  *     Spectator log entry object that formats the log and updates metrics.
  */
class AccessLogger private (entry: IpcLogEntry) {

  private var attempt: Int = 1
  private var maxAttempts: Int = 1

  def withMaxAttempts(max: Int): AccessLogger = {
    maxAttempts = max
    this
  }

  /** Complete the log entry and write out the result. */
  def complete(request: HttpRequest, result: Try[HttpResponse]): Unit = {
    AccessLogger.addRequestInfo(entry, request)
    complete(result)
  }

  /** Complete the log entry and write out the result. */
  def complete(result: Try[HttpResponse]): Unit = {
    var failure = false
    result match {
      case Success(response) =>
        AccessLogger.addResponseInfo(entry, response)
        failure = response.status.isFailure()
      case Failure(t) =>
        entry.withException(t)
        failure = true
    }
    entry
      .markEnd()
      .withLogLevel(if (failure) Level.WARN else Level.DEBUG)
      .withAttempt(attempt)
      .withAttemptFinal(attempt == maxAttempts)
      .log()
    attempt += 1
  }
}

object AccessLogger {

  private val owner = "atlas-pekko"

  private[pekko] val ipcLogger = new IpcLogger(
    Spectator.globalRegistry(),
    LoggerFactory.getLogger(classOf[IpcLogger]),
    new IpcConfig(ConfigManager.get().getConfig("atlas.pekko.ipc-logger"))
  )

  private class IpcConfig(config: Config) extends IpcLoggerConfig {

    override def get(key: String): String = null

    override def inflightMetricsEnabled(): Boolean = {
      config.getBoolean("inflight-metrics-enabled")
    }

    override def cardinalityLimit(tagKey: String): Int = {
      val prop = s"cardinality-limit.$tagKey"
      if (config.hasPath(prop))
        config.getInt(prop)
      else
        config.getInt("cardinality-limit.default")
    }

    override def entryQueueSize(): Int = {
      config.getInt("entry-queue-size")
    }
  }

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
    new AccessLogger(entry)
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
    new AccessLogger(entry)
  }

  private def addRequestInfo(entry: IpcLogEntry, request: HttpRequest): Unit = {
    entry
      .withHttpMethod(request.method.name)
      .withUri(request.uri.toString(), request.uri.path.toString())
    request.headers.foreach(h => entry.addRequestHeader(h.name, h.value))
    entry.addRequestHeader("Content-Type", request.entity.contentType.value)
    request.entity.contentLengthOption.foreach(entry.withRequestContentLength)
  }

  private def addResponseInfo(entry: IpcLogEntry, response: HttpResponse): Unit = {
    entry.withHttpStatus(response.status.intValue)
    response.headers.foreach(h => entry.addResponseHeader(h.name, h.value))
    entry.addResponseHeader("Content-Type", response.entity.contentType.value)
    response.entity.contentLengthOption.foreach(entry.withResponseContentLength)
  }
}
