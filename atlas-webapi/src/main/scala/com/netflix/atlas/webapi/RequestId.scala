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
package com.netflix.atlas.webapi

import java.lang.management.ManagementFactory
import java.util.concurrent.atomic.AtomicLong

import com.typesafe.config.ConfigFactory

/**
 * Identifier for a request to another instance. The identifier consists of a unique string for
 * a given process followed by the attempt. The attempt counter is used to track retry behavior
 * for the same logical request. If a sub-request is issued as part of a request the parent id
 * can be prepended to the local id separated with a ':'.
 *
 * ```
 * id = "[${parent}:]${unique_string}.${attempt}" // pattern
 * id = "i-12345678_5783.2:i-12345678_5783.0"     // concrete example
 * ```
 */
object RequestId {

  private val config = ConfigFactory.load()

  private val instanceId = {
    val prop = "atlas.environment.instanceId"
    if (config.hasPath(prop)) config.getString(prop) else "localhost"
  }

  private val prefix = s"${instanceId}_$getpid"

  private val count = new AtomicLong(0L)

  // Derived from:
  // http://stackoverflow.com/questions/35842/how-can-a-java-program-get-its-own-process-id
  private def getpid: String = {
    val jvmName = ManagementFactory.getRuntimeMXBean.getName
    val index = jvmName.indexOf("@")
    if (index < 1) "unknown" else jvmName.substring(0, index)
  }

  def next: RequestId = RequestId(None, s"${prefix}_${count.getAndIncrement()}", 0)

  def next(parent: RequestId): RequestId = {
    RequestId(Some(parent), s"${prefix}_${count.getAndIncrement()}", 0)
  }

  def apply(id: String): RequestId = {
    val baseId: Option[RequestId] = None
    val path = id.split(":").foldLeft(baseId) { (acc, v) =>
      val index = v.indexOf(".")
      if (index < 0)
        Some(RequestId(acc, v, -1))
      else
        Some(RequestId(acc, v.substring(0, index), v.substring(index + 1).toInt))
    }
    path.get
  }
}

case class RequestId(parent: Option[RequestId], name: String, attempt: Int) {
  def retryId: RequestId = RequestId(parent, name, attempt + 1)

  override def toString: String = {
    val str = s"$name.$attempt"
    parent.fold(str)(p => s"$p:$str")
  }
}
