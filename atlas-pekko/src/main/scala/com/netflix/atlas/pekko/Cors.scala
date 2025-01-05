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

import org.apache.pekko.http.scaladsl.model.Uri
import com.netflix.iep.config.ConfigManager

/** Helper functions for working with CORS requests.  */
object Cors {

  private val allowedHostPatterns = {
    import scala.jdk.CollectionConverters.*
    ConfigManager.get().getStringList("atlas.pekko.cors-host-patterns").asScala.toList
  }

  /**
    * Return a normalized origin string with just the host name if the host is allowed based
    * on the set of patterns configured in `atlas.pekko.cors-host-patterns`.
    */
  def normalizedOrigin(origin: String): Option[String] = {
    val originHostname = extractHostname(origin)
    if (isOriginAllowed(allowedHostPatterns, originHostname)) Option(originHostname) else None
  }

  /**
    * Check if the origin is allowed.
    *
    * @param hosts
    *     Set of host patterns to allow.
    * @param origin
    *     Origin value normally extracted from the request headers.
    * @return
    *     True if the hostname is allowed based on the set of host patterns.
    */
  def isOriginAllowed(hosts: List[String], origin: String): Boolean = {
    try {
      val originHostname = extractHostname(origin)
      checkOrigin(hosts, originHostname)
    } catch {
      case _: Exception =>
        // If there is a failure processing the origin uri, then do not add CORS headers.
        false
    }
  }

  private def extractHostname(origin: String): String = {
    if (origin.startsWith("http:") || origin.startsWith("https:"))
      Uri(origin).authority.host.address()
    else
      origin
  }

  @scala.annotation.tailrec
  private def checkOrigin(hosts: List[String], origin: String): Boolean = {
    hosts match {
      case h :: hs => checkOrigin(h, origin) || checkOrigin(hs, origin)
      case Nil     => false
    }
  }

  private def checkOrigin(host: String, origin: String): Boolean = {
    (host == "*") || (host.startsWith(".") && origin.endsWith(host)) || (host == origin)
  }
}
