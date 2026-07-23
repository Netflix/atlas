/*
 * Copyright 2014-2026 Netflix, Inc.
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

import com.typesafe.config.Config
import org.apache.pekko.http.scaladsl.server.Route

/**
  * Wraps the routes to establish the identity of the caller before a request is handled. An
  * implementation is expected to record the caller with `CustomDirectives.recordCaller` so that
  * it can be read by the inner routes via the `extractCaller` directive and included on the
  * access log. It may also reject the request if it is missing information that is required by
  * the deployment; such rejections are handled by the standard error handling because the
  * authenticator is applied inside it (see `RequestHandler.standardOptions`).
  *
  * The extraction of the identity is Netflix or deployment specific and lives outside of the
  * open source project. This trait is the neutral seam that lets such an implementation be
  * plugged in by class name (see [[RequestAuthenticator.apply]]). Implementations must have a
  * constructor that takes a Config object or that is empty.
  */
trait RequestAuthenticator {

  /** Wrap the route so that the caller identity is established before it is handled. */
  def apply(route: Route): Route
}

object RequestAuthenticator {

  /** Config key used to specify the implementation class. */
  private val configKey = "atlas.pekko.request-handler.authenticator"

  /** Authenticator that passes the route through unchanged. Used when none is configured. */
  val passthrough: RequestAuthenticator = new RequestAuthenticator {
    override def apply(route: Route): Route = route
  }

  /**
    * Create a request authenticator based on the config. If the implementation class is set,
    * then it is loaded by name. Otherwise the [[passthrough]] authenticator is used and no
    * caller identity will be established.
    */
  def apply(config: Config): RequestAuthenticator = {
    if (config.hasPath(configKey)) {
      val cls = Class.forName(config.getString(configKey)).asInstanceOf[Class[RequestAuthenticator]]
      try {
        // Look for a constructor that takes a config object
        cls.getConstructor(classOf[Config]).newInstance(config)
      } catch {
        case _: NoSuchMethodException =>
          // Use an empty constructor
          cls.getConstructor().newInstance()
      }
    } else {
      passthrough
    }
  }
}
