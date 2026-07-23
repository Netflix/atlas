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

import org.apache.pekko.http.scaladsl.model.AttributeKey

/**
  * Identity of the caller as established by an upstream authentication layer, typically a
  * proxy that terminates authentication in front of the service. This is a neutral model:
  * the actual extraction of the identity from the transport or request headers is provided
  * by a [[RequestAuthenticator]], which is expected to be Netflix or deployment specific.
  * Routes read the context with the `extractCaller` directive.
  *
  * @param direct
  *     The immediate caller that made the request to this service. For an application call
  *     it will be the application, for a user call it will be the user.
  * @param original
  *     The caller that initiated the overall chain of requests. When a request is made on
  *     behalf of another party (for example an application acting for a user), this will be
  *     that original party. When there is no such delegation, it is the same as `direct`.
  * @param forwardedToken
  *     Raw end-to-end token, if one was forwarded with the request. It is retained as an
  *     opaque value so that it can be relayed on any subsequent requests the service makes.
  */
case class CallerContext(
  direct: Principal,
  original: Principal,
  forwardedToken: Option[String]
)

object CallerContext {

  /** Request attribute used to propagate the caller context to the inner routes. */
  val attr: AttributeKey[CallerContext] = AttributeKey[CallerContext]("atlas-caller")

  /** Context used when the caller could not be determined. */
  val Anonymous: CallerContext = CallerContext(Principal.Anonymous, Principal.Anonymous, None)
}

/**
  * A party associated with a request.
  *
  * @param kind
  *     Whether the party is an application, a user, or could not be determined.
  * @param id
  *     Identifier for the party. For an application this will be the application name, for a
  *     user it will typically be their email address.
  */
case class Principal(kind: Principal.Kind, id: String)

object Principal {

  /** Indicates whether a [[Principal]] is an application, a user, or unknown. */
  sealed trait Kind

  object Kind {

    /** The party is an application. */
    case object App extends Kind

    /** The party is a user. */
    case object User extends Kind

    /** The party could not be determined. */
    case object Unknown extends Kind
  }

  /** Principal used when the party could not be determined. */
  val Anonymous: Principal = Principal(Kind.Unknown, "unknown")
}
