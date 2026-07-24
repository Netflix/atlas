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

import org.apache.pekko.http.scaladsl.model.HttpRequest

/**
  * Maps a caller and request onto the [[LimitKey]] used to enforce concurrency limits. Isolating
  * this mapping behind a trait lets the way a caller is identified evolve independently of the
  * limiter, for example while migrating from a query parameter based identity to an authenticated
  * one established by a [[RequestAuthenticator]].
  */
trait LimitKeyResolver {

  /**
    * @param caller
    *     Caller identity established for the request, or [[CallerContext.Anonymous]] when it could
    *     not be determined.
    * @param endpoint
    *     Logical endpoint being requested, for example `graph` or `tags`.
    * @param request
    *     The request being limited, available for identity hints beyond the caller context.
    */
  def resolve(caller: CallerContext, endpoint: String, request: HttpRequest): LimitKey
}

/**
  * Default resolver. The caller identity is taken from the authenticated [[CallerContext]] when
  * available, falling back to the legacy `id` query parameter and finally to an anonymous marker.
  * A caller is given its own bucket only when it has been provisioned with a dedicated budget on
  * the endpoint; all other callers share [[LimitKey.DefaultBucket]] but retain their own sub-key
  * so they can be told apart within the shared bucket.
  *
  * @param dedicatedBuckets
  *     Given an endpoint, the set of caller ids that have a dedicated budget on it. Typically
  *     [[RequestLimiter.dedicatedBuckets]].
  */
class DefaultLimitKeyResolver(dedicatedBuckets: String => Set[String]) extends LimitKeyResolver {

  override def resolve(caller: CallerContext, endpoint: String, request: HttpRequest): LimitKey = {
    val id = callerId(caller, request)
    val bucket = if (dedicatedBuckets(endpoint).contains(id)) id else LimitKey.DefaultBucket
    LimitKey(bucket, id, endpoint)
  }

  private def callerId(caller: CallerContext, request: HttpRequest): String = {
    caller.direct.kind match {
      case Principal.Kind.Unknown => legacyId(request).getOrElse(LimitKey.Anonymous)
      case _                      => caller.direct.id
    }
  }

  private def legacyId(request: HttpRequest): Option[String] = {
    request.uri.query().get("id").filter(_.nonEmpty)
  }
}
