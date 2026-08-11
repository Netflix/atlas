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
  * available, and otherwise from the legacy `id` query parameter if it names a provisioned caller,
  * falling back to an anonymous marker. A caller is given its own bucket only when it has been
  * provisioned with a dedicated budget on the endpoint; all other callers share
  * [[LimitKey.DefaultBucket]], with authenticated callers keeping their own sub-key so they can be
  * told apart within the shared bucket.
  *
  * The sub-key decides how a shared budget is divided, so it must not be something an
  * unauthenticated caller can choose freely: a caller able to mint identities would be granted one
  * share of the bucket per identity it invents. The legacy `id` parameter is therefore honored only
  * when it names a caller that has been provisioned a dedicated budget on the endpoint, which
  * bounds the set of values that can reach the limiter to the configured ones. Every other
  * unauthenticated caller shares [[LimitKey.Anonymous]], so unauthenticated traffic contends for a
  * single share no matter how many distinct values it presents.
  *
  * Where it is honored the parameter is still asserted by the caller rather than authenticated, so
  * it is kept in a namespace of its own via [[LimitKey.LegacyPrefix]]. `?id=x` resolves to
  * `legacy.x`, which does not collide with the key of an authenticated caller `x`, so a legacy
  * caller cannot reach an authenticated caller's budget and an authenticated caller is not charged
  * for legacy traffic. An authenticated identity that itself starts with the prefix is kept out of
  * the namespace here rather than assumed away, since nothing constrains what a
  * [[RequestAuthenticator]] may produce. Within that namespace the values remain self-asserted and
  * one legacy caller can still claim another's budget; the parameter exists to carry callers that
  * predate authentication and is expected to be retired once they have moved over.
  *
  * One consequence is worth stating plainly: unauthenticated callers without a provisioned budget
  * are no longer told apart, so one of them behaving badly drives the shared anonymous sub-key over
  * the penalty threshold and holds the others down with it. That is the cost of keeping a legacy
  * unauthenticated path at all, and it resolves itself once callers arrive with an authenticated
  * [[CallerContext]] and each gets a sub-key of its own.
  *
  * @param dedicatedBuckets
  *     Given an endpoint, the set of caller ids that have a dedicated budget on it. Typically
  *     [[RequestLimiter.dedicatedBuckets]].
  */
class DefaultLimitKeyResolver(dedicatedBuckets: String => Set[String]) extends LimitKeyResolver {

  override def resolve(caller: CallerContext, endpoint: String, request: HttpRequest): LimitKey = {
    val provisioned = dedicatedBuckets(endpoint)
    val authenticated = caller.direct.kind != Principal.Kind.Unknown
    val id = if (authenticated) caller.direct.id else legacyId(request, provisioned)
    // The legacy namespace is reserved for self-asserted ids, and nothing constrains what a
    // [[RequestAuthenticator]] may produce, so an authenticated identity that happens to start with
    // the prefix keeps a sub-key of its own but is not allowed to match a legacy caller's budget.
    // Without this the namespacing would hold in one direction only.
    val reserved = authenticated && id.startsWith(LimitKey.LegacyPrefix)
    val bucket = if (!reserved && provisioned.contains(id)) id else LimitKey.DefaultBucket
    LimitKey(bucket, id, endpoint)
  }

  // Only a provisioned id is trusted from the query parameter, and it is namespaced so that it
  // cannot name an authenticated caller. Anything else, including a missing or empty value, is
  // treated as anonymous rather than as an identity of its own. Written without `filter` so that
  // the request path does not allocate a closure capturing `provisioned`. The query is parsed only
  // for an endpoint that has provisioned callers at all, since with none of them there is nothing
  // a value could match.
  private def legacyId(request: HttpRequest, provisioned: Set[String]): String = {
    if (provisioned.isEmpty) LimitKey.Anonymous
    else {
      val id = request.uri.query().get("id")
      // `?id=` parses to `Some("")`, which is not an identity, so the value is checked rather than
      // just the presence of the parameter.
      if (id.isEmpty || id.get.isEmpty) LimitKey.Anonymous
      else {
        val namespaced = LimitKey.LegacyPrefix + id.get
        if (provisioned.contains(namespaced)) namespaced else LimitKey.Anonymous
      }
    }
  }
}
