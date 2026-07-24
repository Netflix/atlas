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

/**
  * Identifies how a request maps onto the limiting policy. The `bucket` selects the configured
  * concurrency budget that will be enforced. The `subKey` identifies the individual caller
  * within that budget; a single bucket is commonly shared by many callers, so the `subKey`
  * distinguishes them for per-caller fairness and diagnostics. For a caller that has its own
  * dedicated bucket the two values are typically the same.
  *
  * @param bucket
  *     Name of the concurrency budget to enforce. Resolved against the per-endpoint
  *     configuration, falling back to the endpoint default budget when not explicitly listed.
  * @param subKey
  *     Identifier for the individual caller within the bucket. Used for per-caller fairness
  *     within a shared bucket and for diagnostic messages. Not used as a metric dimension to
  *     avoid unbounded cardinality.
  * @param endpoint
  *     Logical endpoint the request is being made against, for example `graph` or `tags`.
  *     Budgets are configured per endpoint so the same caller can have different limits on
  *     different endpoints.
  */
case class LimitKey(bucket: String, subKey: String, endpoint: String)

object LimitKey {

  /** Name of the bucket shared by all callers that do not have a dedicated budget. */
  val DefaultBucket: String = "default"

  /** Sub-key used for a caller whose identity could not be determined. */
  val Anonymous: String = "anonymous"
}
