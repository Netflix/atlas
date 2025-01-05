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
package com.netflix.atlas.eval.model

import com.netflix.atlas.json.JsonSupport

/**
  * Subscription message that is returned by the LWC service.
  *
  * @param expression
  *     Expression that was used for the initial subscription.
  * @param exprType
  *     Indicates the type of expression for the subscription. This is typically determined
  *     based on the endpoint used on the URI.
  * @param subExprs
  *     Data expressions that result from the root expression.
  */
case class LwcSubscriptionV2(expression: String, exprType: ExprType, subExprs: List[LwcDataExpr])
    extends JsonSupport {
  val `type`: String = "subscription-v2"
}
