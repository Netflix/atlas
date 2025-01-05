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
  * Pair representing the expression and step size for data being requested from the LWCAPI
  * service. A set of data expressions corresponding with this request will be returned as
  * an [LwcSubscription] response message.
  *
  * @param expression
  *     Expression to subscribe to from LWCAPI.
  * @param exprType
  *     Indicates the type of expression for the subscription. This is typically determined
  *     based on the endpoint used on the URI.
  * @param step
  *     The step size used for this stream of data.
  */
case class LwcExpression(expression: String, exprType: ExprType, step: Long) extends JsonSupport {
  val `type`: String = "expression"
}
