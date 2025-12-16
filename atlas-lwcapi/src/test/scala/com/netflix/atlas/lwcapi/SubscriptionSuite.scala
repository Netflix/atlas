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
package com.netflix.atlas.lwcapi

import com.netflix.atlas.eval.model.ExprType
import com.netflix.spectator.atlas.impl.Parser
import munit.FunSuite

class SubscriptionSuite extends FunSuite {

  private def newSubscription: Subscription = {
    val expr = Parser.parseDataExpr(s"name,test,:eq,:sum")
    val metadata = ExpressionMetadata(expr.toString, ExprType.TIME_SERIES, 60000)
    Subscription(expr.query(), metadata)
  }

  test("equals with same data") {
    val sub1 = newSubscription
    val sub2 = newSubscription
    assertEquals(sub1, sub2)
  }

  test("hashCode with same data") {
    val sub1 = newSubscription
    val sub2 = newSubscription
    assertEquals(sub1.hashCode(), sub2.hashCode(), "hashCode should be equal for equal objects")
  }

  test("set deduplication") {
    val sub1 = newSubscription
    val sub2 = newSubscription
    val set = Set(sub1, sub2)
    assertEquals(set.size, 1, "Set should deduplicate equal subscriptions")
  }

  test("set contains") {
    val sub1 = newSubscription
    val sub2 = newSubscription
    val set = Set(sub1)
    assert(set.contains(sub2), "Set should contain equal subscription")
  }
}
