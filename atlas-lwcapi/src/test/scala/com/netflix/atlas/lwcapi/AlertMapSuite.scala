/*
 * Copyright 2014-2016 Netflix, Inc.
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

import com.netflix.atlas.core.model.Query
import com.netflix.atlas.lwcapi.AlertMap.ReturnableExpression
import org.scalatest.FunSuite

class AlertMapSuite extends FunSuite {
  test("exprForDataExpr returns an empty set if not found") {
    val x = AlertMap()

    assert(x.expressionsForCluster("foo") === List())
  }

  test("deleting") {
    val query1 = ExpressionWithFrequency("nf.cluster,skan-test,:eq,:sum,:des-fast", 30000)
    val ds1a = "nf.cluster,skan-test,:eq,:sum"
    val ret1 = ReturnableExpression(query1.expression, 30000, List(ds1a))

    val query2 = ExpressionWithFrequency("nf.cluster,skan-test,:eq,:sum", 30000)
    val ds2a = "nf.cluster,skan-test,:eq,:sum"
    val ret2 = ReturnableExpression(query2.expression, 30000, List(ds2a))

    val x = AlertMap()

    x.addExpr(query1)
    x.addExpr(query2)
    assert(x.expressionsForCluster("skan-test") === List(ret1, ret2))
    x.delExpr(query1)
    assert(x.expressionsForCluster("skan-test") === List(ret2))
    x.delExpr(query2)
    assert(x.expressionsForCluster("skan-test") === List())
  }
}
