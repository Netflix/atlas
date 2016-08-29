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

import org.scalatest.FunSuite

class AlertMapSuite extends FunSuite {
  test("exprForDataExpr returns an empty set if not found") {
    val query1 = ExpressionWithFrequency("nf.cluster,lando-test,:eq,name,_HeapMemoryUsage_used,:eq,:and,:avg,(,nf.node,),:by")
    val x = AlertMap()

    assert(x.dataExpressionsForCluster("foo") === Set())
  }

  test("deleting") {
    val query1 = ExpressionWithFrequency("nf.cluster,lando-test,:eq,:sum,:des-fast")
    val query2 = ExpressionWithFrequency("nf.cluster,lando-test,:eq,:sum")

    val x = AlertMap()

    x.addExpr(query1)
    x.addExpr(query2)
    assert(x.dataExpressionsForCluster("lando-test") === Set(query1, query2))
    x.delExpr(query1)
    assert(x.dataExpressionsForCluster("lando-test") === Set(query2))
    x.delExpr(query2)
    assert(x.dataExpressionsForCluster("lando-test") === Set())
  }
}
