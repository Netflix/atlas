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
    val x = AlertMap()

    val key = ExpressionWithFrequency("testing", 1)

    assert(x.exprsForDataExprSet("a", Set(key)) === Set())
  }

  test("Two data expressions from the same expr map to the same set") {
    val query1 = ExpressionWithFrequency("nf.cluster,lando-test,:eq,name,_HeapMemoryUsage_used,:eq,:and,:avg,(,nf.node,),:by")
    val ds1a = ExpressionWithFrequency("nf.cluster,lando-test,:eq,name,_HeapMemoryUsage_used,:eq,:and,:sum,(,nf.node,),:by")
    val ds1b = ExpressionWithFrequency("nf.cluster,lando-test,:eq,name,_HeapMemoryUsage_used,:eq,:and,:count,(,nf.node,),:by")

    val x = AlertMap()

    x.addExpr("a", query1)
    val result = x.exprsForDataExprSet("a", Set(ds1a, ds1b))
    assert(result === Set(query1))
  }

  test("two expressions sharing same data expression can be found") {
    val query1 = ExpressionWithFrequency("nf.cluster,lando-test,:eq,name,_HeapMemoryUsage_used,:eq,:and,:sum,:des-fast")
    val query2 = ExpressionWithFrequency("nf.cluster,lando-test,:eq,name,_HeapMemoryUsage_used,:eq,:and,:sum")
    val ds1 = ExpressionWithFrequency("nf.cluster,lando-test,:eq,name,_HeapMemoryUsage_used,:eq,:and,:sum")

    val x = AlertMap()

    x.addExpr("a", query1)
    assert(x.exprsForDataExprSet("a", Set(ds1)) === Set(query1))
    x.addExpr("a", query2)
    assert(x.exprsForDataExprSet("a", Set(ds1)) === Set(query1, query2))
  }

  test("deleting") {
    val query1 = ExpressionWithFrequency("nf.cluster,lando-test,:eq,name,_HeapMemoryUsage_used,:eq,:and,:sum,:des-fast")
    val query2 = ExpressionWithFrequency("nf.cluster,lando-test,:eq,name,_HeapMemoryUsage_used,:eq,:and,:sum")
    val ds1 = ExpressionWithFrequency("nf.cluster,lando-test,:eq,name,_HeapMemoryUsage_used,:eq,:and,:sum")

    val x = AlertMap()

    x.addExpr("a", query1)
    x.addExpr("a", query2)
    assert(x.exprsForDataExprSet("a", Set(ds1)) === Set(query1, query2))
    x.delExpr("a", query1)
    assert(x.exprsForDataExprSet("a", Set(ds1)) === Set(query2))
    x.delExpr("a", query2)
    assert(x.allDataExpressions() === Set())
  }

  test("deleting a non-existing expression is silent") {
    val query1 = ExpressionWithFrequency("nf.cluster,lando-test,:eq,name,_HeapMemoryUsage_used,:eq,:and,:sum,:des-fast")

    val x = AlertMap()

    x.delExpr("a", query1)
    assert(x.allDataExpressions() === Set())
  }

  test("globalAlertMap") {
    val key = ExpressionWithFrequency("testing", 1)
    assert(AlertMap.globalAlertMap.exprsForDataExprSet("a", Set(key)) === Set())
  }
}
