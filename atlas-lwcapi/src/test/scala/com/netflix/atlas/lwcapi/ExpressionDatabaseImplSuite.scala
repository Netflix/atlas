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
import org.scalatest.FunSuite

class ExpressionDatabaseImplSuite extends FunSuite {
  val splitter = ExpressionSplitterImpl()

  test("exprForDataExpr returns an empty set if not found") {
    val x = ExpressionDatabaseImpl()

    assert(x.expressionsForCluster("foo") === List())
  }

  test("index is rebuilt") {
    val query1 = "nf.cluster,skan-test,:eq,:sum,:des-fast"
    val freq1 = 30000

    val x = ExpressionDatabaseImpl()

    val split1 = splitter.split(query1, freq1)
    assert(split1.expressions.size === 1)
    x.addExpr(split1.expressions.head, split1.queries.head)

    var ret = x.expressionsForCluster("skan-test")
    assert(ret.isEmpty)

    var counter = 0
    while (ret.isEmpty && counter < 40) { // about 4 seconds
      Thread.sleep(100)
      ret = x.expressionsForCluster("skan-test")
      counter += 1
    }
    assert(ret.nonEmpty)
  }

  test("same expression different frequency") {
    val query1 = "nf.cluster,skan-test,:eq,:sum,:des-fast"
    val freq1 = 30000
    val id1 = "CxmlI6L5YBQcpWOrayncZKeZekg"
    val ds1a = "nf.cluster,skan-test,:eq,:sum"
    val ret1 = ExpressionWithFrequency(ds1a, freq1, id1)

    val query2 = "nf.cluster,skan-test,:eq,:sum,:des-fast"
    val freq2 = 50000
    val id2 = "xcKZ6tCd4vSMUY-Ug3bToNy3L6k"
    val ds2a = "nf.cluster,skan-test,:eq,:sum"
    val ret2 = ExpressionWithFrequency(ds2a, freq2, id2)

    val x = ExpressionDatabaseImpl()
    x.setTestMode()

    val split1 = splitter.split(query1, freq1)
    assert(split1.expressions.size === 1)
    x.addExpr(split1.expressions.head, split1.queries.head)

    val split2 = splitter.split(query2, freq2)
    assert(split2.expressions.size === 1)
    x.addExpr(split2.expressions.head, split2.queries.head)

    var ret = x.expressionsForCluster("skan-test")
    assert(ret.size === 2)
    assert(ret.contains(ret1))
    assert(ret.contains(ret2))
  }

  test("deleting") {
    val query1 = "nf.cluster,skan-test,:eq,:sum,:des-fast"
    val freq1 = 30000
    val id1 = "CxmlI6L5YBQcpWOrayncZKeZekg"
    val ds1a = "nf.cluster,skan-test,:eq,:sum"
    val ret1 = ExpressionWithFrequency(ds1a, freq1, id1)

    val query2 = "nf.cluster,skan-test,:eq,:sum,:des-fast"
    val freq2 = 50000
    val id2 = "xcKZ6tCd4vSMUY-Ug3bToNy3L6k"
    val ds2a = "nf.cluster,skan-test,:eq,:sum"
    val ret2 = ExpressionWithFrequency(ds2a, freq2, id2)

    val x = ExpressionDatabaseImpl()
    x.setTestMode()

    val split1 = splitter.split(query1, freq1)
    assert(split1.expressions.size === 1)
    x.addExpr(split1.expressions.head, split1.queries.head)

    val split2 = splitter.split(query2, freq2)
    assert(split2.expressions.size === 1)
    x.addExpr(split2.expressions.head, split2.queries.head)

    var ret = x.expressionsForCluster("skan-test")
    assert(ret.size === 2)
    assert(ret.contains(ret1))
    assert(ret.contains(ret2))

    x.delExpr(id1)
    ret = x.expressionsForCluster("skan-test")
    assert(ret.size === 1)
    assert(ret.contains(ret2))

    x.delExpr(id2)
    ret = x.expressionsForCluster("skan-test")
    assert(ret === List())
  }

  test("hasExpr") {
    val expr1 = ExpressionWithFrequency("nf.cluster,skan-test,:eq,:sum", 30000, "idhere")

    val x = ExpressionDatabaseImpl()

    assert(x.hasExpr("idhere") === false)

    x.addExpr(expr1, Query.True)
    assert(x.hasExpr("idhere"))

    x.delExpr("idhere")
    assert(x.hasExpr("idhere") === false)
  }

  test("ignores matches for other clusters") {
    val query1 = "nf.cluster,skan-test,:eq,:sum,:des-fast"
    val freq1 = 30000
    val id1 = "CxmlI6L5YBQcpWOrayncZKeZekg"
    val ds1a = "nf.cluster,skan-test,:eq,:sum"
    val ret1 = ExpressionWithFrequency(ds1a, freq1, id1)

    val query2 = "nf.cluster,foo-test,:eq,:sum,:des-fast"
    val freq2 = 30000
    val id2 = "TmufMvW12uix44g903buDb-0kH4"
    val ds2a = "nf.cluster,foo-test,:eq,:sum"
    val ret2 = ExpressionWithFrequency(ds2a, freq2, id2)

    val x = ExpressionDatabaseImpl()
    x.setTestMode()

    val split1 = splitter.split(query1, freq1)
    assert(split1.expressions.size === 1)
    x.addExpr(split1.expressions.head, split1.queries.head)

    val split2 = splitter.split(query2, freq2)
    assert(split2.expressions.size === 1)
    x.addExpr(split2.expressions.head, split2.queries.head)

    assert(x.expressionsForCluster("bar-test") === List())
    assert(x.expressionsForCluster("foo-test") === List(ret2))
  }
}
