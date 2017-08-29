/*
 * Copyright 2014-2017 Netflix, Inc.
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

class ExpressionDatabaseSuite extends FunSuite {
  val splitter = new ExpressionSplitter()

  test("exprForDataExpr returns an empty set if not found") {
    val x = new ExpressionDatabase()

    assert(x.expressionsForCluster("foo") === List())
  }

  test("index is rebuilt") {
    val query1 = "nf.cluster,skan-test,:eq,:sum,:des-fast"
    val freq1 = 30000

    val x = new ExpressionDatabase()

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
    val id1 = "0b19a523a2f960141ca563ab6b29dc64a7997a48"
    val ds1a = "nf.cluster,skan-test,:eq,:sum"
    val ret1 = ExpressionWithFrequency(ds1a, freq1, id1)

    val query2 = "nf.cluster,skan-test,:eq,:sum,:des-fast"
    val freq2 = 50000
    val id2 = "c5c299ead09de2f48c518f948376d3a0dcb72fa9"
    val ds2a = "nf.cluster,skan-test,:eq,:sum"
    val ret2 = ExpressionWithFrequency(ds2a, freq2, id2)

    val x = new ExpressionDatabase()

    val split1 = splitter.split(query1, freq1)
    assert(split1.expressions.size === 1)
    x.addExpr(split1.expressions.head, split1.queries.head)

    val split2 = splitter.split(query2, freq2)
    assert(split2.expressions.size === 1)
    x.addExpr(split2.expressions.head, split2.queries.head)

    x.regenerateQueryIndex()
    val ret = x.expressionsForCluster("skan-test")
    assert(ret.size === 2)
    assert(ret.contains(ret1))
    assert(ret.contains(ret2))
  }

  test("deleting") {
    val query1 = "nf.cluster,skan-test,:eq,:sum,:des-fast"
    val freq1 = 30000
    val id1 = "0b19a523a2f960141ca563ab6b29dc64a7997a48"
    val ds1a = "nf.cluster,skan-test,:eq,:sum"
    val ret1 = ExpressionWithFrequency(ds1a, freq1, id1)

    val query2 = "nf.cluster,skan-test,:eq,:sum,:des-fast"
    val freq2 = 50000
    val id2 = "c5c299ead09de2f48c518f948376d3a0dcb72fa9"
    val ds2a = "nf.cluster,skan-test,:eq,:sum"
    val ret2 = ExpressionWithFrequency(ds2a, freq2, id2)

    val x = new ExpressionDatabase()

    val split1 = splitter.split(query1, freq1)
    assert(split1.expressions.size === 1)
    x.addExpr(split1.expressions.head, split1.queries.head)

    val split2 = splitter.split(query2, freq2)
    assert(split2.expressions.size === 1)
    x.addExpr(split2.expressions.head, split2.queries.head)

    x.regenerateQueryIndex()
    var ret = x.expressionsForCluster("skan-test")
    assert(ret.size === 2)
    assert(ret.contains(ret1))
    assert(ret.contains(ret2))

    x.delExpr(id1)
    x.regenerateQueryIndex()
    ret = x.expressionsForCluster("skan-test")
    assert(ret.size === 1)
    assert(ret.contains(ret2))

    x.delExpr(id2)
    x.regenerateQueryIndex()
    ret = x.expressionsForCluster("skan-test")
    assert(ret === List())
  }

  test("hasExpr") {
    val expr1 = ExpressionWithFrequency("nf.cluster,skan-test,:eq,:sum", 30000, "idhere")

    val x = new ExpressionDatabase()

    assert(x.hasExpr("idhere") === false)

    x.addExpr(expr1, Query.True)
    x.regenerateQueryIndex()
    assert(x.hasExpr("idhere"))

    x.delExpr("idhere")
    x.regenerateQueryIndex()
    assert(x.hasExpr("idhere") === false)
  }

  test("ignores matches for other clusters") {
    val query1 = "nf.cluster,skan-test,:eq,:sum,:des-fast"
    val freq1 = 30000

    val query2 = "nf.cluster,foo-test,:eq,:sum,:des-fast"
    val freq2 = 30000
    val id2 = "4e6b9f32f5b5dae8b1e3883dd376ee0dbfb4907e"
    val ds2a = "nf.cluster,foo-test,:eq,:sum"
    val ret2 = ExpressionWithFrequency(ds2a, freq2, id2)

    val x = new ExpressionDatabase()

    val split1 = splitter.split(query1, freq1)
    assert(split1.expressions.size === 1)
    x.addExpr(split1.expressions.head, split1.queries.head)

    val split2 = splitter.split(query2, freq2)
    assert(split2.expressions.size === 1)
    x.addExpr(split2.expressions.head, split2.queries.head)

    x.regenerateQueryIndex()
    assert(x.expressionsForCluster("bar-test") === List())
    assert(x.expressionsForCluster("foo-test") === List(ret2))
  }
}
