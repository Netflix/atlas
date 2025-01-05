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

import com.netflix.atlas.core.model.Query
import com.netflix.atlas.eval.model.ExprType
import com.netflix.spectator.atlas.impl.Parser
import com.typesafe.config.ConfigFactory
import munit.FunSuite

class ExpressionSplitterSuite extends FunSuite {

  private val query1 =
    "nf.cluster,skan-test,:eq,name,memUsed,:eq,:and,:avg,(,nf.node,),:by,4500000000,:gt,30,:rolling-count,15,:ge,$nf.node,:legend"
  private val frequency1 = 60000
  private val ds1a = "nf.cluster,skan-test,:eq,name,memUsed,:eq,:and,:count,(,nf.node,),:by"
  private val ds1b = "nf.cluster,skan-test,:eq,name,memUsed,:eq,:and,:sum,(,nf.node,),:by"
  private val matchList1 = Parser.parseQuery("nf.cluster,skan-test,:eq")

  private val splitter = new ExpressionSplitter(ConfigFactory.load())

  test("splits single expression into data expressions") {
    val actual = splitter.split(query1, ExprType.TIME_SERIES, frequency1)
    val expected = List(
      Subscription(matchList1, ExpressionMetadata(ds1a, ExprType.TIME_SERIES, frequency1)),
      Subscription(matchList1, ExpressionMetadata(ds1b, ExprType.TIME_SERIES, frequency1))
    ).reverse
    assertEquals(actual, expected)
  }

  test("splits compound expression into data expressions") {
    val expr = query1 + "," + query1
    val actual = splitter.split(expr, ExprType.TIME_SERIES, frequency1)
    val expected = List(
      Subscription(matchList1, ExpressionMetadata(ds1a, ExprType.TIME_SERIES, frequency1)),
      Subscription(matchList1, ExpressionMetadata(ds1b, ExprType.TIME_SERIES, frequency1))
    ).reverse
    assertEquals(actual, expected)
  }

  test("splits compound trace time series expression into data expressions") {
    def childExpr(e: String): String = {
      s"nf.app,api,:eq,nf.cluster,skan-test,:eq,:child,$e,:span-time-series"
    }
    val expr = s"${childExpr(query1)},${childExpr(query1)}"
    val actual = splitter.split(expr, ExprType.TRACE_TIME_SERIES, frequency1)
    val expected = List(
      Subscription(
        MatchesAll,
        ExpressionMetadata(childExpr(ds1a), ExprType.TRACE_TIME_SERIES, frequency1)
      ),
      Subscription(
        MatchesAll,
        ExpressionMetadata(childExpr(ds1b), ExprType.TRACE_TIME_SERIES, frequency1)
      )
    ).reverse
    assertEquals(actual, expected)
  }

  test("throws IAE for invalid expressions") {
    val msg = intercept[IllegalArgumentException] {
      splitter.split("foo", ExprType.TIME_SERIES, frequency1)
    }
    assertEquals(msg.getMessage, "invalid value on stack: foo")
  }

  test("throws IAE for expressions with offset") {
    val expr = "name,foo,:eq,:sum,PT168H,:offset"
    val msg = intercept[IllegalArgumentException] {
      splitter.split(expr, ExprType.TIME_SERIES, frequency1)
    }
    assertEquals(msg.getMessage, s":offset not supported for streaming evaluation [[$expr]]")
  }

  test("throws IAE for expressions with style offset") {
    val expr = "name,foo,:eq,:sum,(,0h,1w,),:offset"
    val msg = intercept[IllegalArgumentException] {
      splitter.split(expr, ExprType.TIME_SERIES, frequency1)
    }
    val badExpr = "name,foo,:eq,:sum,PT168H,:offset"
    assertEquals(msg.getMessage, s":offset not supported for streaming evaluation [[$badExpr]]")
  }

  //
  // Tests for compress()
  //

  //
  // Keeping keys
  //

  test("compress keeps nf.app") {
    val ret = splitter.compress(Query.Equal("nf.app", "skan"))
    assertEquals(ret, Query.Equal("nf.app", "skan"))
  }

  test("compress keeps nf.stack") {
    val ret = splitter.compress(Query.Equal("nf.stack", "skan"))
    assertEquals(ret, Query.Equal("nf.stack", "skan"))
  }

  test("compress keeps nf.cluster") {
    val ret = splitter.compress(Query.Equal("nf.cluster", "skan"))
    assertEquals(ret, Query.Equal("nf.cluster", "skan"))
  }

  test("compress adds cluster condition based on nf.asg") {
    val ret = splitter.compress(Query.Equal("nf.asg", "skan-v001"))
    assertEquals(ret, Query.Equal("nf.cluster", "skan"))
  }

  test("compress with bad nf.asg value") {
    val ret = splitter.compress(Query.Equal("nf.asg", "--v001"))
    assertEquals(ret, Query.True)
  }

  test("compress removes arbitrary other equal comparisons") {
    val ret = splitter.compress(Query.Equal("xxx", "skan"))
    assertEquals(ret, Query.True)
  }

  //
  // And
  //

  test("compress converts true,true,:and to true") {
    val ret = splitter.compress(Query.And(Query.True, Query.True))
    assertEquals(ret, Query.True)
  }

  test("compress converts false,true,:and to false") {
    val ret = splitter.compress(Query.And(Query.False, Query.True))
    assertEquals(ret, Query.False)
  }

  test("compress converts true,false,:and to false") {
    val ret = splitter.compress(Query.And(Query.True, Query.False))
    assertEquals(ret, Query.False)
  }

  test("compress converts false,false,:and to false") {
    val ret = splitter.compress(Query.And(Query.False, Query.False))
    assertEquals(ret, Query.False)
  }

  test("compress converts nf.app,b,:eq,:true,:and to nf.app,b,:eq") {
    val ret = splitter.compress(Query.And(Query.Equal("nf.app", "b"), Query.True))
    assertEquals(ret, Query.Equal("nf.app", "b"))
  }

  test("compress converts :true,nf.app,b,:eq,:and to nf.app,b,:eq") {
    val ret = splitter.compress(Query.And(Query.True, Query.Equal("nf.app", "b")))
    assertEquals(ret, Query.Equal("nf.app", "b"))
  }

  test("compress converts nf.app,b,:eq,:false,:and to :false") {
    val ret = splitter.compress(Query.And(Query.Equal("nf.app", "b"), Query.False))
    assertEquals(ret, Query.False)
  }

  test("compress converts :false,nf.app,b,:eq,:and to :false") {
    val ret = splitter.compress(Query.And(Query.False, Query.Equal("nf.app", "b")))
    assertEquals(ret, Query.False)
  }

  test("compress converts nf.stack,iep,:eq,nf.app,b,:eq,:and to identity") {
    val query = Query.And(Query.Equal("nf.stack", "iep"), Query.Equal("nf.app", "b"))
    val ret = splitter.compress(query)
    assertEquals(ret, query)
  }

  //
  // Or
  //

  test("compress converts false,true,:or to true") {
    val ret = splitter.compress(Query.Or(Query.False, Query.True))
    assertEquals(ret, Query.True)
  }

  test("compress converts true,false,:or to true") {
    val ret = splitter.compress(Query.Or(Query.True, Query.False))
    assertEquals(ret, Query.True)
  }

  test("compress converts false,false,:or to false") {
    val ret = splitter.compress(Query.Or(Query.False, Query.False))
    assertEquals(ret, Query.False)
  }

  test("compress converts a,b,:eq,c:d::eq:and to :true") {
    val ret = splitter.compress(Query.And(Query.Equal("a", "b"), Query.Equal("a", "b")))
    assertEquals(ret, Query.True)
  }

  test("compress converts nf.stack,iep,:eq,nf.app,b,:eq,:or to identity") {
    val query = Query.Or(Query.Equal("nf.stack", "iep"), Query.Equal("nf.app", "b"))
    val ret = splitter.compress(query)
    assertEquals(ret, query)
  }

  //
  // Not
  //

  test("compress converts :true,:not to :true") {
    // yes, not converts to true here on purpose.
    val ret = splitter.compress(Query.Not(Query.True))
    assertEquals(ret, Query.True)
  }

  test("compress converts :false,:not to :true") {
    val ret = splitter.compress(Query.Not(Query.False))
    assertEquals(ret, Query.True)
  }

  test("compress converts nf.stack,iep,:eq,:not to identity") {
    val query = Query.Not(Query.Equal("nf.stack", "iep"))
    val ret = splitter.compress(query)
    assertEquals(ret, query)
  }

}
