/*
 * Copyright 2014-2019 Netflix, Inc.
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
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

class ExpressionSplitterSuite extends AnyFunSuite {
  private val query1 =
    "nf.cluster,skan-test,:eq,name,memUsed,:eq,:and,:avg,(,nf.node,),:by,4500000000,:gt,30,:rolling-count,15,:ge,$nf.node,:legend"
  private val frequency1 = 60000
  private val ds1a = "nf.cluster,skan-test,:eq,name,memUsed,:eq,:and,:count,(,nf.node,),:by"
  private val ds1b = "nf.cluster,skan-test,:eq,name,memUsed,:eq,:and,:sum,(,nf.node,),:by"
  private val matchList1 = Query.Equal("nf.cluster", "skan-test")

  private val splitter = new ExpressionSplitter(ConfigFactory.load())

  test("splits single expression into data expressions") {
    val actual = splitter.split(query1, frequency1)
    val expected = List(
      Subscription(matchList1, ExpressionMetadata(ds1a, frequency1)),
      Subscription(matchList1, ExpressionMetadata(ds1b, frequency1))
    ).reverse
    assert(actual === expected)
  }

  test("splits compound expression into data expressions") {
    val expr = query1 + "," + query1
    val actual = splitter.split(expr, frequency1)
    val expected = List(
      Subscription(matchList1, ExpressionMetadata(ds1a, frequency1)),
      Subscription(matchList1, ExpressionMetadata(ds1b, frequency1))
    ).reverse
    assert(actual === expected)
  }

  test("throws IAE for invalid expressions") {
    val msg = intercept[IllegalArgumentException] {
      splitter.split("foo", frequency1)
    }
    assert(msg.getMessage === "expression is invalid")
  }

  test("throws IAE for expressions with offset") {
    val expr = "name,foo,:eq,:sum,PT168H,:offset"
    val msg = intercept[IllegalArgumentException] {
      splitter.split(expr, frequency1)
    }
    assert(msg.getMessage === s":offset not supported for streaming evaluation [[$expr]]")
  }

  test("throws IAE for expressions with style offset") {
    val expr = "name,foo,:eq,:sum,(,0h,1w,),:offset"
    val msg = intercept[IllegalArgumentException] {
      splitter.split(expr, frequency1)
    }
    val badExpr = "name,foo,:eq,:sum,PT168H,:offset"
    assert(msg.getMessage === s":offset not supported for streaming evaluation [[$badExpr]]")
  }

  //
  // Tests for compress()
  //

  //
  // Keeping keys
  //

  test("compress keeps nf.app") {
    val ret = splitter.compress(Query.Equal("nf.app", "skan"))
    assert(ret === Query.Equal("nf.app", "skan"))
  }

  test("compress keeps nf.stack") {
    val ret = splitter.compress(Query.Equal("nf.stack", "skan"))
    assert(ret === Query.Equal("nf.stack", "skan"))
  }

  test("compress keeps nf.cluster") {
    val ret = splitter.compress(Query.Equal("nf.cluster", "skan"))
    assert(ret === Query.Equal("nf.cluster", "skan"))
  }

  test("compress removes arbitrary other equal comparisons") {
    val ret = splitter.compress(Query.Equal("xxx", "skan"))
    assert(ret === Query.True)
  }

  //
  // And
  //

  test("compress converts true,true,:and to true") {
    val ret = splitter.compress(Query.And(Query.True, Query.True))
    assert(ret === Query.True)
  }

  test("compress converts false,true,:and to false") {
    val ret = splitter.compress(Query.And(Query.False, Query.True))
    assert(ret === Query.False)
  }

  test("compress converts true,false,:and to false") {
    val ret = splitter.compress(Query.And(Query.True, Query.False))
    assert(ret === Query.False)
  }

  test("compress converts false,false,:and to false") {
    val ret = splitter.compress(Query.And(Query.False, Query.False))
    assert(ret === Query.False)
  }

  test("compress converts nf.app,b,:eq,:true,:and to nf.app,b,:eq") {
    val ret = splitter.compress(Query.And(Query.Equal("nf.app", "b"), Query.True))
    assert(ret === Query.Equal("nf.app", "b"))
  }

  test("compress converts :true,nf.app,b,:eq,:and to nf.app,b,:eq") {
    val ret = splitter.compress(Query.And(Query.True, Query.Equal("nf.app", "b")))
    assert(ret === Query.Equal("nf.app", "b"))
  }

  test("compress converts nf.app,b,:eq,:false,:and to :false") {
    val ret = splitter.compress(Query.And(Query.Equal("nf.app", "b"), Query.False))
    assert(ret === Query.False)
  }

  test("compress converts :false,nf.app,b,:eq,:and to :false") {
    val ret = splitter.compress(Query.And(Query.False, Query.Equal("nf.app", "b")))
    assert(ret === Query.False)
  }

  test("compress converts nf.stack,iep,:eq,nf.app,b,:eq,:and to identity") {
    val query = Query.And(Query.Equal("nf.stack", "iep"), Query.Equal("nf.app", "b"))
    val ret = splitter.compress(query)
    assert(ret === query)
  }

  //
  // Or
  //

  test("compress converts false,true,:or to true") {
    val ret = splitter.compress(Query.Or(Query.False, Query.True))
    assert(ret === Query.True)
  }

  test("compress converts true,false,:or to true") {
    val ret = splitter.compress(Query.Or(Query.True, Query.False))
    assert(ret === Query.True)
  }

  test("compress converts false,false,:or to false") {
    val ret = splitter.compress(Query.Or(Query.False, Query.False))
    assert(ret === Query.False)
  }

  test("compress converts a,b,:eq,c:d::eq:and to :true") {
    val ret = splitter.compress(Query.And(Query.Equal("a", "b"), Query.Equal("a", "b")))
    assert(ret === Query.True)
  }

  test("compress converts nf.stack,iep,:eq,nf.app,b,:eq,:or to identity") {
    val query = Query.Or(Query.Equal("nf.stack", "iep"), Query.Equal("nf.app", "b"))
    val ret = splitter.compress(query)
    assert(ret === query)
  }

  //
  // Not
  //

  test("compress converts :true,:not to :true") {
    // yes, not converts to true here on purpose.
    val ret = splitter.compress(Query.Not(Query.True))
    assert(ret === Query.True)
  }

  test("compress converts :false,:not to :true") {
    val ret = splitter.compress(Query.Not(Query.False))
    assert(ret === Query.True)
  }

  test("compress converts nf.stack,iep,:eq,:not to identity") {
    val query = Query.Not(Query.Equal("nf.stack", "iep"))
    val ret = splitter.compress(query)
    assert(ret === query)
  }

  //
  // Interner exerciser
  //
  test("interner exerciser") {
    val tests = List(
      Query.True,
      Query.False,
      Query.Equal("a", "b"),
      Query.LessThan("a", "123"),
      Query.LessThanEqual("a", "123"),
      Query.GreaterThan("a", "123"),
      Query.GreaterThanEqual("a", "123"),
      Query.Regex("a", "b"),
      Query.RegexIgnoreCase("a", "b"),
      Query.In("a", List("b", "c")),
      Query.HasKey("a"),
      Query.And(Query.True, Query.True),
      Query.Or(Query.True, Query.True),
      Query.Not(Query.True)
    )
    tests.foreach(query => assert(splitter.intern(query) == query))
  }

}
