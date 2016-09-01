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

class ExpressionSplitterSuite extends FunSuite {
  private val query1 = "nf.cluster,skan-test,:eq,name,memUsed,:eq,:and,:avg,(,nf.node,),:by,4500000000,:gt,30,:rolling-count,15,:ge,$nf.node,:legend"
  private val ds1a = "nf.cluster,skan-test,:eq,name,memUsed,:eq,:and,:sum,(,nf.node,),:by"
  private val ds1b = "nf.cluster,skan-test,:eq,name,memUsed,:eq,:and,:count,(,nf.node,),:by"
  private val matchList1 = List(
    Query.And(
      Query.Equal("nf.cluster", "skan-test"),
      Query.Equal("name", "memUsed")
    )
  )

  private val interner = new ExpressionSplitter.QueryInterner()

  test("splits single expression into data expressions") {
    val splitter = ExpressionSplitter(interner)
    val ret = splitter.split(query1)
    assert(ret.isDefined)
    assert(ret.get.dataExprs === List(ds1a, ds1b))
    assert(ret.get.matchExprs === matchList1)
  }

  test("splits compound expression into data expressions") {
    val splitter = ExpressionSplitter(interner)
    val ret = splitter.split(query1 + "," + query1)
    assert(ret.isDefined)
    assert(ret.get.dataExprs === List(ds1a, ds1b))
    assert(ret.get.matchExprs === matchList1)
  }

  test("returns None for invalid expressions") {
    val splitter = ExpressionSplitter(interner)
    val ret = splitter.split("this")
    assert(ret.isEmpty)
  }
}
