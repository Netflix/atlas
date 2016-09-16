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
import com.netflix.atlas.lwcapi.ExpressionSplitter.{QueryContainer, SplitResult}
import org.scalatest.FunSuite

class ExpressionSplitterSuite extends FunSuite {
  private val query1 = "nf.cluster,skan-test,:eq,name,memUsed,:eq,:and,:avg,(,nf.node,),:by,4500000000,:gt,30,:rolling-count,15,:ge,$nf.node,:legend"
  private val frequency1 = 60000
  private val ds1a = "nf.cluster,skan-test,:eq,name,memUsed,:eq,:and,:count,(,nf.node,),:by"
  private val ds1b = "nf.cluster,skan-test,:eq,name,memUsed,:eq,:and,:sum,(,nf.node,),:by"
  private val matchList1 = Query.Equal("nf.cluster", "skan-test")
  private val expected1 = List(
    QueryContainer(matchList1, ds1a),
    QueryContainer(matchList1, ds1b)
  )

  private val interner = new ExpressionSplitter.QueryInterner()
  private val splitter = ExpressionSplitter(interner)

  test("splits single expression into data expressions") {
    val ret = splitter.split(ExpressionWithFrequency(query1, frequency1))
    assert(ret === SplitResult(query1, frequency1, "1ylCY_sReTZWQ-U7zXqfl8S7ARs", expected1))
  }

  test("splits compound expression into data expressions") {
    val expr = query1 + "," + query1
    val ret = splitter.split(ExpressionWithFrequency(expr, frequency1))
    assert(ret === SplitResult(expr, frequency1, "1ylCY_sReTZWQ-U7zXqfl8S7ARs", expected1))
  }

  test("returns None for invalid expressions") {
    val msg = intercept[IllegalArgumentException] {
      splitter.split(ExpressionWithFrequency("foo", frequency1))
    }
   assert(msg.getMessage === "Expression is not a valid expression")
  }
}
