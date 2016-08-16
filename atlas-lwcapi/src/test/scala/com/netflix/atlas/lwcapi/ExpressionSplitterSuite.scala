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

class ExpressionSplitterSuite extends FunSuite {
  private val query1 = "nf.cluster,lando-test,:eq,name,_HeapMemoryUsage_used,:eq,:and,:avg,(,nf.node,),:by,4500000000,:gt,30,:rolling-count,15,:ge,$nf.node,:legend"
  private val ds1a = "nf.cluster,lando-test,:eq,name,_HeapMemoryUsage_used,:eq,:and,:sum,(,nf.node,),:by"
  private val ds1b = "nf.cluster,lando-test,:eq,name,_HeapMemoryUsage_used,:eq,:and,:count,(,nf.node,),:by"

  test("splits single expression into data expressions") {
    val splitter = ExpressionSplitter()
    val ret = splitter.split(query1)
    assert(ret === Set(ds1a, ds1b))
  }

  test("splits compound expression into data expressions") {
    val splitter = ExpressionSplitter()
    val ret = splitter.split(query1 + "," + query1)
    assert(ret === Set(ds1a, ds1b))
  }

  test("splits single ExpressionWithFrequency into data expressions") {
    val splitter = ExpressionSplitter()
    val ret = splitter.split(ExpressionWithFrequency(query1, 999))
    assert(ret === Set(ExpressionWithFrequency(ds1a, 999), ExpressionWithFrequency(ds1b, 999)))
  }
}
