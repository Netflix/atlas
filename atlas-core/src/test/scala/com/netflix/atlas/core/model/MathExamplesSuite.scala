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
package com.netflix.atlas.core.model

import com.netflix.atlas.core.stacklang.BaseExamplesSuite
import com.netflix.atlas.core.stacklang.Vocabulary

class MathExamplesSuite extends BaseExamplesSuite {

  override def vocabulary: Vocabulary = MathVocabulary

  test("toString with offsets") {
    val expr = eval("name,test,:eq,:sum,1h,:offset")
    assertEquals(expr.toString, "name,test,:eq,:sum,PT1H,:offset")
  }

  test("rewrite toString with offsets") {
    val expr = eval("name,test,:eq,:dist-avg,1h,:offset")
    assertEquals(expr.toString, "name,test,:eq,:dist-avg,PT1H,:offset")
  }

  test("rewrite toString with offsets and cq") {
    val expr = eval("name,test,:eq,:avg,1h,:offset,app,foo,:eq,:cq")
    assertEquals(expr.toString, "name,test,:eq,app,foo,:eq,:and,:avg,PT1H,:offset")
  }

  test("rewrite group by toString with offsets") {
    val expr = eval("name,test,:eq,:dist-avg,(,cluster,),:by,1h,:offset")
    assertEquals(expr.toString, "name,test,:eq,:dist-avg,PT1H,:offset,(,cluster,),:by")
  }

  test("rewrite group by toString with offsets and cq") {
    val expr = eval("name,test,:eq,:dist-avg,(,cluster,),:by,1h,:offset,app,foo,:eq,:cq")
    assertEquals(
      expr.toString,
      "name,test,:eq,app,foo,:eq,:and,:dist-avg,PT1H,:offset,(,cluster,),:by"
    )
  }
}
