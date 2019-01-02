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
package com.netflix.atlas.eval.model

import nl.jqno.equalsverifier.EqualsVerifier
import nl.jqno.equalsverifier.Warning
import org.scalatest.FunSuite

class ChunkDataSuite extends FunSuite {
  test("ArrayData equals") {
    EqualsVerifier
      .forClass(classOf[ArrayData])
      .suppress(Warning.NULL_FIELDS)
      .verify()
  }

  test("ArrayData encode empty") {
    val empty = ArrayData(Array())
    assert(empty.toJson === """{"type":"array","values":[]}""")
  }

  test("ArrayData encode values") {
    val data = ArrayData(Array(1.0, 2.0, 3.0))
    assert(data.toJson === """{"type":"array","values":[1.0,2.0,3.0]}""")
  }
}
