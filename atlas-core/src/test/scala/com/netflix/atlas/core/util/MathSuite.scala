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
package com.netflix.atlas.core.util

import org.scalatest.FunSuite


class MathSuite extends FunSuite {

  import java.lang.{Double => JDouble}

  import com.netflix.atlas.core.util.Math._

  test("isNearlyZero") {
    assert(isNearlyZero(1.0) === false)
    assert(isNearlyZero(-1000.0) === false)
    assert(isNearlyZero(0.0) === true)
    assert(isNearlyZero(1e-12) === false)
    assert(isNearlyZero(1e-13) === true)
    assert(isNearlyZero(Double.NaN) === true)
  }

  test("toBoolean") {
    assert(toBoolean(1.0) === true)
    assert(toBoolean(-1000.0) === true)
    assert(toBoolean(0.0) === false)
    assert(toBoolean(1e-12) === true)
    assert(toBoolean(1e-13) === false)
    assert(toBoolean(Double.NaN) === false)
  }

  test("addNaN") {
    assert(addNaN(1.0, 2.0) === 3.0)
    assert(addNaN(Double.NaN, 2.0) === 2.0)
    assert(addNaN(1.0, Double.NaN) === 1.0)
    assert(JDouble.isNaN(addNaN(Double.NaN, Double.NaN)))
  }

  test("subtractNaN") {
    assert(subtractNaN(1.0, 2.0) === -1.0)
    assert(subtractNaN(Double.NaN, 2.0) === -2.0)
    assert(subtractNaN(1.0, Double.NaN) === 1.0)
    assert(JDouble.isNaN(subtractNaN(Double.NaN, Double.NaN)))
  }

  test("maxNaN") {
    assert(maxNaN(1.0, 2.0) === 2.0)
    assert(maxNaN(2.0, 1.0) === 2.0)
    assert(maxNaN(Double.NaN, 2.0) === 2.0)
    assert(maxNaN(1.0, Double.NaN) === 1.0)
    assert(JDouble.isNaN(maxNaN(Double.NaN, Double.NaN)))
  }

  test("minNaN") {
    assert(minNaN(1.0, 2.0) === 1.0)
    assert(minNaN(2.0, 1.0) === 1.0)
    assert(minNaN(Double.NaN, 2.0) === 2.0)
    assert(minNaN(1.0, Double.NaN) === 1.0)
    assert(JDouble.isNaN(minNaN(Double.NaN, Double.NaN)))
  }

  test("gtNaN") {
    assert(gtNaN(1.0, 2.0) === 0.0)
    assert(gtNaN(2.0, 1.0) === 1.0)
    assert(gtNaN(Double.NaN, 2.0) === 0.0)
    assert(gtNaN(1.0, Double.NaN) === 1.0)
    assert(JDouble.isNaN(gtNaN(Double.NaN, Double.NaN)))
  }

  test("ltNaN") {
    assert(ltNaN(1.0, 2.0) === 1.0)
    assert(ltNaN(2.0, 1.0) === 0.0)
    assert(ltNaN(Double.NaN, 2.0) === 1.0)
    assert(ltNaN(1.0, Double.NaN) === 0.0)
    assert(JDouble.isNaN(ltNaN(Double.NaN, Double.NaN)))
  }

}
