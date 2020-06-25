/*
 * Copyright 2014-2020 Netflix, Inc.
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

import org.scalatest.funsuite.AnyFunSuite

class StepSuite extends AnyFunSuite {

  private def days(n: Long): Long = n * 24 * 60 * 60 * 1000

  test("round: allow arbitrary number of days") {
    (1 until 500).foreach { i =>
      assert(days(i) === Step.round(60000, days(i)))
    }
  }

  test("compute: allow arbitrary number of days") {
    (1 until 500).foreach { i =>
      assert(days(i) === Step.compute(60000, 1, 0L, days(i)))
    }
  }
}
