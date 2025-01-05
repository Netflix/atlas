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
package com.netflix.atlas.core.util

import munit.FunSuite

import scala.util.Using

class CardinalityEstimatorSuite extends FunSuite {

  private def check(estimator: CardinalityEstimator, values: Seq[String]): Unit = {
    var errorSum = 0.0
    values.zipWithIndex.foreach {
      case (v, i) =>
        estimator.update(v)
        val actual = i + 1
        val estimate = estimator.cardinality
        val percentError = 100.0 * math.abs(estimate - actual) / actual
        errorSum += percentError
    }
    val avgPctError = errorSum / values.size

    assert(avgPctError < 5, "error should be less than 5%")
  }

  test("estimate sha1 string") {
    val values = Using.resource(Streams.resource("cardinalityEstimator.txt")) { in =>
      Streams.lines(in).toList
    }

    check(CardinalityEstimator.newEstimator(), values.slice(0, 1))
    check(CardinalityEstimator.newEstimator(), values.slice(0, 10))
    check(CardinalityEstimator.newEstimator(), values.slice(0, 100))
    check(CardinalityEstimator.newEstimator(), values.slice(0, 200))
  }

  test("estimate int string") {
    // verify reasonably accurate estimate with strings that are fairly similar
    val values = (0 until 1000).map(_.toString)

    check(CardinalityEstimator.newEstimator(), values.slice(0, 1))
    check(CardinalityEstimator.newEstimator(), values.slice(0, 10))
    check(CardinalityEstimator.newEstimator(), values.slice(0, 100))
    check(CardinalityEstimator.newEstimator(), values.slice(0, 200))
  }

}
