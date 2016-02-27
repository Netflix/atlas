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
package com.netflix.atlas.core.util

import org.scalatest.FunSuite


class UnitPrefixSuite extends FunSuite {

  test("decimal isNearlyZero") {
    assert(UnitPrefix.decimal(1e-13).text === "")
  }

  test("decimal infinity") {
    assert(UnitPrefix.decimal(Double.PositiveInfinity).text === "")
  }

  test("decimal NaN") {
    assert(UnitPrefix.decimal(Double.NaN).text === "")
  }

  test("decimal milli") {
    assert(UnitPrefix.decimal(1.23e-3).text === "milli")
    assert(UnitPrefix.decimal(-1.23e-3).text === "milli")
  }

  test("decimal kilo") {
    assert(UnitPrefix.decimal(1.23e3).text === "kilo")
    assert(UnitPrefix.decimal(-1.23e3).text === "kilo")
  }

  test("decimal mega") {
    assert(UnitPrefix.decimal(1.23e6).text === "mega")
    assert(UnitPrefix.decimal(-1.23e6).text === "mega")
  }

  test("decimal giga") {
    assert(UnitPrefix.decimal(1.23e9).text === "giga")
    assert(UnitPrefix.decimal(-1.23e9).text === "giga")
  }

  test("binary isNearlyZero") {
    assert(UnitPrefix.binary(1e-13).text === "")
  }

  test("binary infinity") {
    assert(UnitPrefix.binary(Double.PositiveInfinity).text === "")
  }

  test("binary NaN") {
    assert(UnitPrefix.binary(Double.NaN).text === "")
  }

  test("binary milli") {
    assert(UnitPrefix.binary(1.23e-3).text === "milli")
    assert(UnitPrefix.binary(-1.23e-3).text === "milli")
  }

  test("binary kibi") {
    assert(UnitPrefix.binary(1023.0).text === "")
    assert(UnitPrefix.binary(1.23e3).text === "kibi")
    assert(UnitPrefix.binary(-1.23e3).text === "kibi")
  }

  test("binary mebi") {
    assert(UnitPrefix.binary(1.23e6).text === "mebi")
    assert(UnitPrefix.binary(-1.23e6).text === "mebi")
  }

  test("binary gibi") {
    assert(UnitPrefix.binary(1.23e9).text === "gibi")
    assert(UnitPrefix.binary(-1.23e9).text === "gibi")
  }

  test("format MaxValue") {
    assert(UnitPrefix.format(Double.MaxValue) === " 2e+308")
  }

  test("format MinValue") {
    assert(UnitPrefix.format(Double.MinValue) === "-2e+308")
  }

}
