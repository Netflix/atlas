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

class UnitPrefixSuite extends FunSuite {

  test("decimal isNearlyZero") {
    assertEquals(UnitPrefix.decimal(1e-13).text, "")
  }

  test("decimal infinity") {
    assertEquals(UnitPrefix.decimal(Double.PositiveInfinity).text, "")
  }

  test("decimal NaN") {
    assertEquals(UnitPrefix.decimal(Double.NaN).text, "")
  }

  test("decimal milli") {
    assertEquals(UnitPrefix.decimal(1.23e-3).text, "milli")
    assertEquals(UnitPrefix.decimal(-1.23e-3).text, "milli")
  }

  test("decimal kilo") {
    assertEquals(UnitPrefix.decimal(1.23e3).text, "kilo")
    assertEquals(UnitPrefix.decimal(-1.23e3).text, "kilo")
  }

  test("decimal mega") {
    assertEquals(UnitPrefix.decimal(1.23e6).text, "mega")
    assertEquals(UnitPrefix.decimal(-1.23e6).text, "mega")
  }

  test("decimal giga") {
    assertEquals(UnitPrefix.decimal(1.23e9).text, "giga")
    assertEquals(UnitPrefix.decimal(-1.23e9).text, "giga")
  }

  test("binary isNearlyZero") {
    assertEquals(UnitPrefix.binary(1e-13).text, "")
  }

  test("binary infinity") {
    assertEquals(UnitPrefix.binary(Double.PositiveInfinity).text, "")
  }

  test("binary NaN") {
    assertEquals(UnitPrefix.binary(Double.NaN).text, "")
  }

  test("binary milli") {
    assertEquals(UnitPrefix.binary(1.23e-3).text, "milli")
    assertEquals(UnitPrefix.binary(-1.23e-3).text, "milli")
  }

  test("binary kibi") {
    assertEquals(UnitPrefix.binary(1023.0).text, "")
    assertEquals(UnitPrefix.binary(1.23e3).text, "kibi")
    assertEquals(UnitPrefix.binary(-1.23e3).text, "kibi")
  }

  test("binary mebi") {
    assertEquals(UnitPrefix.binary(1.23e6).text, "mebi")
    assertEquals(UnitPrefix.binary(-1.23e6).text, "mebi")
  }

  test("binary gibi") {
    assertEquals(UnitPrefix.binary(1.23e9).text, "gibi")
    assertEquals(UnitPrefix.binary(-1.23e9).text, "gibi")
  }

  test("format MaxValue") {
    assertEquals(UnitPrefix.format(Double.MaxValue), " 2e+308")
  }

  test("format MinValue") {
    assertEquals(UnitPrefix.format(Double.MinValue), "-2e+308")
  }

}
