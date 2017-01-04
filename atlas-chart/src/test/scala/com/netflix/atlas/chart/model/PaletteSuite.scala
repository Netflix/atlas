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
package com.netflix.atlas.chart.model

import java.awt.Color

import org.scalatest.FunSuite


class PaletteSuite extends FunSuite {

  test("colors:") {
    intercept[IllegalArgumentException] {
      Palette.create("colors:")
    }
  }

  test("colors:foo") {
    intercept[IllegalArgumentException] {
      Palette.create("colors:foo")
    }
  }

  test("colors:f00") {
    val p = Palette.create("colors:f00")
    assert(p.colors(0) === Color.RED)
  }

  test("colors:f00,00ff00") {
    val p = Palette.create("colors:f00,00ff00")
    assert(p.colors(0) === Color.RED)
    assert(p.colors(1) === Color.GREEN)
  }

  test("colors:f00,00ff00,ff0000ff") {
    val p = Palette.create("colors:f00,00ff00,ff0000ff")
    assert(p.colors(0) === Color.RED)
    assert(p.colors(1) === Color.GREEN)
    assert(p.colors(2) === Color.BLUE)
  }

  test("armytage") {
    val p = Palette.create("armytage")
    assert(p.colors(0) === new Color(0, 117, 220))
  }

  test("foo") {
    intercept[IllegalArgumentException] {
      Palette.create("foo")
    }
  }
}
