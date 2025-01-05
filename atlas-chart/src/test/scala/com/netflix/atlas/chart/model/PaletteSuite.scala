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
package com.netflix.atlas.chart.model

import java.awt.Color
import munit.FunSuite

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
    assertEquals(p.colors(0), Color.RED)
    assertEquals(1, p.colorArray.size)
  }

  test("colors:f00,00ff00") {
    val p = Palette.create("colors:f00,00ff00")
    assertEquals(p.colors(0), Color.RED)
    assertEquals(p.colors(1), Color.GREEN)
    assertEquals(2, p.colorArray.size)
  }

  test("colors:f00,00ff00,ff0000ff") {
    val p = Palette.create("colors:f00,00ff00,ff0000ff")
    assertEquals(p.colors(0), Color.RED)
    assertEquals(p.colors(1), Color.GREEN)
    assertEquals(p.colors(2), Color.BLUE)
    assertEquals(3, p.colorArray.size)
  }

  test("(,)") {
    intercept[IllegalArgumentException] {
      Palette.create("(,)")
    }
  }

  test("(,foo,)") {
    intercept[IllegalArgumentException] {
      Palette.create("(,foo,)")
    }
  }

  test("(,f00,)") {
    val p = Palette.create("(,f00,)")
    assertEquals(p.colors(0), Color.RED)
    assertEquals(1, p.colorArray.size)
  }

  test("(,f00,00ff00,)") {
    val p = Palette.create("(,f00,00ff00,)")
    assertEquals(p.colors(0), Color.RED)
    assertEquals(p.colors(1), Color.GREEN)
    assertEquals(2, p.colorArray.size)
  }

  test("(,f00,00ff00,ff0000ff,)") {
    val p = Palette.create("(,f00,00ff00,ff0000ff,)")
    assertEquals(p.colors(0), Color.RED)
    assertEquals(p.colors(1), Color.GREEN)
    assertEquals(p.colors(2), Color.BLUE)
    assertEquals(3, p.colorArray.size)
  }

  test("armytage") {
    val p = Palette.create("armytage")
    assertEquals(p.colors(0), new Color(0, 117, 220))
    assertEquals(24, p.colorArray.size)
  }

  test("foo") {
    intercept[IllegalArgumentException] {
      Palette.create("foo")
    }
  }
}
