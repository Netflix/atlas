/*
 * Copyright 2014-2026 Netflix, Inc.
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
package com.netflix.atlas.chart.graphics

import java.awt.image.BufferedImage

import munit.FunSuite

class TextSuite extends FunSuite {

  private def createGraphics(): java.awt.Graphics2D = {
    val image = new BufferedImage(400, 300, BufferedImage.TYPE_INT_ARGB)
    image.createGraphics()
  }

  test("draw with empty string") {
    val text = Text("")
    val g = createGraphics()
    // This should not throw IllegalArgumentException
    text.draw(g, 0, 0, 100, 20)
    g.dispose()
  }

  test("draw with normal text") {
    val text = Text("Hello World")
    val g = createGraphics()
    text.draw(g, 0, 0, 100, 20)
    g.dispose()
  }

  test("truncate to empty string when width is too small") {
    val text = Text("Some long text")
    // Very small width should result in empty string (maxChars < 5)
    val truncated = text.truncate(10)
    assertEquals(truncated.str, "")

    // Drawing the truncated empty text should not throw
    val g = createGraphics()
    truncated.draw(g, 0, 0, 100, 20)
    g.dispose()
  }

  test("truncate with ellipsis when width allows") {
    val text = Text("Some long text that needs truncation")
    // Width that allows more than 5 chars should add ellipsis
    val truncated = text.truncate(100)
    assert(truncated.str.endsWith("..."))
  }

  test("truncate keeps original when text fits") {
    val text = Text("Short")
    val truncated = text.truncate(1000)
    assertEquals(truncated.str, "Short")
  }

  test("computeHeight with empty string") {
    val text = Text("")
    val g = createGraphics()
    val height = text.computeHeight(g, 100)
    // Should return a valid height without throwing
    assert(height >= 0)
    g.dispose()
  }

  test("computeHeight with normal text") {
    val text = Text("Hello World")
    val g = createGraphics()
    val height = text.computeHeight(g, 100)
    assert(height > 0)
    g.dispose()
  }

  test("minHeight is positive") {
    val text = Text("test")
    assert(text.minHeight > 0)
  }

  test("minHeight with empty string is positive") {
    val text = Text("")
    assert(text.minHeight > 0)
  }
}
