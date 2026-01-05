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
package com.netflix.atlas.chart.util

import java.awt.Font
import java.awt.font.TextAttribute

import munit.FunSuite

class FontSetSuite extends FunSuite {

  private val monoFont = Fonts.loadFont("fonts/NotoSansMono-Regular.ttf")
  private val symbolsFont = Fonts.loadFont("fonts/NotoSansSymbols-Regular.ttf")
  private val emojiFont = Fonts.loadFont("fonts/NotoEmoji-Regular.ttf")

  private val fontSet = new FontSet(List(monoFont, symbolsFont, emojiFont))

  test("require non-empty font list") {
    intercept[IllegalArgumentException] {
      new FontSet(List.empty)
    }
  }

  test("primaryFont returns first font") {
    assertEquals(fontSet.primaryFont, monoFont)
  }

  test("createAttributedString with empty string") {
    val attrStr = fontSet.createAttributedString("")
    assertEquals(attrStr.getIterator.getEndIndex, 0)
  }

  test("createAttributedString with simple ASCII text") {
    val text = "Hello World"
    val attrStr = fontSet.createAttributedString(text)

    val iter = attrStr.getIterator
    assertEquals(iter.getEndIndex, text.length)

    // All ASCII characters should use the mono font
    val font = iter.getAttribute(TextAttribute.FONT).asInstanceOf[Font]
    assertEquals(font, monoFont)
  }

  test("createAttributedString with mixed text and emoji") {
    val text = "Hello 😀 World"
    val attrStr = fontSet.createAttributedString(text)

    val iter = attrStr.getIterator
    assertEquals(iter.getEndIndex, text.length)

    // First part should use mono font
    iter.setIndex(0)
    val font1 = iter.getAttribute(TextAttribute.FONT).asInstanceOf[Font]
    assertEquals(font1, monoFont)

    // Emoji should use emoji font
    iter.setIndex(6) // Position of emoji
    val font2 = iter.getAttribute(TextAttribute.FONT).asInstanceOf[Font]
    assertEquals(font2, emojiFont)

    // Text after emoji should use mono font again
    iter.setIndex(9) // Position after emoji (emoji is 2 chars)
    val font3 = iter.getAttribute(TextAttribute.FONT).asInstanceOf[Font]
    assertEquals(font3, monoFont)
  }

  test("createAttributedString with symbols") {
    // Test with a mathematical symbol that should be in NotoSansSymbols
    val text = "x ± y"
    val attrStr = fontSet.createAttributedString(text)

    val iter = attrStr.getIterator
    assertEquals(iter.getEndIndex, text.length)

    // Regular characters should use mono font
    iter.setIndex(0)
    val font1 = iter.getAttribute(TextAttribute.FONT).asInstanceOf[Font]
    assertEquals(font1, monoFont)

    // The ± symbol might be in symbols font (depends on font coverage)
    iter.setIndex(2)
    val font2 = iter.getAttribute(TextAttribute.FONT).asInstanceOf[Font]
    assert(font2 == monoFont || font2 == symbolsFont)
  }

  test("createAttributedString with multiple emojis") {
    val text = "😀😁😂"
    val attrStr = fontSet.createAttributedString(text)

    val iter = attrStr.getIterator
    // Each emoji is 2 chars (surrogate pair)
    assertEquals(iter.getEndIndex, 6)

    // All emojis should use emoji font and be batched together
    iter.setIndex(0)
    val font = iter.getAttribute(TextAttribute.FONT).asInstanceOf[Font]
    assertEquals(font, emojiFont)
  }

  test("createAttributedString handles code points correctly") {
    // Test with a character outside BMP (requires surrogate pair)
    val text = "A\uD83D\uDE00B" // A + 😀 + B
    val attrStr = fontSet.createAttributedString(text)

    val iter = attrStr.getIterator
    assertEquals(iter.getEndIndex, 4) // A(1) + emoji(2) + B(1)

    // First char should use mono font
    iter.setIndex(0)
    assertEquals(iter.getAttribute(TextAttribute.FONT), monoFont)

    // Emoji should use emoji font
    iter.setIndex(1)
    assertEquals(iter.getAttribute(TextAttribute.FONT), emojiFont)

    // Last char should use mono font
    iter.setIndex(3)
    assertEquals(iter.getAttribute(TextAttribute.FONT), monoFont)
  }

  test("createAttributedString batches consecutive characters with same font") {
    val text = "Hello"
    val attrStr = fontSet.createAttributedString(text)

    val iter = attrStr.getIterator

    // All characters should use the same font and be in one batch
    iter.setIndex(0)
    val font1 = iter.getAttribute(TextAttribute.FONT)

    iter.setIndex(4)
    val font2 = iter.getAttribute(TextAttribute.FONT)

    assertEquals(font1, font2)
    assertEquals(font1, monoFont)
  }
}
