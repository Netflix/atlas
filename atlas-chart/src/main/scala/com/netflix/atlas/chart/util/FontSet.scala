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
import java.text.AttributedString

/**
  * A set of fonts to use for rendering text with characters that may not be supported
  * by a single font. For each character, the first font in the list that can display it
  * will be used.
  *
  * @param fonts
  *     List of fonts to use, in priority order. Must be non-empty. The first font is
  *     considered the primary font and will be used as a fallback if no font in the
  *     set can display a particular character.
  */
class FontSet(fonts: List[Font]) {

  require(fonts.nonEmpty, "font set cannot be empty")

  /**
    * Returns the primary (first) font in the set.
    */
  def primaryFont: Font = fonts.head

  /**
    * Creates an AttributedString from the input string, selecting fonts from the set
    * for each character. Characters that can be displayed by the same font are grouped
    * into batches for efficient attribute application.
    *
    * The implementation iterates by Unicode code points rather than characters to
    * properly handle characters outside the Basic Multilingual Plane (e.g., emoji,
    * special symbols).
    *
    * @param str
    *     The input string to convert. May be empty.
    * @return
    *     An AttributedString with appropriate font attributes applied.
    */
  def createAttributedString(str: String): AttributedString = {
    if (str.isEmpty) {
      return new AttributedString("")
    }

    val attrStr = new AttributedString(str)

    // Find the appropriate font for each code point and group into batches
    var batchStart = 0
    var currentFont = findFontFor(str.codePointAt(0))

    var i = Character.charCount(str.codePointAt(0))
    while (i < str.length) {
      val codePoint = str.codePointAt(i)
      val font = findFontFor(codePoint)

      // If font changes, apply the current font to the batch and start a new one
      if (font != currentFont) {
        attrStr.addAttribute(java.awt.font.TextAttribute.FONT, currentFont, batchStart, i)
        batchStart = i
        currentFont = font
      }
      i += Character.charCount(codePoint)
    }

    // Apply font to the final batch
    attrStr.addAttribute(java.awt.font.TextAttribute.FONT, currentFont, batchStart, str.length)

    attrStr
  }

  /**
    * Finds the first font in the set that can display the given code point.
    *
    * @param codePoint
    *     The Unicode code point to find a font for.
    * @return
    *     The first font that can display the code point, or the primary font if none can.
    */
  private def findFontFor(codePoint: Int): Font = {
    fonts.find(_.canDisplay(codePoint)).getOrElse(fonts.head)
  }

  /**
    * Returns a new font set adjusted to the specified size.
    */
  def withSize(size: Float): FontSet = {
    new FontSet(fonts.map(_.deriveFont(size)))
  }

  /**
    * Returns a new font set adjusted to the specified style.
    */
  def withStyle(style: Int): FontSet = {
    new FontSet(fonts.map(_.deriveFont(style)))
  }
}
