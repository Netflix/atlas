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
package com.netflix.atlas.chart.graphics

import java.awt.Graphics2D

/**
  * Base type for elements to draw as part of the chart.
  */
trait Element {

  def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit

  /** Compute the width for the element if restricted to the specified height. */
  def getWidth(g: Graphics2D, height: Int): Int = {
    this match {
      case h: FixedWidth    => h.width
      case h: VariableWidth => h.computeWidth(g, height)
      case _                => 0
    }
  }

  /** Compute the height for the element if restricted to the specified width. */
  def getHeight(g: Graphics2D, width: Int): Int = {
    this match {
      case h: FixedHeight    => h.height
      case h: VariableHeight => h.computeHeight(g, width)
      case _                 => 0
    }
  }
}

/** Indicates the element has a fixed width. */
trait FixedWidth {

  def width: Int
}

/** Indicates the element has a fixed height. */
trait FixedHeight {

  def height: Int
}

/**
  * Indicates that the height of the element is variable depending on the width. The most common
  * example is text that can wrap to fit the width available.
  */
trait VariableWidth {

  def minWidth: Int

  def computeWidth(g: Graphics2D, width: Int): Int
}

/**
  * Indicates that the height of the element is variable depending on the width. The most common
  * example is text that can wrap to fit the width available.
  */
trait VariableHeight {

  def minHeight: Int

  def computeHeight(g: Graphics2D, width: Int): Int
}
