/*
 * Copyright 2015 Netflix, Inc.
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
package com.netflix.atlas.chart

import java.awt.Color

object Palette {

  private val fname = "palettes/%s_palette.txt"

  private val defaultPalette = Array[Color](
    Color.RED,
    Color.GREEN,
    Color.BLUE,
    Color.MAGENTA,
    Color.YELLOW,
    Color.CYAN,
    Color.PINK,
    Color.ORANGE)

  // http://eleanormaclure.files.wordpress.com/2011/03/colour-coding.pdf
  private val palettes = {
    val names = List("armytage", "bw", "epic", "highcharts")
    val colors = names.map { n =>
      n -> Colors.load(fname.format(n)).toArray
    }
    colors.toMap.withDefaultValue(defaultPalette)
  }

  def apply(name: String): Palette = new Palette(palettes(name))
}

class Palette(colors: Array[Color]) {
  private var pos = -1
  def nextColor: Color = {
    pos += 1
    val index = pos % colors.length
    colors(index)
  }
}
