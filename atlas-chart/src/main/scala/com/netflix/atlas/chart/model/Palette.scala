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
import java.io.FileNotFoundException
import java.util.concurrent.ConcurrentHashMap

import com.netflix.atlas.chart.Colors
import com.netflix.atlas.core.util.Strings

case class Palette(name: String, colors: Int => Color) {
  def withAlpha(alpha: Int): Palette = {
    def f(i: Int): Color = {
      val c = colors(i)
      new Color(c.getRed, c.getGreen, c.getBlue, alpha)
    }
    Palette(name, f)
  }

  def withVisionType(vision: VisionType): Palette = {
    def f(i: Int): Color = {
      val c = colors(i)
      vision.convert(c)
    }
    Palette(s"${vision.name}_$name", f)
  }

  /**
   * Convert colors from another palette into grayscale. For information about the conversion
   * see: http://www.johndcook.com/blog/2009/08/24/algorithms-convert-color-grayscale/.
   */
  def asGrayscale: Palette = {
    def f(i: Int): Color = {
      val c = colors(i)
      val v = (0.21 * c.getRed + 0.72 * c.getGreen + 0.07 * c.getBlue).toInt
      new Color(v, v, v, c.getAlpha)
    }
    Palette(s"grayscale_$name", f)
  }

  def iterator: Iterator[Color] = new Iterator[Color] {
    private var pos = -1
    override def hasNext: Boolean = true
    override def next(): Color = {
      pos += 1
      colors(pos)
    }
  }
}

object Palette {

  val default = fromArray("default", Array[Color](
    Color.RED,
    Color.GREEN,
    Color.BLUE,
    Color.MAGENTA,
    Color.YELLOW,
    Color.CYAN,
    Color.PINK,
    Color.ORANGE))

  private val palettes = new ConcurrentHashMap[String, Palette]

  /**
    * Creates a palette instance from a description string. The description can be an explicit
    * list of colors or the name of a palette file in the classpath. An explicit list is specified
    * with a prefix of 'colors:' followed by a comma separated list of color values. For example:
    *
    * ```
    * colors:f00,00ff00,000000ff
    * ```
    *
    * The color values will be parsed using `Strings.parseColor`.
    * Otherwise the description will be used to find a palette file in the classpath named
    * `palettes/{desc}_palette.txt` that has one color per line.
    */
  def create(desc: String): Palette = {
    if (desc.startsWith("colors:"))
      fromArray("colors", desc.substring("colors:".length).split(",").map(Strings.parseColor))
    else
      fromResource(desc)
  }

  def fromArray(name: String, colors: Array[Color]): Palette = {
    Palette(name, i => colors(math.abs(i) % colors.length))
  }

  /**
    * Create a palette from a file in the classpath named `palettes/{name}_palette.txt`. The
    * file should have one color per line in a format supported by `Strings.parseColor`.
    */
  def fromResource(name: String): Palette = {
    palettes.computeIfAbsent(name, loadFromResource)
  }

  private def loadFromResource(name: String): Palette = {
    try {
      val colors = Colors.load(s"palettes/${name}_palette.txt").toArray
      Palette.fromArray(name, colors)
    } catch {
      case _: FileNotFoundException =>
        throw new IllegalArgumentException(s"invalid palette name: '$name'")
    }
  }

  def singleColor(c: Color): Palette = {
    Palette("%08X".format(c.getRGB), _ => c)
  }

  def brighter(c: Color, n: Int): Palette = {
    val colors = new Array[Color](n)
    colors(0) = c
    (1 until n).foreach { i => colors(i) = colors(i - 1).brighter() }
    Palette.fromArray("brighter_%08X".format(c.getRGB), colors)
  }

  def darker(c: Color, n: Int): Palette = {
    val colors = new Array[Color](n)
    colors(0) = c
    (1 until n).foreach { i => colors(i) = colors(i - 1).darker() }
    Palette.fromArray("darker_%08X".format(c.getRGB), colors)
  }

  def main(args: Array[String]): Unit = {
    val p = fromResource("armytage").asGrayscale
    (0 until 26).foreach { i => println("%08X".format(p.colors(i).getRGB)) }
  }
}
