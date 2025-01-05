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
import java.io.FileNotFoundException
import java.util.concurrent.ConcurrentHashMap
import com.netflix.atlas.chart.Colors
import com.netflix.atlas.core.util.Strings

import scala.collection.immutable.ArraySeq

case class Palette(name: String, colorArray: ArraySeq[Color]) {

  def withAlpha(alpha: Int): Palette =
    Palette(name, colorArray.map(c => new Color(c.getRed, c.getGreen, c.getBlue, alpha)))

  def withVisionType(vision: VisionType): Palette =
    Palette(s"${vision.name}_$name", colorArray.map(vision.convert))

  /**
    * Convert colors from another palette into grayscale. For information about the conversion
    * see: http://www.johndcook.com/blog/2009/08/24/algorithms-convert-color-grayscale/.
    */
  def asGrayscale: Palette =
    Palette(
      s"grayscale_$name",
      colorArray.map { c =>
        val v = (0.21 * c.getRed + 0.72 * c.getGreen + 0.07 * c.getBlue).toInt
        new Color(v, v, v, c.getAlpha)
      }
    )

  def iterator: Iterator[Color] = new Iterator[Color] {

    private var pos = -1

    override def hasNext: Boolean = true

    override def next(): Color = {
      pos += 1
      if (pos >= colorArray.length) pos = 0
      colorArray(pos)
    }
  }

  /**
    * Rotates through the colors in the palette based on the index, returning a
    * deterministic color.
    *
    * @param i
    *   A positive integer value.
    * @return
    *   A deterministic color in the palette.
    */
  def colors(i: Int): Color = {
    val index = math.abs(i) % colorArray.length
    colorArray(index)
  }
}

object Palette {

  val default: Palette = fromArray(
    "default",
    Array[Color](
      Color.RED,
      Color.GREEN,
      Color.BLUE,
      Color.MAGENTA,
      Color.YELLOW,
      Color.CYAN,
      Color.PINK,
      Color.ORANGE
    )
  )

  private val palettes = new ConcurrentHashMap[String, Palette]

  /**
    * Creates a palette instance from a description string. The description can be an explicit
    * list of colors or the name of a palette file in the classpath. An explicit list is specified
    * as an ASL list of colors. For example:
    *
    * ```
    * (,f00,00ff00,000000ff,)
    * ```
    *
    * The color values will be parsed using `Strings.parseColor`.
    * Otherwise the description will be used to find a palette file in the classpath named
    * `palettes/{desc}_palette.txt` that has one color per line.
    */
  def create(desc: String): Palette = {
    // `colors:` prefix is deprecated, use list variant that is consistent between
    // the url parameter and expression
    if (desc.startsWith("colors:"))
      fromArray("colors", parseColors(desc.substring("colors:".length)))
    else if (desc.startsWith("("))
      fromArray("colors", parseColors(desc))
    else
      fromResource(desc)
  }

  private def parseColors(colorsString: String): Array[Color] = {
    colorsString
      .split(",")
      .map(_.trim)
      .filterNot(s => s.isEmpty || s == "(" || s == ")")
      .map(Strings.parseColor)
  }

  def fromArray(name: String, colors: Array[Color]): Palette = {
    require(colors.nonEmpty, "palette must contain at least one color")
    Palette(name, ArraySeq.from(colors))
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
    Palette("%08X".format(c.getRGB), ArraySeq(c))
  }

  def gradient(color: Color): Palette = {
    var alpha = color.getAlpha
    val delta = if (alpha == 0 || alpha >= 255) 255 / 5 else alpha
    val colors = new Array[Color](255 / delta)
    alpha = 255
    var i = colors.length - 1
    while (i >= 0) {
      colors(i) = new Color(color.getRed, color.getGreen, color.getBlue, alpha)
      alpha -= delta
      i -= 1
    }
    Palette("gradient", ArraySeq.unsafeWrapArray(colors))
  }

  def brighter(c: Color, n: Int): Palette = {
    val colors = new Array[Color](n)
    colors(0) = c
    (1 until n).foreach { i =>
      colors(i) = colors(i - 1).brighter()
    }
    Palette.fromArray("brighter_%08X".format(c.getRGB), colors)
  }

  def darker(c: Color, n: Int): Palette = {
    val colors = new Array[Color](n)
    colors(0) = c
    (1 until n).foreach { i =>
      colors(i) = colors(i - 1).darker()
    }
    Palette.fromArray("darker_%08X".format(c.getRGB), colors)
  }

  def main(args: Array[String]): Unit = {
    val p = fromResource("armytage").asGrayscale
    (0 until 26).foreach { i =>
      println("%08X".format(p.colors(i).getRGB))
    }
  }
}
