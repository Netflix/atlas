/*
 * Copyright 2014-2019 Netflix, Inc.
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

import com.netflix.atlas.core.util.Streams

/**
  * Helper functions for working with fonts.
  */
object Fonts {
  private def loadTrueTypeFont(resource: String): Font = {
    Streams.scope(getClass.getClassLoader.getResourceAsStream(resource)) { in =>
      Font.createFont(Font.TRUETYPE_FONT, in).deriveFont(12.0f)
    }
  }

  /**
    * Load a font from the system or from the classpath.
    */
  def loadFont(font: String): Font = {
    if (font.endsWith(".ttf"))
      loadTrueTypeFont(font)
    else
      new Font(font, Font.PLAIN, 12)
  }

  /**
    * Font that is provided with the library and thus will be available on all systems. There
    * may be slight differences in the rendering on different versions of the JDK.
    */
  val default: Font = loadFont("fonts/RobotoMono-Regular.ttf")

  /**
    * Returns true if the JDK and OS being used match those used to generate the blessed
    * reference images for test cases. On other systems there will be slight differences in
    * the font rendering causing diffs.
    */
  def shouldRunTests: Boolean = {
    val isJdk11 = System.getProperty("java.specification.version") == "11"
    val isMacOS = System.getProperty("os.name") == "Mac OS X"
    isJdk11 && isMacOS
  }
}
