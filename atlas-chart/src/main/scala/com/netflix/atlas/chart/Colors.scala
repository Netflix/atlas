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
package com.netflix.atlas.chart

import java.awt.Color

import com.netflix.atlas.core.util.Streams
import com.netflix.atlas.core.util.Strings

import scala.util.Using

object Colors {

  def withAlpha(c: Color, alpha: Int): Color = {
    new Color(c.getRed, c.getGreen, c.getBlue, alpha)
  }

  /**
    * Load a list of colors from a resource file.
    */
  def load(name: String): List[Color] = {
    Using.resource(Streams.resource(name)) { in =>
      Streams
        .lines(in)
        .filterNot(s => s.isEmpty || s.startsWith("#"))
        .map(Strings.parseColor)
        .toList
    }
  }
}
