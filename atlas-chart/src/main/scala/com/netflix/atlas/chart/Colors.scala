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
import java.nio.charset.Charset

import com.google.common.io.Resources
import com.netflix.atlas.core.util.Strings


object Colors {
  /**
   * Load a list of colors from a resource file.
   */
  def load(name: String): List[Color] = {
    import scala.collection.JavaConversions._
    val url = Resources.getResource(name)
    val data = Resources.readLines(url, Charset.forName("UTF-8"))
    data.toList.map(Strings.parseColor)
  }
}
