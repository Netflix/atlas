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

import java.awt.image.RenderedImage
import java.io.OutputStream

import com.netflix.atlas.chart.model.GraphDef
import com.netflix.atlas.chart.util.PngImage

trait PngGraphEngine extends GraphEngine {

  val contentType: String = "image/png"

  def write(config: GraphDef, output: OutputStream): Unit = {
    val metadata = config.source.fold(Map.empty[String, String]) { s =>
      val desc = s"start=${config.startTime.toString}, end=${config.endTime.toString}"
      Map("Source" -> s, "Description" -> desc)
    }
    val image = PngImage(createImage(config), metadata)
    image.write(output)
  }

  def createImage(config: GraphDef): RenderedImage
}
