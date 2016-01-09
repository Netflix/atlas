/*
 * Copyright 2014-2016 Netflix, Inc.
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
package com.netflix.atlas.standalone

import java.awt.image.RenderedImage

import com.netflix.atlas.core.util.PngImage
import com.netflix.atlas.core.util.Streams._

/**
  * Command line tool for finding differences between two png images. The exit code will be 0
  * if there is no difference and 1 if there is a difference. An output image will be created
  * showing which pixels are different, black pixesls are the same for both, red pixels are
  * different.
  */
object ImageDiff {

  private def load(fname: String): RenderedImage = {
    PngImage(scope(fileIn(fname))(byteArray)).data
  }

  private def store(fname: String, image: PngImage): Unit = {
    scope(fileOut(fname))(image.write)
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      System.err.println(s"Usage: ImageDiff <image1> <image2> <diff-image>")
      sys.exit(2)
    }

    val image1 = load(args(0))
    val image2 = load(args(1))

    val diff = PngImage.diff(image1, image2)
    store(args(2), diff)
    sys.exit(if (diff.metadata("identical") == "true") 0 else 1)
  }
}
