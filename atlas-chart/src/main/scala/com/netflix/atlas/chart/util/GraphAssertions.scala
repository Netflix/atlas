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
package com.netflix.atlas.chart.util

import java.awt.image.RenderedImage
import java.io.File
import java.io.FileInputStream
import java.io.FileNotFoundException
import java.io.FileOutputStream
import java.io.InputStream

import com.netflix.atlas.core.util.Streams

import scala.util.Using

/**
  * Helper assertions for working with graphs.
  */
class GraphAssertions(goldenDir: String, targetDir: String, assert: (Any, Any) => Unit) {

  PngImage.useAntiAliasing = false

  private val absoluteGoldenDir = {
    val dir = new File(goldenDir)
    dir.getAbsolutePath
  }

  private def getInputStream(file: String): InputStream = {
    new FileInputStream(new File(s"$goldenDir/$file"))
  }

  private def getImage(file: String): PngImage = {
    PngImage(getInputStream(file))
  }

  private def getString(file: String): String = {
    Using.resource(getInputStream(file)) { in =>
      new String(Streams.byteArray(in), "UTF-8")
    }
  }

  def generateReport(clazz: Class[?], diffsOnly: Boolean = true): Unit = {
    val report = s"""<html>
      <head><title>${clazz.getSimpleName}</title></head>
      <body><h1>${clazz.getSimpleName}</h1><hr/> ${
        val dir = new File(targetDir)
        dir.mkdirs()
        dir.listFiles
          .filter(_.getName.endsWith(".png"))
          .filterNot(_.getName.startsWith("diff_"))
          .map { f =>
            val diffImg = new File(s"$targetDir/diff_${f.getName}")
            if (diffsOnly && !diffImg.isFile) ""
            else {
              s"""<div>
                <h2>${f.getName}</h2>
                <table border="1">
                  <tr><th>Golden</th><th>Test</th><th>Diff</th></tr>
                  <tr valign="top">
                    <td><img src="${absoluteGoldenDir + '/' + f.getName}"/></td>
                    <td><img src="${f.getName}"/></td>
                    ${
                  if (diffImg.isFile)
                    s"""<td><img src="${s"diff_${f.getName}"}"/></td>"""
                  else
                    """<td></td>"""
                }
                  </tr>
                </table>
              </div>"""
            }
          }
          .mkString("")
      } </body>
    </html>"""

    Using.resource(Streams.fileOut(new File(s"$targetDir/report.html"))) { out =>
      out.write(report.getBytes("UTF-8"))
    }
  }

  def assertEquals(v1: Double, v2: Double, delta: Double): Unit = {
    val diff = scala.math.abs(v1 - v2)
    if (diff > delta) {
      throw new AssertionError("%f != %f".format(v1, v2))
    }
  }

  def assertEquals(v1: Double, v2: Double, delta: Double, msg: String): Unit = {
    val diff = scala.math.abs(v1 - v2)
    if (diff > delta) {
      throw new AssertionError("%f != %f, %s".format(v1, v2, msg))
    }
  }

  def assertEquals(i1: RenderedImage, i2: RenderedImage): Unit = {
    val diff = PngImage.diff(i1, i2)
    assert(diff.metadata("identical"), "true")
  }

  def assertEquals(i1: PngImage, i2: PngImage): Unit = {
    assertEquals(i1.data, i2.data)
    assert(i1.metadata, i2.metadata)
  }

  def assertEquals(i1: PngImage, f: String, bless: Boolean = false): Unit = {
    // Skip on systems with incompatible font rendering
    if (!Fonts.shouldRunTests) return

    if (bless) blessImage(i1, f)
    val i2 =
      try getImage(f)
      catch {
        case e: FileNotFoundException => PngImage.error(e.getMessage, 400, 300)
      }
    val diff = PngImage.diff(i1.data, i2.data)
    writeImage(i1, targetDir, f)

    // For reporting we use the existence of the diff image to determine whether
    // to show an entry. Only create if there is a diff and remove old diffs if
    // it is now the same to avoid a false report based on an old diff image in
    // the workspace.
    if (diff.metadata("identical") != "true")
      writeImage(diff, targetDir, "diff_" + f)
    else
      new File(s"$targetDir/diff_$f").delete()
    assertEquals(i1, i2)
  }

  private def blessImage(img: PngImage, f: String): Unit = {
    writeImage(img, goldenDir, f)
  }

  private def writeImage(img: PngImage, dir: String, f: String): Unit = {
    val file = new File(new File(dir), f)
    file.getParentFile.mkdirs()
    val stream = new FileOutputStream(file)
    img.write(stream)
  }

  def assertEquals(s1: String, f: String, bless: Boolean): Unit = {
    if (bless) blessString(s1, f)
    val s2 = getString(f)
    assert(s1, s2)
  }

  private def blessString(s: String, f: String): Unit = {
    writeString(s, goldenDir, f)
  }

  private def writeString(s: String, dir: String, f: String): Unit = {
    val file = new File(new File(dir), f)
    file.getParentFile.mkdirs()
    val stream = new FileOutputStream(file)
    try stream.write(s.getBytes("UTF-8"))
    finally stream.close()
  }
}
