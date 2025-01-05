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

import java.io.InputStream

import com.netflix.atlas.core.util.Streams
import munit.FunSuite

class PngImageSuite extends FunSuite {

  // SBT working directory gets updated with fork to be the dir for the project
  private val baseDir = SrcPath.forProject("atlas-chart")
  private val goldenDir = s"$baseDir/src/test/resources/pngimage"
  private val targetDir = s"$baseDir/target/pngimage"

  private val graphAssertions =
    new GraphAssertions(goldenDir, targetDir, (a, b) => assertEquals(a, b))

  private val bless = false

  // From: http://en.wikipedia.org/wiki/Atlas_(mythology)
  private val sampleText = """
    |In Greek mythology, Atlas (English pronunciation: /ˈætləs/)
    | was the primordial Titan who supported the heavens. Although associated
    | with various places, he became commonly identified with the Atlas
    | Mountains in northwest Africa (Modern-day Morocco and Algeria).
    | The etymology of the name Atlas is uncertain and still debated. Virgil
    | took pleasure in translating etymologies of Greek names by combining them
    | with adjectives that explained them: for Atlas his adjective is durus,
    | "hard, enduring", which suggested to George Doig that Virgil was aware of
    | the Greek τλήναι "to endure"; Doig offers the further possibility that
    | Virgil was aware of Strabo's remark that the native North African name
    | for this mountain was Douris. Since the Atlas mountains rise in the
    | region inhabited by Berbers, it has been suggested that the name might be
    | taken from one of the Berber languages, specifically adrar, Berber for
    | "mountain".
  """.stripMargin

  def getInputStream(file: String): InputStream = {
    Streams.resource("pngimage/" + file)
  }

  def getImage(file: String): PngImage = {
    PngImage(getInputStream(file))
  }

  test("load image") {
    val image = getImage("test.png")
    assertEquals(image.metadata.size, 2)
    assertEquals(image.metadata("identical"), "false")
    assertEquals(image.metadata("diff-pixel-count"), "48302")
  }

  test("diff image, identical") {
    val i1 = PngImage.error("", 800, 100)
    val i2 = PngImage.error("", 800, 100)
    val diff = PngImage.diff(i1.data, i2.data)
    assertEquals(diff.metadata("identical"), "true")
    assertEquals(diff.metadata("diff-pixel-count"), "0")
  }

  test("diff image, with delta") {
    val i1 = PngImage.error("", 800, 100)
    val i2 = PngImage.error("", 801, 121)
    val diff = PngImage.diff(i1.data, i2.data)
    assertEquals(diff.metadata("identical"), "false")
    assertEquals(diff.metadata("diff-pixel-count"), "16921")
  }

  test("error image, 400x300") {
    val found = PngImage.error(sampleText, 400, 300)
    graphAssertions.assertEquals(found, "test_error_400x300.png", bless)
  }

  test("error image, 800x100") {
    val found = PngImage.error(sampleText, 800, 100)
    graphAssertions.assertEquals(found, "test_error_800x100.png", bless)
  }

  test("error diff image, 800x100") {
    val i1 = PngImage.error(sampleText, 800, 100)
    val i2 = PngImage.error(sampleText.toLowerCase, 800, 120)
    val diff = PngImage.diff(i1.data, i2.data)
    graphAssertions.assertEquals(diff, "test_diff.png", bless)
  }

  test("user error image matches expected") {
    val found = PngImage.userError("User Error Text", 800, 100)
    graphAssertions.assertEquals(found, "test_user_error.png", bless)
  }

  test("system error image matches expected") {
    val found = PngImage.systemError("System Error Text", 800, 100)
    graphAssertions.assertEquals(found, "test_system_error.png", bless)
  }

  test("user error image is different than system error image") {
    val userErrorImage = PngImage.userError("", 800, 100)
    val systemErrorImage = PngImage.systemError("", 800, 100)
    val diff = PngImage.diff(userErrorImage.data, systemErrorImage.data)

    assertEquals(diff.metadata("identical"), "false")
    assertEquals(diff.metadata("diff-pixel-count"), "80000")
  }

  test("image metadata") {
    val metadata = Map(
      "english"    -> "source url",
      "japanese"   -> "ソースURL",
      "compressed" -> (0 until 10000).mkString(",")
    )
    val img = PngImage.error("test", 100, 100).copy(metadata = metadata)

    val bytes = img.toByteArray
    val decoded = PngImage(bytes)

    assertEquals(metadata, decoded.metadata)
  }
}
