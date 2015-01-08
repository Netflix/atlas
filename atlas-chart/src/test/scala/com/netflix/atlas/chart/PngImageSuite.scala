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

import java.io.InputStream

import com.google.common.io.Resources
import org.scalatest.FunSuite


class PngImageSuite extends FunSuite {

  // From: http://en.wikipedia.org/wiki/Atlas_(mythology)
  val sampleText = """
    |In Greek mythology, Atlas (English pronunciation: /ˈætləs/, Greek. Ἄτλας)
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
    Resources.getResource("pngimage/" + file).openStream()
  }

  def getImage(file: String): PngImage = {
    PngImage(getInputStream(file))
  }

  test("load image") {
    val image = getImage("test.png")
    assert(image.metadata.size === 2)
    assert(image.metadata("identical") === "false")
    assert(image.metadata("diff-pixel-count") === "48302")
  }

  test("diff image, identical") {
    val i1 = PngImage.error("", 800, 100)
    val i2 = PngImage.error("", 800, 100)
    val diff = PngImage.diff(i1.data, i2.data)
    assert(diff.metadata("identical") === "true")
    assert(diff.metadata("diff-pixel-count") === "0")
  }

  test("diff image, with delta") {
    val i1 = PngImage.error("", 800, 100)
    val i2 = PngImage.error("", 801, 121)
    val diff = PngImage.diff(i1.data, i2.data)
    assert(diff.metadata("identical") === "false")
    assert(diff.metadata("diff-pixel-count") === "16921")
  }

  test("error image, 400x300") {
    val expected = getImage("test_error_400x300.png")
    val found = PngImage.error(sampleText, 400, 300)
    val diff = PngImage.diff(expected.data, found.data)
    assert(diff.metadata("identical") === "true")
  }

  test("error image, 800x100") {
    val expected = getImage("test_error_800x100.png")
    val found = PngImage.error(sampleText, 800, 100)
    val diff = PngImage.diff(expected.data, found.data)
    assert(diff.metadata("identical") === "true")
  }

  test("error diff image, 800x100") {
    val expected = getImage("test_diff.png")
    val i1 = PngImage.error(sampleText, 800, 100)
    val i2 = PngImage.error(sampleText.toLowerCase, 800, 120)
    val diff = PngImage.diff(i1.data, i2.data)
    assert(diff.metadata("identical") === "false")
    assert(diff.metadata("diff-pixel-count") === "32529")

    val diff2 = PngImage.diff(expected.data, diff.data)
    assert(diff2.metadata("identical") === "true")
    assert(diff2.metadata("diff-pixel-count") === "0")
  }
}
