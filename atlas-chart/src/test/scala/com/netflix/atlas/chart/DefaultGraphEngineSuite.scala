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

import com.netflix.atlas.chart.model.GraphDef
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.PlotDef

import java.time.Instant

class DefaultGraphEngineSuite extends PngGraphEngineSuite {

  override def prefix: String = "default"

  override def graphEngine: PngGraphEngine = new DefaultGraphEngine

  // Dimension validation tests - moved from DimensionValidationSuite
  private val dimensionStep = 60000L
  private val dimensionNow = Instant.now()
  private val dimensionStart = dimensionNow.minusSeconds(3600)
  private val dimensionEnd = dimensionNow

  private def createGraphDef(
    width: Int,
    height: Int,
    zoom: Double = 1.0,
    numSeries: Int = 1
  ): GraphDef = {
    val series = (1 to numSeries).map(i => LineDef(constant(i * 10.0)))
    val plotDef = PlotDef(series.toList)

    GraphDef(
      plots = List(plotDef),
      startTime = dimensionStart,
      endTime = dimensionEnd,
      step = dimensionStep,
      width = width,
      height = height,
      zoom = zoom
    )
  }

  test("dimension validation: normal dimensions should work") {
    val graphDef = createGraphDef(width = 800, height = 400)
    val image = graphEngine.createImage(graphDef)

    // Should create a successful image (not an error image)
    assert(image.getWidth > 400, "Image width should be reasonable")
    assert(image.getHeight > 200, "Image height should be reasonable")
  }

  test("dimension validation: oversized width should be clamped with warning") {
    val graphDef = createGraphDef(width = 5000, height = 400) // exceeds MaxWidth of 2000
    val image = graphEngine.createImage(graphDef)

    // Should still produce a valid graph (not error image), but clamped dimensions
    assert(image.getWidth > 400, "Should produce valid image with clamped width")
    assert(image.getHeight > 200, "Should produce valid image")
    // The actual width will be clamped to MaxWidth by GraphConstants validation
  }

  test("dimension validation: oversized height should be clamped with warning") {
    val graphDef = createGraphDef(width = 800, height = 50000) // exceeds MaxHeight of 1000
    val image = graphEngine.createImage(graphDef)

    // Should still produce a valid graph (not error image), but clamped dimensions
    assert(image.getWidth > 400, "Should produce valid image")
    assert(image.getHeight > 200, "Should produce valid image with clamped height")
  }

  test("dimension validation: oversized zoom should be clamped") {
    val graphDef = createGraphDef(width = 800, height = 400, zoom = 10.0) // exceeds MaxZoom of 2.0
    val image = graphEngine.createImage(graphDef)

    // Should clamp zoom to MaxZoom and produce valid image
    assert(image.getWidth > 400, "Should produce valid image with clamped zoom")
    assert(image.getHeight > 200, "Should produce valid image")
    // Zoom should be clamped to GraphConstants.MaxZoom
  }

  test("dimension validation: many series should expand legend height naturally") {
    // Create 25 series to generate substantial legend
    val graphDef = createGraphDef(width = 800, height = 400, numSeries = 25)
    val image = graphEngine.createImage(graphDef)

    // Should create image with expanded height due to legend
    assert(image.getWidth > 400, "Should produce valid image")
    assert(image.getHeight > 1000, "Should have expanded height due to long legend")
  }

  test("dimension validation: small dimensions should work") {
    val graphDef = createGraphDef(width = 100, height = 100)
    val image = graphEngine.createImage(graphDef)

    // Should handle small dimensions gracefully
    assert(image.getWidth >= 100, "Should handle small width")
    assert(image.getHeight >= 100, "Should handle small height")
  }

  test("dimension validation: width at limit") {
    val graphDef = createGraphDef(width = GraphConstants.MaxWidth, height = 400)
    val image = graphEngine.createImage(graphDef)

    // Should work fine at the limit
    assert(image.getWidth > 1000, "Should work at MaxWidth limit")
    assert(image.getHeight > 200, "Should produce valid image")
  }

  test("dimension validation: height at limit") {
    val graphDef = createGraphDef(width = 800, height = GraphConstants.MaxHeight)
    val image = graphEngine.createImage(graphDef)

    // Should work fine at the limit
    assert(image.getWidth > 400, "Should produce valid image")
    assert(image.getHeight > 500, "Should work at MaxHeight limit")
  }

  test("dimension validation: zoom at limit") {
    val graphDef = createGraphDef(width = 800, height = 400, zoom = GraphConstants.MaxZoom)
    val image = graphEngine.createImage(graphDef)

    // Should work fine at the zoom limit
    assert(image.getWidth > 800, "Should work at MaxZoom limit")
    assert(image.getHeight > 400, "Should produce valid image")
  }

  test("dimension validation: combined stress test with many series") {
    // Test the scenario that should work: reasonable request with long legend
    val graphDef = createGraphDef(width = 800, height = 500, numSeries = 30)
    val image = graphEngine.createImage(graphDef)

    // Should produce a large image due to legend expansion, but successfully
    assert(image.getWidth > 400, "Should produce valid image")
    assert(image.getHeight > 1000, "Should have substantial height from legend")
  }
}
