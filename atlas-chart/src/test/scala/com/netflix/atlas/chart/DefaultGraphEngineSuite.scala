/*
 * Copyright 2014-2026 Netflix, Inc.
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
    assert(
      image.getHeight <= GraphConstants.MaxHeight + 300,
      "Height should not exceed MaxHeight + UI"
    )
  }

  test("dimension validation: oversized zoom should be clamped") {
    val graphDef = createGraphDef(width = 800, height = 400, zoom = 10.0) // exceeds MaxZoom of 2.0
    val image = graphEngine.createImage(graphDef)

    // Should clamp zoom to MaxZoom and produce valid image
    assert(image.getWidth > 400, "Should produce valid image with clamped zoom")
    assert(image.getHeight > 200, "Should produce valid image")
    // Zoom should be clamped to GraphConstants.MaxZoom
    // The final image.getWidth should be smaller than the
    // GraphConstants.MaxWidth
    assert(image.getWidth <= GraphConstants.MaxWidth)
  }

  test("dimension validation: many series should expand legend height naturally") {
    // Create 25 series to generate substantial legend
    val graphDef = createGraphDef(width = 800, height = 400, numSeries = 25)
    val image = graphEngine.createImage(graphDef)

    // Should create image with expanded height due to legend
    assert(image.getWidth > 400, "Should produce valid image")
    assert(image.getWidth < GraphConstants.MaxWidth)
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

    // Should work fine at the limit and should also account
    // for the UI elements which are estimated to be 200
    assert(image.getWidth >= GraphConstants.MaxWidth, "Width should be at least MaxWidth")
    assert(image.getWidth <= GraphConstants.MaxWidth + 200, "Width should not exceed MaxWidth + UI")
    assert(image.getHeight > 200, "Should produce valid image")
  }

  test("dimension validation: height at limit") {
    val graphDef = createGraphDef(width = 800, height = GraphConstants.MaxHeight)
    val image = graphEngine.createImage(graphDef)

    // Should work fine at the limit
    assert(image.getWidth <= GraphConstants.MaxWidth, "Should produce valid image")
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

  test("GraphConstants validation: oversized dimensions should be clamped") {
    // Test the validation logic directly before any UI rendering
    val validation = GraphConstants.validate(5000, 50000, 10.0)

    // Original values should be preserved
    assertEquals(validation.originalWidth, 5000)
    assertEquals(validation.originalHeight, 50000)
    assertEquals(validation.originalZoom, 10.0)

    // Clamped values should respect limits
    assertEquals(validation.clampedWidth, GraphConstants.MaxWidth)
    assertEquals(validation.clampedHeight, GraphConstants.MaxHeight)
    assertEquals(validation.clampedZoom, GraphConstants.MaxZoom)

    // Should generate warnings
    assertEquals(validation.warnings.size, 3)
    assert(validation.warnings.exists(_.contains("width")))
    assert(validation.warnings.exists(_.contains("height")))
    assert(validation.warnings.exists(_.contains("zoom")))
  }

  test("GraphConstants validation: normal dimensions should pass") {
    val validation = GraphConstants.validate(800, 400, 1.0)

    assertEquals(validation.clampedWidth, 800)
    assertEquals(validation.clampedHeight, 400)
    assertEquals(validation.clampedZoom, 1.0)
    assertEquals(validation.warnings.size, 0)
  }

  test("GraphConstants validation: only width oversized") {
    val validation = GraphConstants.validate(5000, 400, 1.0)

    assertEquals(validation.clampedWidth, GraphConstants.MaxWidth)
    assertEquals(validation.clampedHeight, 400)
    assertEquals(validation.clampedZoom, 1.0)
    assertEquals(validation.warnings.size, 1)
    assert(validation.warnings.head.contains("width"))
  }

  test("GraphConstants validation: only height oversized") {
    val validation = GraphConstants.validate(800, 5000, 1.0)

    assertEquals(validation.clampedWidth, 800)
    assertEquals(validation.clampedHeight, GraphConstants.MaxHeight)
    assertEquals(validation.clampedZoom, 1.0)
    assertEquals(validation.warnings.size, 1)
    assert(validation.warnings.head.contains("height"))
  }

  test("GraphConstants validation: only zoom oversized") {
    val validation = GraphConstants.validate(800, 400, 10.0)

    assertEquals(validation.clampedWidth, 800)
    assertEquals(validation.clampedHeight, 400)
    assertEquals(validation.clampedZoom, GraphConstants.MaxZoom)
    assertEquals(validation.warnings.size, 1)
    assert(validation.warnings.head.contains("zoom"))
  }

  test("GraphConstants validation: dimensions at exact limits") {
    val validation = GraphConstants.validate(
      GraphConstants.MaxWidth,
      GraphConstants.MaxHeight,
      GraphConstants.MaxZoom
    )

    assertEquals(validation.clampedWidth, GraphConstants.MaxWidth)
    assertEquals(validation.clampedHeight, GraphConstants.MaxHeight)
    assertEquals(validation.clampedZoom, GraphConstants.MaxZoom)
    assertEquals(validation.warnings.size, 0)
  }

  test("GraphConstants validation: dimensions one over limit") {
    val validation = GraphConstants.validate(
      GraphConstants.MaxWidth + 1,
      GraphConstants.MaxHeight + 1,
      GraphConstants.MaxZoom + 0.1
    )

    assertEquals(validation.clampedWidth, GraphConstants.MaxWidth)
    assertEquals(validation.clampedHeight, GraphConstants.MaxHeight)
    assertEquals(validation.clampedZoom, GraphConstants.MaxZoom)
    assertEquals(validation.warnings.size, 3)
  }

  test("GraphConstants validation: zero and negative dimensions") {
    val validation = GraphConstants.validate(0, -100, 0.0)

    // math.min preserves smaller values (including negatives)
    assertEquals(validation.clampedWidth, 0)
    assertEquals(validation.clampedHeight, -100)
    assertEquals(validation.clampedZoom, 0.0)
    assertEquals(validation.warnings.size, 0) // No warnings since under limits
  }

  test("GraphConstants validation: extreme values") {
    val validation = GraphConstants.validate(Int.MaxValue, Int.MaxValue, Double.MaxValue)

    assertEquals(validation.clampedWidth, GraphConstants.MaxWidth)
    assertEquals(validation.clampedHeight, GraphConstants.MaxHeight)
    assertEquals(validation.clampedZoom, GraphConstants.MaxZoom)
    assertEquals(validation.warnings.size, 3)
  }
}
