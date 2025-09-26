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

import munit.FunSuite

class GraphConstantsSuite extends FunSuite {

  test("validate - normal dimensions should pass without warnings") {
    val result = GraphConstants.validate(800, 400, 1.0)
    
    assertEquals(result.originalWidth, 800)
    assertEquals(result.originalHeight, 400)
    assertEquals(result.originalZoom, 1.0)
    assertEquals(result.clampedWidth, 800)
    assertEquals(result.clampedHeight, 400)
    assertEquals(result.clampedZoom, 1.0)
    assertEquals(result.warnings, List.empty)
  }

  test("validate - width exceeding max should be clamped with warning") {
    val result = GraphConstants.validate(5000, 400, 1.0)
    
    assertEquals(result.originalWidth, 5000)
    assertEquals(result.originalHeight, 400)
    assertEquals(result.originalZoom, 1.0)
    assertEquals(result.clampedWidth, GraphConstants.MaxWidth)
    assertEquals(result.clampedHeight, 400)
    assertEquals(result.clampedZoom, 1.0)
    assertEquals(result.warnings.size, 1)
    assert(result.warnings.head.contains("Restricted graph width"))
    assert(result.warnings.head.contains(s"${GraphConstants.MaxWidth}"))
  }

  test("validate - height exceeding max should be clamped with warning") {
    val result = GraphConstants.validate(800, 5000, 1.0)
    
    assertEquals(result.originalWidth, 800)
    assertEquals(result.originalHeight, 5000)
    assertEquals(result.originalZoom, 1.0)
    assertEquals(result.clampedWidth, 800)
    assertEquals(result.clampedHeight, GraphConstants.MaxHeight)
    assertEquals(result.clampedZoom, 1.0)
    assertEquals(result.warnings.size, 1)
    assert(result.warnings.head.contains("Restricted graph height"))
    assert(result.warnings.head.contains(s"${GraphConstants.MaxHeight}"))
  }

  test("validate - zoom exceeding max should be clamped with warning") {
    val result = GraphConstants.validate(800, 400, 10.0)
    
    assertEquals(result.originalWidth, 800)
    assertEquals(result.originalHeight, 400)
    assertEquals(result.originalZoom, 10.0)
    assertEquals(result.clampedWidth, 800)
    assertEquals(result.clampedHeight, 400)
    assertEquals(result.clampedZoom, GraphConstants.MaxZoom)
    assertEquals(result.warnings.size, 1)
    assert(result.warnings.head.contains("Restricted zoom"))
    assert(result.warnings.head.contains(s"${GraphConstants.MaxZoom}"))
  }

  test("validate - all dimensions exceeding max should generate multiple warnings") {
    val result = GraphConstants.validate(5000, 3000, 8.0)
    
    assertEquals(result.originalWidth, 5000)
    assertEquals(result.originalHeight, 3000)
    assertEquals(result.originalZoom, 8.0)
    assertEquals(result.clampedWidth, GraphConstants.MaxWidth)
    assertEquals(result.clampedHeight, GraphConstants.MaxHeight)
    assertEquals(result.clampedZoom, GraphConstants.MaxZoom)
    assertEquals(result.warnings.size, 3)
    
    val warningText = result.warnings.mkString(" ")
    assert(warningText.contains("Restricted graph height"))
    assert(warningText.contains("Restricted graph width"))
    assert(warningText.contains("Restricted zoom"))
  }

  test("validate - dimensions at exact limits should not generate warnings") {
    val result = GraphConstants.validate(GraphConstants.MaxWidth, GraphConstants.MaxHeight, GraphConstants.MaxZoom)
    
    assertEquals(result.originalWidth, GraphConstants.MaxWidth)
    assertEquals(result.originalHeight, GraphConstants.MaxHeight)
    assertEquals(result.originalZoom, GraphConstants.MaxZoom)
    assertEquals(result.clampedWidth, GraphConstants.MaxWidth)
    assertEquals(result.clampedHeight, GraphConstants.MaxHeight)
    assertEquals(result.clampedZoom, GraphConstants.MaxZoom)
    assertEquals(result.warnings, List.empty)
  }

  test("validate - dimensions one over limit should generate warnings") {
    val result = GraphConstants.validate(
      GraphConstants.MaxWidth + 1, 
      GraphConstants.MaxHeight + 1, 
      GraphConstants.MaxZoom + 0.1
    )
    
    assertEquals(result.originalWidth, GraphConstants.MaxWidth + 1)
    assertEquals(result.originalHeight, GraphConstants.MaxHeight + 1)
    assertEquals(result.originalZoom, GraphConstants.MaxZoom + 0.1)
    assertEquals(result.clampedWidth, GraphConstants.MaxWidth)
    assertEquals(result.clampedHeight, GraphConstants.MaxHeight)
    assertEquals(result.clampedZoom, GraphConstants.MaxZoom)
    assertEquals(result.warnings.size, 3)
  }

  test("validate - negative dimensions stay negative (math.min behavior)") {
    val result = GraphConstants.validate(-100, -50, -1.0)
    
    assertEquals(result.originalWidth, -100)
    assertEquals(result.originalHeight, -50)
    assertEquals(result.originalZoom, -1.0)
    // math.min keeps negative values since they're smaller than positive max values
    assertEquals(result.clampedWidth, -100) // math.min(-100, MaxWidth) = -100
    assertEquals(result.clampedHeight, -50) // math.min(-50, MaxHeight) = -50
    assertEquals(result.clampedZoom, -1.0) // math.min(-1.0, MaxZoom) = -1.0
    assertEquals(result.warnings, List.empty) // No warnings since they're under the max
  }

  test("validate - zero dimensions should work") {
    val result = GraphConstants.validate(0, 0, 0.0)
    
    assertEquals(result.originalWidth, 0)
    assertEquals(result.originalHeight, 0)
    assertEquals(result.originalZoom, 0.0)
    assertEquals(result.clampedWidth, 0)
    assertEquals(result.clampedHeight, 0)
    assertEquals(result.clampedZoom, 0.0)
    assertEquals(result.warnings, List.empty)
  }

  test("validate - very large dimensions should be handled gracefully") {
    val result = GraphConstants.validate(Int.MaxValue, Int.MaxValue, Double.MaxValue)
    
    assertEquals(result.originalWidth, Int.MaxValue)
    assertEquals(result.originalHeight, Int.MaxValue)
    assertEquals(result.originalZoom, Double.MaxValue)
    assertEquals(result.clampedWidth, GraphConstants.MaxWidth)
    assertEquals(result.clampedHeight, GraphConstants.MaxHeight)
    assertEquals(result.clampedZoom, GraphConstants.MaxZoom)
    assertEquals(result.warnings.size, 3)
  }

  test("ValidationResult - should preserve all original values") {
    val original = (1234, 5678, 3.14)
    val result = GraphConstants.validate(original._1, original._2, original._3)
    
    // Original values should always be preserved exactly
    assertEquals(result.originalWidth, original._1)
    assertEquals(result.originalHeight, original._2)  
    assertEquals(result.originalZoom, original._3)
  }

  test("ValidationResult - clamped values should never exceed constants") {
    val result = GraphConstants.validate(99999, 99999, 99999.0)
    
    assert(result.clampedWidth <= GraphConstants.MaxWidth)
    assert(result.clampedHeight <= GraphConstants.MaxHeight)
    assert(result.clampedZoom <= GraphConstants.MaxZoom)
  }
}