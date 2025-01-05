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
package com.netflix.atlas.chart.graphics

import com.netflix.atlas.core.model.TagKey
import munit.FunSuite

class HeatmapSuite extends FunSuite {

  test("percentileBucketRange: timer") {
    val tags = Map(TagKey.percentile -> "T0042")
    assertEquals(Heatmap.percentileBucketRange(tags), Some(4.915e-5 -> 5.4611e-5))
  }

  test("percentileBucketRange: distribution summary") {
    val tags = Map(TagKey.percentile -> "D0042")
    assertEquals(Heatmap.percentileBucketRange(tags), Some(49150.0 -> 54611.0))
  }

  test("percentileBucketRange: unknown") {
    val tags = Map(TagKey.percentile -> "foo")
    assertEquals(Heatmap.percentileBucketRange(tags), None)
  }
}
