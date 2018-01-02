/*
 * Copyright 2014-2018 Netflix, Inc.
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
package com.netflix.atlas.chart.model;

/**
 * Type of scale to use for mapping raw input values into y-coordinates on the chart.
 */
public enum Scale {

  LINEAR,
  LOGARITHMIC,
  POWER_2,
  SQRT;

  /** Returns the scale constant associated with a given name. */
  public static Scale fromName(String s) {
    switch (s) {
      case "linear": return LINEAR;
      case "log":    return LOGARITHMIC;
      case "pow2":   return POWER_2;
      case "sqrt":   return SQRT;
      default:
        throw new IllegalArgumentException("unknown scale type '" + s
            + "', should be linear, log, pow2, or sqrt");
    }
  }
}
