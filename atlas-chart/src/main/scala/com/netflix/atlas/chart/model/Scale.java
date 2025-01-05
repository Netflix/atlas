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
package com.netflix.atlas.chart.model;

/**
 * Type of scale to use for mapping raw input values into y-coordinates on the chart.
 */
public enum Scale {

  LINEAR,
  LOGARITHMIC,
  LOG_LINEAR,
  POWER_2,
  SQRT;

  /** Returns the scale constant associated with a given name. */
  public static Scale fromName(String s) {
    return switch (s) {
      case "linear"     -> LINEAR;
      case "log"        -> LOGARITHMIC;
      case "log-linear" -> LOG_LINEAR;
      case "pow2"       -> POWER_2;
      case "sqrt"       -> SQRT;
      default           -> throw new IllegalArgumentException("unknown scale type '" + s
          + "', should be linear, log, log-linear, pow2, or sqrt");
    };
  }
}
