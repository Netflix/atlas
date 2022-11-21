/*
 * Copyright 2014-2022 Netflix, Inc.
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

import java.util.Locale;

/**
 * Mode for how to format the tick labels on Y-axes.
 */
public enum TickLabelMode {
  /**
   * Do no show tick labels. It can be useful to suppress the labels for including the charts
   * in presentations or support tickets where it is not desirable to share the exact numbers.
   */
  OFF,

  /**
   * Use decimal metric prefixes for tick labels.
   *
   * https://en.wikipedia.org/wiki/Metric_prefix
   */
  DECIMAL,

  /**
   * Use binary prefixes for tick labels. Typically only used for data in bytes such as disk
   * sizes.
   *
   * https://en.wikipedia.org/wiki/Binary_prefix
   */
  BINARY,

  /**
   * Use SI for durations less than a second and 's', 'min', 'hr', 'day', 'mon', 'yr' over that.
   */
  DURATION;

  /**
   * Case insensitive conversion of a string name to an enum value.
   */
  public static TickLabelMode apply(String mode) {
    try {
      return valueOf(mode.toUpperCase(Locale.US));
    } catch (IllegalArgumentException e) {
      // Make the error message a bit more user friendly
      throw new IllegalArgumentException("tick_labels: unknown value '" + mode
          + "', acceptable values are off, decimal, and binary");
    }
  }
}
