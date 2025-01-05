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

import java.awt.Color;

/**
 * Convert a color to simulate a type of color blindness for those with normal vision. Based on:
 * <a href="http://web.archive.org/web/20081014161121/http://www.colorjack.com/labs/colormatrix/">colormatrix</a>.
 */
public enum VisionType {
  normal(new double[] {
    1.000, 0.000, 0.000, 0.000, // R
    0.000, 1.000, 0.000, 0.000, // G
    0.000, 0.000, 1.000, 0.000, // B
    0.000, 0.000, 0.000, 1.000  // A
  }),

  protanopia(new double[] {
    0.567, 0.433, 0.000, 0.000, // R
    0.558, 0.442, 0.000, 0.000, // G
    0.000, 0.242, 0.758, 0.000, // B
    0.000, 0.000, 0.000, 1.000  // A
  }),

  protanomaly(new double[] {
    0.817, 0.183, 0.000, 0.000, // R
    0.333, 0.667, 0.000, 0.000, // G
    0.000, 0.125, 0.875, 0.000, // B
    0.000, 0.000, 0.000, 1.000  // A
  }),

  deuteranopia(new double[] {
    0.625, 0.375, 0.000, 0.000, // R
    0.700, 0.300, 0.000, 0.000, // G
    0.000, 0.300, 0.700, 0.000, // B
    0.000, 0.000, 0.000, 1.000  // A
  }),

  deuteranomaly(new double[] {
    0.800, 0.200, 0.000, 0.000, // R
    0.258, 0.742, 0.000, 0.000, // G
    0.000, 0.142, 0.858, 0.000, // B
    0.000, 0.000, 0.000, 1.000  // A
  }),

  tritanopia(new double[] {
    0.950, 0.050, 0.000, 0.000, // R
    0.000, 0.433, 0.567, 0.000, // G
    0.000, 0.475, 0.525, 0.000, // B
    0.000, 0.000, 0.000, 1.000  // A
  }),

  tritanomaly(new double[] {
    0.967, 0.033, 0.000, 0.000, // R
    0.000, 0.733, 0.267, 0.000, // G
    0.000, 0.183, 0.817, 0.000, // B
    0.000, 0.000, 0.000, 1.000  // A
  }),

  achromatopsia(new double[] {
    0.299, 0.587, 0.114, 0.000, // R
    0.299, 0.587, 0.114, 0.000, // G
    0.299, 0.587, 0.114, 0.000, // B
    0.000, 0.000, 0.000, 1.000  // A
  }),

  achromatomaly(new double[] {
    0.618, 0.320, 0.062, 0.000, // R
    0.163, 0.775, 0.062, 0.000, // G
    0.163, 0.320, 0.516, 0.000, // B
    0.000, 0.000, 0.000, 1.000  // A
  });

  private final double[] m;

  VisionType(double[] m) {
    this.m = m;
  }

  public Color convert(Color c) {
    final int cr = c.getRed();
    final int cg = c.getGreen();
    final int cb = c.getBlue();
    final int ca = c.getAlpha();

    final int br = (int) (cr * m[0] + cg * m[1] + cb * m[2] + ca * m[3]);
    final int bg = (int) (cr * m[4] + cg * m[5] + cb * m[6] + ca * m[7]);
    final int bb = (int) (cr * m[8] + cg * m[9] + cb * m[10] + ca * m[11]);
    final int ba = (int) (cr * m[12] + cg * m[13] + cb * m[14] + ca * m[15]);

    return new Color(br, bg, bb, ba);
  }
}

