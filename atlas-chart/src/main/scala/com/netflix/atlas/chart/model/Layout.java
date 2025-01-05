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
 * Layout mode to use for the chart.
 */
public enum Layout {
  /**
   * Size is specified for the canvas showing the data and the image size is determined
   * automatically. This mode is the default and is intended to make it easy to get a consistent
   * canvas size for a set of charts.
   */
  CANVAS(false, false),

  /**
   * Size is specified for the image. Other elements will resize or shut off to fit within the
   * requested size.
   */
  IMAGE(true, true),

  /**
   * Width is for the image, but height is for the canvas. This can be useful for pages that
   * need to perform column spans for some rows. The height will be automatically determined.
   */
  IMAGE_WIDTH(true, false),

  /**
   * Height is for the image, but width is for the canvas.
   */
  IMAGE_HEIGHT(false, true);


  private final boolean fixedWidth;
  private final boolean fixedHeight;

  Layout(boolean fixedWidth, boolean fixedHeight) {
    this.fixedWidth = fixedWidth;
    this.fixedHeight = fixedHeight;
  }

  public boolean isFixedWidth() {
    return fixedWidth;
  }

  public boolean isFixedHeight() {
    return fixedHeight;
  }

  public static Layout create(String name) {
    return switch (name) {
      case "canvas" -> Layout.CANVAS;
      case "image"  -> Layout.IMAGE;
      case "iw"     -> Layout.IMAGE_WIDTH;
      case "ih"     -> Layout.IMAGE_HEIGHT;
      default       -> throw new IllegalArgumentException("unknown layout: " + name);
    };
  }
}
