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
package com.netflix.atlas.eval.model;

/** Indicates the type of expression for a subscription. */
public enum ExprType {

  /** Expression to select a set of events to be passed through. */
  EVENTS(false),

  /**
   * Time series expression such as used with Atlas Graph API. Can also be used for analytics
   * queries on top of event data.
   */
  TIME_SERIES(true),

  /** Expression to select a set of traces to be passed through. */
  TRACE_EVENTS(false),

  /** Time series expression based on data extraced from traces. */
  TRACE_TIME_SERIES(true);

  private final boolean timeSeries;

  ExprType(boolean timeSeries) {
    this.timeSeries = timeSeries;
  }

  public boolean isEventType() {
    return !timeSeries;
  }

  public boolean isTimeSeriesType() {
    return timeSeries;
  }
}
