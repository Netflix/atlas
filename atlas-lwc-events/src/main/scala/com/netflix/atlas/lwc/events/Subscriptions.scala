/*
 * Copyright 2014-2023 Netflix, Inc.
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
package com.netflix.atlas.lwc.events

/**
  * Set of subscriptions to receive event data.
  *
  * @param passThrough
  *     Subscriptions looking for the raw events to be passed through.
  * @param analytics
  *     Subscriptions that should be mapped into time series.
  * @param tracePassThrough
  *     Trace subscriptions looking for the trace spans to be passed through.
  * @param traceAnalytics
  *     Trace subscriptions that should map the selected spans into time-series.
  */
case class Subscriptions(
  passThrough: List[Subscription] = Nil,
  analytics: List[Subscription] = Nil,
  tracePassThrough: List[Subscription] = Nil,
  traceAnalytics: List[Subscription] = Nil
)
