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
package com.netflix.atlas.lwc.events

/**
  * Set of subscriptions to receive event data.
  *
  * @param events
  *     Subscriptions looking for the raw events to be passed through.
  * @param timeSeries
  *     Subscriptions that should be mapped into time series.
  * @param traceEvents
  *     Trace subscriptions looking for the trace spans to be passed through.
  * @param traceTimeSeries
  *     Trace subscriptions that should map the selected spans into time-series.
  */
case class Subscriptions(
  events: List[Subscription] = Nil,
  timeSeries: List[Subscription] = Nil,
  traceEvents: List[Subscription] = Nil,
  traceTimeSeries: List[Subscription] = Nil
)

object Subscriptions {

  val Events = "EVENTS"
  val TimeSeries = "TIME_SERIES"
  val TraceEvents = "TRACE_EVENTS"
  val TraceTimeSeries = "TRACE_TIME_SERIES"

  /**
    * Create instance from a flattened list with types based on the ExprType enum
    * from the eval library.
    */
  def fromTypedList(subs: List[Subscription]): Subscriptions = {
    val groups = subs.groupBy(_.exprType)
    Subscriptions(
      events = groups.getOrElse(Events, Nil),
      timeSeries = groups.getOrElse(TimeSeries, Nil),
      traceEvents = groups.getOrElse(TraceEvents, Nil),
      traceTimeSeries = groups.getOrElse(TraceTimeSeries, Nil)
    )
  }

  /** Compute set of added and removed expressions between the two sets. */
  def diff(a: Subscriptions, b: Subscriptions): Diff = {
    val (addedE, removedE, unchangedE) = diff(a.events, b.events)
    val (addedTS, removedTS, unchangedTS) = diff(a.timeSeries, b.timeSeries)
    val (addedTE, removedTE, unchangedTE) = diff(a.traceEvents, b.traceEvents)
    val (addedTTS, removedTTS, unchangedTTS) = diff(a.traceTimeSeries, b.traceTimeSeries)
    val added = Subscriptions(addedE, addedTS, addedTE, addedTTS)
    val removed = Subscriptions(removedE, removedTS, removedTE, removedTTS)
    val unchanged = Subscriptions(unchangedE, unchangedTS, unchangedTE, unchangedTTS)
    Diff(added, removed, unchanged)
  }

  private def diff[T](a: List[T], b: List[T]): (List[T], List[T], List[T]) = {
    val setA = a.toSet
    val setB = b.toSet
    val added = setB.diff(setA)
    val removed = setA.diff(setB)
    val unchanged = setA.intersect(setB)
    (added.toList, removed.toList, unchanged.toList)
  }

  case class Diff(added: Subscriptions, removed: Subscriptions, unchanged: Subscriptions)
}
