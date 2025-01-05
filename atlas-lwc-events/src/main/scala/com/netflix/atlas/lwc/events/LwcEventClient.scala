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

import com.netflix.atlas.json.Json
import com.netflix.spectator.api.Clock

import java.io.StringWriter
import scala.util.Using

/**
  * Client for processing events to make available via LWC stream.
  */
trait LwcEventClient {

  /**
    * Check if an event with a partial set of tags could match one of the expressions.
    * This can be used as a pre-filter for cases where an expensive transform is required
    * to generate the final event for consideration.
    *
    * @param tags
    *     Function that returns the tag value for a given key, or `null` if there is no
    *     value for that key. This tag set may be a subset of the final set of tags that
    *     would be on the actual events.
    * @return
    *     True if an event with the partial set of tags might match one of the current
    *     expressions.
    */
  def couldMatch(tags: String => String): Boolean

  /**
    * Submit an event for a given subscription.
    *
    * @param id
    *     Id of the subscription that should receive the event.
    * @param event
    *     Event to submit to the subscription.
    */
  def submit(id: String, event: LwcEvent): Unit

  /** Process event for the stream. */
  def process(event: LwcEvent): Unit

  /**
    * Process a trace for the stream.
    *
    * @param trace
    *     The trace is modelled as a sequence of spans as the trace graph can be costly
    *     to construct as part of the ingestion pipeline.
    */
  def processTrace(trace: Seq[LwcEvent.Span]): Unit
}

object LwcEventClient {

  /**
    * Create a new local client that forwards the results to the consumer function. This
    * is mostly used for testing and debugging.
    *
    * @param subscriptions
    *     Set of subscriptions to match with the events.
    * @param consumer
    *     Function that will receive the output.
    * @param clock
    *     Clock to use for timing events.
    * @return
    *     Client instance.
    */
  def apply(
    subscriptions: Subscriptions,
    consumer: String => Unit,
    clock: Clock = Clock.SYSTEM
  ): LwcEventClient = {
    new LocalLwcEventClient(subscriptions, consumer, clock)
  }

  private class LocalLwcEventClient(
    subscriptions: Subscriptions,
    consumer: String => Unit,
    clock: Clock
  ) extends AbstractLwcEventClient(clock) {

    sync(subscriptions)

    override def submit(id: String, event: LwcEvent): Unit = {
      Using.resource(new StringWriter()) { w =>
        Using.resource(Json.newJsonGenerator(w)) { gen =>
          gen.writeStartObject()
          gen.writeStringField("id", id)
          gen.writeFieldName("event")
          event.encode(gen)
          gen.writeEndObject()
        }
        consumer(s"data: ${w.toString}")
      }
    }
  }
}
