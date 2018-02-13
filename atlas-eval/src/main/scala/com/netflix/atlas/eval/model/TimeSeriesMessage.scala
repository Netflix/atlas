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
package com.netflix.atlas.eval.model

import com.fasterxml.jackson.core.JsonGenerator
import com.netflix.atlas.core.model._
import com.netflix.atlas.json.JsonSupport
import com.netflix.atlas.core.util.SmallHashMap

/**
  * Message type use for emitting time series data in LWC and fetch responses.
  *
  * @param id
  *     Identifier for the time series. This can be used to stitch together messages for
  *     the same time series over time. For example in a streaming use-case you get one
  *     message per interval for each time series. To get all of the message for a given
  *     time series group by this id.
  * @param query
  *     Expression for the time series. Note, the same expression can result in many time
  *     series when using group by. For matching the data for a particular time series the
  *     id field should be used.
  * @param groupByKeys
  *     The final keys used for grouping the result. The value will be an empty list if
  *     the expression is not grouped. For multi-level group by this will be the final
  *     grouping used for the result.
  * @param start
  *     Start time for the data.
  * @param end
  *     End time for the data.
  * @param step
  *     Time interval between data points.
  * @param label
  *     Label associated with the time series. This is either the auto-generated string
  *     based on the expression or the value specified by the legend.
  * @param tags
  *     Tags associated with the final expression result. This is the set of exact matches
  *     from the query plus any keys used in the group by clause.
  * @param data
  *     Data for the time series.
  */
case class TimeSeriesMessage(
  id: String,
  query: String,
  groupByKeys: List[String],
  start: Long,
  end: Long,
  step: Long,
  label: String,
  tags: Map[String, String],
  data: ChunkData
) extends JsonSupport {

  override def encode(gen: JsonGenerator) {
    gen.writeStartObject()
    gen.writeStringField("type", "timeseries")
    gen.writeStringField("id", id)
    gen.writeStringField("query", query)
    if (groupByKeys.nonEmpty) {
      gen.writeArrayFieldStart("groupByKeys")
      groupByKeys.foreach(gen.writeString)
      gen.writeEndArray()
    }
    gen.writeStringField("label", label)
    encodeTags(gen, tags)
    gen.writeNumberField("start", start)
    gen.writeNumberField("end", end)
    gen.writeNumberField("step", step)
    gen.writeFieldName("data")
    data.encode(gen)
    gen.writeEndObject()
  }

  private def encodeTags(gen: JsonGenerator, tags: Map[String, String]) {
    gen.writeObjectFieldStart("tags")
    tags match {
      case m: SmallHashMap[String, String] =>
        m.foreachItem { (k, v) =>
          gen.writeStringField(k, v)
        }
      case m: Map[String, String] =>
        m.foreach { t =>
          gen.writeStringField(t._1, t._2)
        }
    }
    gen.writeEndObject()
  }
}

object TimeSeriesMessage {

  /**
    * Create a new time series message.
    *
    * @param expr
    *     Expression for the time series. Note, the same expression can result in many time
    *     series when using group by. For matching the data for a particular time series the
    *     id field should be used.
    * @param context
    *     Evaluation context that is used for getting the start, end, and step size used
    *     for the message.
    * @param ts
    *     Time series to use for the message.
    */
  def apply(expr: StyleExpr, context: EvalContext, ts: TimeSeries): TimeSeriesMessage = {
    val query = expr.toString
    val id = TaggedItem.computeId(ts.tags + ("atlas.query" -> query)).toString
    val data = ts.data.bounded(context.start, context.end)
    TimeSeriesMessage(
      id,
      query,
      expr.expr.finalGrouping,
      context.start,
      context.end,
      context.step,
      ts.label,
      ts.tags,
      ArrayData(data.data)
    )
  }
}
