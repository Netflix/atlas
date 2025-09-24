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
package com.netflix.atlas.eval.model

import com.fasterxml.jackson.core.JsonGenerator
import com.netflix.atlas.json.JsonSupport

/**
  * Chunk of data for LWC and fetch responses.
  */
sealed trait ChunkData extends JsonSupport {

  def typeName: String
}

/**
  * This is a set of values with a fixed start time and step. The additional
  * metadata can be found on the [[TimeSeriesMessage]] object that contains
  * this chunk.
  *
  * @param values
  *     Time series values assocated with a [[TimeSeriesMessage]].
  */
final case class ArrayData(values: Array[Double]) extends ChunkData {

  def typeName: String = "array"

  override def hasCustomEncoding: Boolean = true

  override def encode(gen: JsonGenerator): Unit = {
    gen.writeStartObject()
    gen.writeStringField("type", "array")
    gen.writeArrayFieldStart("values")
    var i = 0
    while (i < values.length) {
      val v = values(i)
      if (v.isNaN || v.isInfinite) gen.writeString(v.toString) else gen.writeNumber(v)
      i += 1
    }
    gen.writeEndArray()
    gen.writeEndObject()
  }

  override def toString: String = values.mkString("ArrayData(", ",", ")")

  override def equals(other: Any): Boolean = {

    // Follows guidelines from: http://www.artima.com/pins1ed/object-equality.html#28.4
    other match {
      case that: ArrayData =>
        that.canEqual(this) && java.util.Arrays.equals(values, that.values)
      case _ => false
    }
  }

  override def hashCode: Int = {
    java.util.Arrays.hashCode(values)
  }

  def canEqual(other: Any): Boolean = {
    other.isInstanceOf[ArrayData]
  }
}
