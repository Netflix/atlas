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
package com.netflix.atlas.eval.util

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.netflix.atlas.core.util.SortedTagMap

/**
  * Custom deserializer for tag maps to go directly to `SortedTagMap` type. It is assumed
  * that each tag map should have a relatively small number of entries.
  *
  * @param initSize
  *     The initial size to use for the maps.
  */
class SortedTagMapDeserializer(initSize: Int) extends JsonDeserializer[SortedTagMap] {

  /** Default constructor so it can be used with the annotations. */
  def this() = this(10)

  override def deserialize(p: JsonParser, ctxt: DeserializationContext): SortedTagMap = {
    val builder = SortedTagMap.builder(initSize)
    var k = p.nextFieldName()
    while (k != null) {
      val v = p.nextTextValue()
      builder.add(k, v)
      k = p.nextFieldName()
    }
    builder.result()
  }
}
