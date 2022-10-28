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
package com.netflix.atlas.json

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider

private[json] class JsonSupportSerializer(
  defaultSerializer: JsonSerializer[AnyRef]
) extends JsonSerializer[JsonSupport] {

  override def serialize(
    value: JsonSupport,
    gen: JsonGenerator,
    serializers: SerializerProvider
  ): Unit = {

    if (value.hasCustomEncoding)
      value.encode(gen)
    else
      defaultSerializer.serialize(value, gen, serializers)
  }
}
