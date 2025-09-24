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
package com.netflix.atlas.json

import java.io.StringWriter
import com.fasterxml.jackson.core.JsonGenerator

import scala.util.Using

/**
  * Trait that adds methods to easily encode the object to JSON. Can be used to indicate
  * a type that has a known mapping to JSON format.
  */
trait JsonSupport {

  /** Returns true if a custom encoding is used that does not rely on `Json.encode`. */
  def hasCustomEncoding: Boolean = false

  /**
    * Encode this object as JSON. By default it will just use `Json.encode`. This method
    * can be overridden to customize the format or to provide a more performance implementation.
    * When using a custom format, the subclass should also override `hasCustomEncoding` to
    * return true. This will cause `Json.encode` to use the custom implementation rather than
    * the default serializer for the type.
    */
  def encode(gen: JsonGenerator): Unit = {
    Json.encode(gen, this)
  }

  /** Returns a JSON string representing this object. */
  final def toJson: String = {
    Using.resource(new StringWriter) { w =>
      Using.resource(Json.newJsonGenerator(w)) { gen =>
        encode(gen)
      }
      w.toString
    }
  }
}
