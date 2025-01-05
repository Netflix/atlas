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
package com.netflix.atlas.chart

/**
  * This is only present for backwards compatibility. There is no longer any difference between
  * this and the `json` format.
  *
  * Json output format that quotes non-numeric values like NaN and Infinity. Historically we
  * have always emitted them unquoted, but that is not standard JSON and so it creates issues with
  * some parsers. To avoid breaking backwards compatibility the existing "json" type will not
  * change, but this class adds a "std.json" type. The non-compatible json will not be used on
  * any v2 endpoints.
  */
class StdJsonGraphEngine extends JsonGraphEngine {

  override def name: String = "std.json"
}
