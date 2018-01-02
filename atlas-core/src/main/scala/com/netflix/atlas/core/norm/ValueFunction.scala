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
package com.netflix.atlas.core.norm

trait ValueFunction {
  def apply(timestamp: Long, value: Double)
}

/**
 * Simple value function used for testing and debugging. Collects output in
 * a list so it is easy to examine.
 */
class ListValueFunction extends ValueFunction {
  private var builder = List.newBuilder[(Long, Double)]

  var f: ValueFunction = this

  def update(timestamp: Long, value: Double): List[(Long, Double)] = {
    builder = List.newBuilder[(Long, Double)]
    f(timestamp, value)
    builder.result
  }

  def apply(timestamp: Long, value: Double): Unit = {
    builder += timestamp -> value
  }
}
