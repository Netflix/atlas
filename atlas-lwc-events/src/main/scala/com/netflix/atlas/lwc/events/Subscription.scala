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
  * Subscription to receive data.
  *
  * @param id
  *     Id used to pair the received data with a given consumer.
  * @param frequency
  *     Step size for the subscription when mapped into a time series.
  * @param expression
  *     Expression for matching events and mapping into the expected output.
  * @param exprType
  *     Type for the expression.
  */
case class Subscription(id: String, frequency: Long, expression: String, exprType: String) {

  def step: Long = frequency
}
