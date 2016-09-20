/*
 * Copyright 2014-2016 Netflix, Inc.
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
package com.netflix.atlas.lwcapi

import akka.actor.ActorRef

abstract class SubscriptionManager() {
  def register(sseId: String, ref: ActorRef): Unit
  def subscribe(expressionId: String, sseId: String, ref: ActorRef): Unit
  def unsubscribe(expressionId: String, sseId: String, ref: ActorRef): Unit
  def getActorsForExpressionId(expressionId: String): Set[ActorRef]
  def getExpressionsForSSEId(sseId: String): Set[String]
  def unsubscribeAll(sseId: String, ref: ActorRef): Unit
}
