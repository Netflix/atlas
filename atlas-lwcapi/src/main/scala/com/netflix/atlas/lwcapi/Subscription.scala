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
package com.netflix.atlas.lwcapi

case class Subscription(query: SpectatorQuery, metadata: ExpressionMetadata) {

  /**
    * The hash code is needed for most of the usage, precompute when it is created to
    * keep that usage as cheap as possible.
    */
  private val cachedHashCode = super.hashCode()

  override def hashCode(): Int = cachedHashCode
}
