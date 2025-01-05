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
package com.netflix.atlas.pekko

import scala.concurrent.ExecutionContext

/**
  * Helper for accessing the opportunistic execution context added in 2.13.4. See
  * https://github.com/scala/scala/releases/tag/v2.13.4
  */
object OpportunisticEC {

  implicit val ec: ExecutionContext = {
    try {
      ExecutionContext.getClass
        .getDeclaredMethod("opportunistic")
        .invoke(ExecutionContext)
        .asInstanceOf[ExecutionContext]
    } catch {
      case _: NoSuchMethodException => ExecutionContext.global
    }
  }
}
