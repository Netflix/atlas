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
package com.netflix.atlas.postgres

import java.time.Instant

private[postgres] case class Interval(start: Instant, end: Instant) {

  def overlaps(interval: Interval): Boolean = {
    val s1 = start.toEpochMilli
    val e1 = end.toEpochMilli
    val s2 = interval.start.toEpochMilli
    val e2 = interval.end.toEpochMilli

    (s1 >= s2 && s1 <= e2) || (e1 >= s2 && e1 <= e2) || (s1 >= s2 && e1 <= e2) || (s2 >= s1 && e2 <= e1)
  }
}

private[postgres] object Interval {

  def apply(s: Long, e: Long): Interval = {
    Interval(Instant.ofEpochMilli(s), Instant.ofEpochMilli(e))
  }
}
