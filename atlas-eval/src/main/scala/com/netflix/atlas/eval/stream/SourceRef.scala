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
package com.netflix.atlas.eval.stream

import org.apache.pekko.Done
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Promise
import scala.util.Success

/**
  * Reference to a source and a promise that can be completed to stop the
  * source.
  */
private[stream] case class SourceRef[T, M](source: Source[T, M], promise: Promise[Done]) {

  def stop(): Unit = promise.complete(Success(Done))
}
