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

import com.netflix.atlas.json.JsonSupport
import com.netflix.atlas.pekko.StreamOps

private[stream] trait DataSourceLogger {

  def apply(ds: Evaluator.DataSource, msg: JsonSupport): Unit

  def close(): Unit
}

private[stream] object DataSourceLogger {

  case object Noop extends DataSourceLogger {

    override def apply(ds: Evaluator.DataSource, msg: JsonSupport): Unit = {}

    override def close(): Unit = {}
  }

  case class Queue(queue: StreamOps.SourceQueue[Evaluator.MessageEnvelope])
      extends DataSourceLogger {

    override def apply(ds: Evaluator.DataSource, msg: JsonSupport): Unit = {
      val env = new Evaluator.MessageEnvelope(ds.id, msg)
      queue.offer(env)
    }

    override def close(): Unit = {
      queue.complete()
    }
  }
}
