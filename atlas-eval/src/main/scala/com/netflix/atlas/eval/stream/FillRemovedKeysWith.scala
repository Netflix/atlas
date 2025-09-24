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

import org.apache.pekko.stream.Attributes
import org.apache.pekko.stream.FlowShape
import org.apache.pekko.stream.Inlet
import org.apache.pekko.stream.Outlet
import org.apache.pekko.stream.stage.GraphStage
import org.apache.pekko.stream.stage.GraphStageLogic
import org.apache.pekko.stream.stage.InHandler
import org.apache.pekko.stream.stage.OutHandler

import scala.collection.Map

/**
  * This operator compares keys of current Map with the very previous one in the stream, and fill
  * removed keys using "valueForRemovedKey" function.
  *
  * Typical usage is to plug this flow before groupBy, so that if a key is removed, a custom value
  * can be pushed down to sub-streams to trigger appropriate action immediately, as apposed to wait
  * for subscription timeout and then cleanup.
  */
private[stream] class FillRemovedKeysWith[K, V](valueForRemovedKey: K => V)
    extends GraphStage[FlowShape[Map[K, V], Map[K, V]]] {

  private val in = Inlet[Map[K, V]]("FillRemovedKeys.in")
  private val out = Outlet[Map[K, V]]("FillRemovedKeys.out")

  override val shape: FlowShape[Map[K, V], Map[K, V]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler with OutHandler {

      private var prevKeySet = Set.empty[K]

      override def onPush(): Unit = {
        val map = grab(in)
        val newMap = scala.collection.mutable.Map.empty[K, V] ++ map

        val removedKeys = prevKeySet -- newMap.keySet
        removedKeys.foreach { k =>
          newMap.put(k, valueForRemovedKey(k))
        }

        prevKeySet = map.keySet.toSet
        push(out, newMap)
      }

      override def onPull(): Unit = {
        pull(in)
      }

      setHandlers(in, out, this)
    }
  }
}
