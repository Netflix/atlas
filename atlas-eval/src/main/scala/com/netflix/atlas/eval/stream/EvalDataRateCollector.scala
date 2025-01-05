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

import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.util.RefIntHashMap
import com.netflix.atlas.eval.model.EvalDataRate
import com.netflix.atlas.eval.model.EvalDataSize

import scala.collection.mutable

class EvalDataRateCollector(timestamp: Long, step: Long) {

  private val inputCounts = mutable.Map.empty[String, RefIntHashMap[DataExpr]]
  private val intermediateCounts = mutable.Map.empty[String, RefIntHashMap[DataExpr]]
  private val outputCounts = new RefIntHashMap[String]

  def incrementOutput(id: String, amount: Int): Unit = {
    outputCounts.increment(id, amount)
  }

  def incrementIntermediate(id: String, dataExpr: DataExpr, amount: Int): Unit = {
    increment(intermediateCounts, id, dataExpr, amount)
  }

  def incrementInput(id: String, dataExpr: DataExpr, amount: Int): Unit = {
    increment(inputCounts, id, dataExpr, amount)
  }

  def getAll: Map[String, EvalDataRate] = {
    inputCounts.map {
      case (id, _) =>
        id -> EvalDataRate(
          timestamp,
          step,
          getDataRate(inputCounts, id),
          getDataRate(intermediateCounts, id),
          EvalDataSize(outputCounts.get(id, 0))
        )
    }.toMap
  }

  private def getDataRate(
    counts: mutable.Map[String, RefIntHashMap[DataExpr]],
    id: String
  ): EvalDataSize = {
    counts.get(id) match {
      case Some(v: RefIntHashMap[DataExpr]) =>
        var total = 0
        val builder = Map.newBuilder[String, Int]
        v.foreach { (dataExpr, count) =>
          builder += dataExpr.toString -> count
          total += count
        }
        EvalDataSize(total, builder.result())
      case None => EvalDataRateCollector.EmptyRate
    }
  }

  private def increment(
    counts: mutable.Map[String, RefIntHashMap[DataExpr]],
    id: String,
    dataExpr: DataExpr,
    amount: Int
  ): Unit = {
    counts.getOrElseUpdate(id, new RefIntHashMap[DataExpr]).increment(dataExpr, amount)
  }
}

object EvalDataRateCollector {
  val EmptyRate: EvalDataSize = EvalDataSize(0)
}
