/*
 * Copyright 2014-2021 Netflix, Inc.
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
package com.netflix.atlas.core.model

import com.netflix.atlas.core.util.IdentityMap

/**
  * Evaluator that can be used to incrementally process compute the final results
  * with a bounded memory use. Input data must be supplied in order based on the
  * keys in the `orderBy` set. Stateful operations will have to maintain state and
  * thus will result in a much higher memory usage.
  */
trait IncrementalEvaluator {

  /**
    * Set of keys in the order needed for evaluation.
    */
  def orderBy: List[String]

  /**
    * Update the evaluation with a time series that is associated with the specified
    * data expression. Any results that are already complete will be returned.
    */
  def update(dataExpr: DataExpr, ts: TimeSeries): List[TimeSeries]

  /**
    * When the input has been fully processed, flush should be called to get the final
    * results for the evaluation.
    */
  def flush(): List[TimeSeries]

  /**
    * Expression state resulting from the evaluation.
    */
  def state: Map[StatefulExpr, Any]

  /**
    * Return a new evaluator that will apply the mapping function to the output of
    * this evaluator.
    */
  def map(f: TimeSeries => TimeSeries): IncrementalEvaluator = {
    val self = this
    new IncrementalEvaluator {
      override def orderBy: List[String] = self.orderBy

      override def update(dataExpr: DataExpr, ts: TimeSeries): List[TimeSeries] = {
        self.update(dataExpr, ts).map(f)
      }

      override def flush(): List[TimeSeries] = {
        self.flush().map(f)
      }

      override def state: Map[StatefulExpr, Any] = self.state
    }
  }
}

object IncrementalEvaluator {

  /** Create a simple evaluator for a static set of data. */
  def apply(data: List[TimeSeries]): IncrementalEvaluator = {
    new IncrementalEvaluator {
      override def orderBy: List[String] = Nil

      override def update(dataExpr: DataExpr, ts: TimeSeries): List[TimeSeries] = Nil

      override def flush(): List[TimeSeries] = data

      override def state: Map[StatefulExpr, Any] = IdentityMap.empty
    }
  }
}
