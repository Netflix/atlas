/*
 * Copyright 2015 Netflix, Inc.
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
package com.netflix.atlas.core.db

import com.netflix.atlas.core.index.LazyTagIndex
import com.netflix.atlas.core.index.TagIndex
import com.netflix.atlas.core.index.TagQuery
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.DsType
import com.netflix.atlas.core.model.EvalContext
import com.netflix.atlas.core.model.FunctionTimeSeq
import com.netflix.atlas.core.model.TimeSeries
import com.typesafe.config.Config

/**
 * Simple database with a predefined set of time series.
 */
class StaticDatabase(data: List[TimeSeries]) extends Database {

  def this(config: Config) = this(DataSet.get(config.getString("dataset")))

  val index: TagIndex[TimeSeries] = new LazyTagIndex(data.toArray)

  def execute(context: EvalContext, expr: DataExpr): List[TimeSeries] = {
    val q = TagQuery(Some(expr.query))
    val offset = expr.offset.toMillis
    if (offset == 0) expr.eval(context, index.findItems(q)).data else {
      val offsetContext = context.withOffset(expr.offset.toMillis)
      expr.eval(offsetContext, index.findItems(q)).data.map { t =>
        t.offset(offset)
      }
    }
  }
}

object StaticDatabase {
  /** Create a simple database with a range of fixed integer values. Mostly used for testing. */
  def range(s: Int, e: Int): StaticDatabase = {
    val len = e.toString.length
    val ts = (s to e).map { i =>
      val tagsBuilder = Map.newBuilder[String, String]
      tagsBuilder += "name"  -> s"%0${len}d".format(i)
      tagsBuilder += "class" -> (if (i % 2 == 1) "odd" else "even")
      if (probablyPrime(i))
        tagsBuilder += "prime" -> "probably"
      TimeSeries(tagsBuilder.result(), new FunctionTimeSeq(DsType.Gauge, 60000, _ => i))
    }
    new StaticDatabase(ts.toList)
  }

  /** Generate a database with some synthetic data used for demos and examples. */
  def demo: StaticDatabase = new StaticDatabase(DataSet.staticAlertSet)

  private def probablyPrime(v: Int): Boolean = BigInt(v).isProbablePrime(100)
}
