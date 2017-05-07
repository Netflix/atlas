/*
 * Copyright 2014-2017 Netflix, Inc.
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

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.eval.model.ArrayData
import com.netflix.atlas.eval.model.TimeGroup
import com.netflix.atlas.eval.model.TimeSeriesMessage
import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class DatapointEvalSuite extends FunSuite {

  implicit val system = ActorSystem(getClass.getSimpleName)
  implicit val materializer = ActorMaterializer()

  private val step = 60000

  private def createGroup(expr: DataExpr, t: Long, nodes: Int): TimeGroup[Datapoint] = {
    val datapoints = (0 until nodes).toList.map { i =>
      val node = f"i-$i%08d"
      val tags = Map("name" -> "cpu")
      if (expr.isGrouped)
        Datapoint(tags + ("node" -> node), t, i, step)
      else
        Datapoint(tags, t, i, step)
    }
    TimeGroup(t, datapoints)
  }

  private def createDataSet(expr: DataExpr, window: Int, nodes: Int): List[TimeGroup[Datapoint]] = {
    (0 until window).toList.map { i =>
      createGroup(expr, i * step, nodes)
    }
  }

  private def eval(expr: DataExpr, dataSet: List[TimeGroup[Datapoint]]): List[List[TimeSeriesMessage]] = {
    val styleExpr = StyleExpr(expr, Map.empty)
    val future = Source(dataSet)
      .via(new DatapointEval(styleExpr, step))
      .runWith(Sink.seq[List[TimeSeriesMessage]])
    Await.result(future, Duration.Inf).toList
  }

  test("eval sum") {
    val expr = DataExpr.Sum(Query.True)
    val dataSet = createDataSet(expr, 5, 10)
    val results = eval(expr, dataSet)
    assert(results.size === 5)
    results.foreach { vs =>
      assert(vs.size === 1)
      vs.foreach { case TimeSeriesMessage(_, _, _, _, _, _, _, ArrayData(values)) =>
        assert(values === Array(45.0))
      }
    }
  }

  test("eval max") {
    val expr = DataExpr.Max(Query.True)
    val dataSet = createDataSet(expr, 5, 10)
    val results = eval(expr, dataSet)
    assert(results.size === 5)
    results.foreach { vs =>
      assert(vs.size === 1)
      vs.foreach { case TimeSeriesMessage(_, _, _, _, _, _, _, ArrayData(values)) =>
        assert(values === Array(9.0))
      }
    }
  }

  test("eval by") {
    val expr = DataExpr.GroupBy(DataExpr.Max(Query.True), List("node"))
    val dataSet = createDataSet(expr, 5, 10)
    val results = eval(expr, dataSet)
    assert(results.size === 5)
    results.foreach { vs =>
      assert(vs.size === 10)
      vs.foreach { case TimeSeriesMessage(_, _, _, _, _, _, tags, ArrayData(values)) =>
        val v = tags("node").substring(2).toDouble
        assert(values === Array(v))
      }
    }
  }

}

