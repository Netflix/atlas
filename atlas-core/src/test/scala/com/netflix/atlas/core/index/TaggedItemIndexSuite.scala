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
package com.netflix.atlas.core.index

import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.TimeSeries
import munit.FunSuite

import scala.collection.immutable.ArraySeq

class TaggedItemIndexSuite extends FunSuite {

  import TaggedItemIndexSuite.*

  private val index = TaggedItemIndex(ArraySeq.unsafeWrapArray(dataset))

  private def findItems(query: Query, offset: Int = 0): List[TimeSeries] = {
    val builder = List.newBuilder[TimeSeries]
    val iter = index.find(query, offset).getIntIterator
    while (iter.hasNext) {
      builder += dataset(iter.next())
    }
    builder.result()
  }

  test("true query") {
    val result = findItems(Query.True)
    assertEquals(result.size, dataset.length)
  }

  test("false query") {
    val result = findItems(Query.False)
    assert(result.isEmpty)
  }

  test("equal query") {
    val q = Query.Equal("name", "sps_9")
    val result = findItems(q)
    result.foreach { m =>
      assertEquals(m.tags("name"), "sps_9")
    }
    assertEquals(result.size, 764)
  }

  test("equal query repeat") {
    val q = Query.Equal("name", "sps_9")
    val result = findItems(q)
    result.foreach { m =>
      assertEquals(m.tags("name"), "sps_9")
    }
    assertEquals(result.size, 764)
  }

  test("equal query with offset") {
    val q = Query.Equal("name", "sps_9")
    val offset = 500
    val result = findItems(q, offset)
    result.foreach { m =>
      assert(m.idString > dataset(offset).idString)
    }
  }

  test("equal query with offset repeat") {
    val q = Query.Equal("name", "sps_9")
    val offset = 500
    val result = findItems(q, offset)
    result.foreach { m =>
      assert(m.idString > dataset(offset).idString)
    }
  }

  test("equal query with paging") {
    val q = Query.Equal("name", "sps_9")
    val result = findItems(q)
    val pageSize = 10
    val builder = List.newBuilder[TimeSeries]
    var tmp = findItems(q).take(pageSize)
    while (tmp.size == pageSize) {
      builder ++= tmp
      val offset = dataset.indexOf(tmp.last)
      tmp = findItems(q, offset).take(pageSize)
    }
    builder ++= tmp
    assertEquals(result.size, builder.result().size)
    assertEquals(result, builder.result())
  }

  test("gt query") {
    val q = Query.GreaterThan("name", "sps_4")
    val result = findItems(q)
    result.foreach { m =>
      assert(m.tags("name") > "sps_4")
    }
    assertEquals(result.size, 3820)
  }

  test("ge query") {
    val q = Query.GreaterThanEqual("name", "sps_4")
    val result = findItems(q)
    result.foreach { m =>
      assert(m.tags("name") >= "sps_4")
    }
    assertEquals(result.size, 4584)
  }

  test("lt query") {
    val q = Query.LessThan("name", "sps_5")
    val result = findItems(q)
    result.foreach { m =>
      assert(m.tags("name") < "sps_5")
    }
    assertEquals(result.size, 3820)
  }

  test("le query") {
    val q = Query.LessThanEqual("name", "sps_5")
    val result = findItems(q)
    result.foreach { m =>
      assert(m.tags("name") <= "sps_5")
    }
    assertEquals(result.size, 4584)
  }

  test("in query") {
    val q = Query.In("name", List("sps_5", "sps_7"))
    val result = findItems(q)
    result.foreach { m =>
      assert(m.tags("name") == "sps_5" || m.tags("name") == "sps_7")
    }
    assertEquals(result.size, 1528)
  }

  test("regex query, prefix") {
    val q = Query.Regex("nf.cluster", "^nccp-silver.*")
    val result = findItems(q)
    result.foreach { m =>
      assertEquals(m.tags("nf.cluster"), "nccp-silverlight")
    }
    assertEquals(result.size, 3000)
  }

  test("regex query, index of") {
    val q = Query.Regex("nf.cluster", ".*silver.*")
    val result = findItems(q)
    result.foreach { m =>
      assertEquals(m.tags("nf.cluster"), "nccp-silverlight")
    }
    assertEquals(result.size, 3000)
  }

  test("regex query, case insensitive index of") {
    val q = Query.RegexIgnoreCase("type2", ".*dea.*")
    val result = findItems(q)
    result.foreach { m =>
      assertEquals(m.tags("type2"), "IDEAL")
    }
    assertEquals(result.size, 7640)
  }

  test("haskey query") {
    val q = Query.HasKey("nf.cluster")
    val result = findItems(q)
    assertEquals(result.size, 7640)
  }

  test("and query") {
    val q1 = Query.Equal("name", "sps_9")
    val q2 = Query.Regex("nf.cluster", "^nccp-silver.*")
    val result = findItems(Query.And(q1, q2))
    result.foreach { m =>
      assertEquals(m.tags("name"), "sps_9")
      assertEquals(m.tags("nf.cluster"), "nccp-silverlight")
    }
    assertEquals(result.size, 300)
  }

  test("and query: substring") {
    val q1 = Query.Equal("name", "sps_9")
    val q2 = Query.Regex("nf.cluster", ".*silver.*")
    val result = findItems(Query.And(q1, q2))
    result.foreach { m =>
      assertEquals(m.tags("name"), "sps_9")
      assertEquals(m.tags("nf.cluster"), "nccp-silverlight")
    }
    assertEquals(result.size, 300)
  }

  test("or query") {
    val q1 = Query.Equal("name", "sps_9")
    val q2 = Query.Regex("nf.cluster", "^nccp-silver.*")
    val result = findItems(Query.Or(q1, q2))
    result.foreach { m =>
      assert(m.tags("name") == "sps_9" || m.tags("nf.cluster") == "nccp-silverlight")
    }
    assertEquals(result.size, 3464)
  }

  test("not query") {
    val q = Query.Regex("nf.cluster", "^nccp-silver.*")
    val result = findItems(Query.Not(q))
    result.foreach { m =>
      assert(m.tags("nf.cluster") != "nccp-silverlight")
    }
    assertEquals(result.size, 4640)
  }

  test("not query: CLDMTA-863") {
    val q = Query.Equal("nf.cluster", "nccp-silverlight")
    val result = findItems(Query.Not(q))
    result.foreach { m =>
      assert(m.tags("nf.cluster") != "nccp-silverlight")
    }
    assertEquals(result.size, 4640)
  }
}

object TaggedItemIndexSuite {

  val dataset: Array[TimeSeries] = DataSet
    .largeStaticSet(10)
    .sortWith(_.idString < _.idString)
    .toArray
}
