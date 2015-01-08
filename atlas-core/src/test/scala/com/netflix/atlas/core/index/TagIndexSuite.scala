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
package com.netflix.atlas.core.index

import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.Tag
import com.netflix.atlas.core.model._
import org.scalatest.FunSuite

object TagIndexSuite {
  val dataset: List[TimeSeries] = DataSet.largeStaticSet(10)
}

abstract class TagIndexSuite extends FunSuite {

  def index: TagIndex[TimeSeries]

  test("size") {
    assert(index.size === 7640)
  }

  test("findTags all") {
    val result = index.findTags(TagQuery(None)).map(_.copy(count = -1))
    assert(result.find(_.key == "atlas.legacy") === Some(Tag("atlas.legacy", "epic")))
    assert(result.find(_.value == "nccp-appletv") === Some(Tag("nf.cluster", "nccp-appletv")))
    assert(result.find(_.key == "type") === Some(Tag("type", "ideal")))
    assert(result.size === 790)
  }

  test("findTags all repeat - might be cached") {
    val result = index.findTags(TagQuery(None)).map(_.copy(count = -1))
    assert(result.find(_.key == "atlas.legacy") === Some(Tag("atlas.legacy", "epic")))
    assert(result.find(_.value == "nccp-appletv") === Some(Tag("nf.cluster", "nccp-appletv")))
    assert(result.find(_.key == "type") === Some(Tag("type", "ideal")))
    assert(result.size === 790)
  }

  test("findTags all with paging") {
    val expected = index.findTags(TagQuery(None)).map(_.copy(count = -1))
    val pageSize = 10
    val builder = List.newBuilder[Tag]
    var tmp = index.findTags(TagQuery(None, limit = pageSize)).map(_.copy(count = -1))
    while (tmp.size == pageSize) {
      builder ++= tmp
      val last = "%s,%s".format(tmp.last.key, tmp.last.value)
      tmp = index.findTags(TagQuery(None, offset = last, limit = pageSize)).map(_.copy(count = -1))
    }
    builder ++= tmp
    assert(expected.size === builder.result.size)
    assert(expected === builder.result)
  }

  test("findTags all with key restriction") {
    val result = index.findTags(TagQuery(None, key = Some("nf.cluster"))).map(_.copy(count = -1))
    assert(result.find(_.value == "nccp-appletv") === Some(Tag("nf.cluster", "nccp-appletv")))
    assert(result.size === 6)
  }

  test("findTags all with key restriction and paging") {
    val expected = index.findTags(TagQuery(None, Some("nf.node"))).map(_.copy(count = -1))
    val pageSize = 10
    val builder = List.newBuilder[Tag]
    var tmp = index.findTags(TagQuery(None, Some("nf.node"), limit = pageSize)).map(_.copy(count = -1))
    while (tmp.size == pageSize) {
      builder ++= tmp
      val last = "%s,%s".format(tmp.last.key, tmp.last.value)
      tmp = index.findTags(TagQuery(None, Some("nf.node"), offset = last, limit = pageSize)).map(_.copy(count = -1))
    }
    builder ++= tmp
    assert(expected.size === builder.result.size)
    assert(expected === builder.result)
  }

  test("findTags query") {
    val q = Query.Equal("name", "sps_9")
    val result = index.findTags(TagQuery(Some(q))).map(_.copy(count = -1))
    assert(result.find(_.key == "atlas.legacy") === Some(Tag("atlas.legacy", "epic")))
    assert(result.find(_.value == "nccp-appletv") === Some(Tag("nf.cluster", "nccp-appletv")))
    assert(result.find(_.key == "type") === Some(Tag("type", "ideal")))
    assert(result.size === 781)
  }

  test("findTags query with key restriction") {
    val q = Query.Equal("name", "sps_9")
    val result = index.findTags(TagQuery(Some(q), key = Some("nf.cluster"))).map(_.copy(count = -1))
    assert(result.find(_.value == "nccp-appletv") === Some(Tag("nf.cluster", "nccp-appletv")))
    assert(result.size === 6)
  }

  test("equal query") {
    val q = Query.Equal("name", "sps_9")
    val result = index.findItems(TagQuery(Some(q)))
    result.foreach { m =>
      assert(m.tags("name") === "sps_9")
    }
    assert(result.size === 764)
  }

  test("equal query repeat") {
    val q = Query.Equal("name", "sps_9")
    val result = index.findItems(TagQuery(Some(q)))
    result.foreach { m =>
      assert(m.tags("name") === "sps_9")
    }
    assert(result.size === 764)
  }

  test("equal query with offset") {
    val q = Query.Equal("name", "sps_9")
    val offset = "a" * 40
    val result = index.findItems(TagQuery(Some(q), offset = offset, limit = 200))
    result.foreach { m =>
      assert(m.idString > offset)
    }
    assert(result.size === 200)
  }

  test("equal query with offset repeat") {
    val q = Query.Equal("name", "sps_9")
    val offset = "a" * 40
    val result = index.findItems(TagQuery(Some(q), offset = offset, limit = 200))
    result.foreach { m =>
      assert(m.idString > offset)
    }
    assert(result.size === 200)
  }

  test("equal query with paging") {
    val q = Query.Equal("name", "sps_9")
    val result = index.findItems(TagQuery(Some(q)))
    val pageSize = 10
    val builder = List.newBuilder[TimeSeries]
    var tmp = index.findItems(TagQuery(Some(q), limit = pageSize))
    while (tmp.size == pageSize) {
      builder ++= tmp
      val last = tmp.last.idString
      tmp = index.findItems(TagQuery(Some(q), offset = last, limit = pageSize))
    }
    builder ++= tmp
    assert(result.size === builder.result.size)
    assert(result === builder.result)
  }

  test("gt query") {
    val q = Query.GreaterThan("name", "sps_4")
    val result = index.findItems(TagQuery(Some(q)))
    result.foreach { m =>
      assert(m.tags("name") > "sps_4")
    }
    assert(result.size === 3820)
  }

  test("ge query") {
    val q = Query.GreaterThanEqual("name", "sps_4")
    val result = index.findItems(TagQuery(Some(q)))
    result.foreach { m =>
      assert(m.tags("name") >= "sps_4")
    }
    assert(result.size === 4584)
  }

  test("lt query") {
    val q = Query.LessThan("name", "sps_5")
    val result = index.findItems(TagQuery(Some(q)))
    result.foreach { m =>
      assert(m.tags("name") < "sps_5")
    }
    assert(result.size === 3820)
  }

  test("le query") {
    val q = Query.LessThanEqual("name", "sps_5")
    val result = index.findItems(TagQuery(Some(q)))
    result.foreach { m =>
      assert(m.tags("name") <= "sps_5")
    }
    assert(result.size === 4584)
  }

  test("in query") {
    val q = Query.In("name", List("sps_5", "sps_7"))
    val result = index.findItems(TagQuery(Some(q)))
    result.foreach { m =>
      assert(m.tags("name") == "sps_5" || m.tags("name") == "sps_7")
    }
    assert(result.size === 1528)
  }

  test("regex query, prefix") {
    val q = Query.Regex("nf.cluster", "^nccp-silver.*")
    val result = index.findItems(TagQuery(Some(q)))
    result.foreach { m =>
      assert(m.tags("nf.cluster") === "nccp-silverlight")
    }
    assert(result.size === 3000)
  }

  test("regex query, index of") {
    val q = Query.Regex("nf.cluster", ".*silver.*")
    val result = index.findItems(TagQuery(Some(q)))
    result.foreach { m =>
      assert(m.tags("nf.cluster") === "nccp-silverlight")
    }
    assert(result.size === 3000)
  }

  test("regex query, case insensitive index of") {
    val q = Query.RegexIgnoreCase("type2", ".*dea.*")
    val result = index.findItems(TagQuery(Some(q)))
    result.foreach { m =>
      assert(m.tags("type2") === "IDEAL")
    }
    assert(result.size === 7640)
  }

  test("haskey query") {
    val q = Query.HasKey("nf.cluster")
    val result = index.findItems(TagQuery(Some(q)))
    assert(result.size === 7640)
  }

  test("and query") {
    val q1 = Query.Equal("name", "sps_9")
    val q2 = Query.Regex("nf.cluster", "^nccp-silver.*")
    val result = index.findItems(TagQuery(Some(Query.And(q1, q2))))
    result.foreach { m =>
      assert(m.tags("name") === "sps_9")
      assert(m.tags("nf.cluster") === "nccp-silverlight")
    }
    assert(result.size === 300)
  }

  test("and query: substring") {
    val q1 = Query.Equal("name", "sps_9")
    val q2 = Query.Regex("nf.cluster", ".*silver.*")
    val result = index.findItems(TagQuery(Some(Query.And(q1, q2))))
    result.foreach { m =>
      assert(m.tags("name") === "sps_9")
      assert(m.tags("nf.cluster") === "nccp-silverlight")
    }
    assert(result.size === 300)
  }

  test("or query") {
    val q1 = Query.Equal("name", "sps_9")
    val q2 = Query.Regex("nf.cluster", "^nccp-silver.*")
    val result = index.findItems(TagQuery(Some(Query.Or(q1, q2))))
    result.foreach { m =>
      assert(m.tags("name") == "sps_9" || m.tags("nf.cluster") == "nccp-silverlight")
    }
    assert(result.size === 3464)
  }

  test("not query") {
    val q = Query.Regex("nf.cluster", "^nccp-silver.*")
    val result = index.findItems(TagQuery(Some(Query.Not(q))))
    result.foreach { m =>
      assert(m.tags("nf.cluster") != "nccp-silverlight")
    }
    assert(result.size === 4640)
  }

  test("not query: CLDMTA-863") {
    val q = Query.Equal("nf.cluster", "nccp-silverlight")
    val result = index.findItems(TagQuery(Some(Query.Not(q))))
    result.foreach { m =>
      assert(m.tags("nf.cluster") != "nccp-silverlight")
    }
    assert(result.size === 4640)
  }
}
