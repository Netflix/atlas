/*
 * Copyright 2014-2023 Netflix, Inc.
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
package com.netflix.atlas.core.util

import java.util.UUID

import munit.FunSuite

import scala.util.Random

class SmallHashMapSuite extends FunSuite {

  // Set of keys taken from prod.us-east-1. This tends to be our biggest region and these are the
  // actual keys we see in the data.
  val keys = List(
    "action",
    "app",
    "asn",
    "atlas.dstype",
    "atlas.legacy",
    "aws.dbname",
    "aws.elb",
    "aws.namespace",
    "aws.queue",
    "aws.statistic",
    "aws.topic",
    "backend",
    "bitrate",
    "blobtype",
    "blocked",
    "bucket",
    "cache",
    "cacheId",
    "cacheName",
    "cacheid",
    "callback",
    "caller",
    "category",
    "cdn",
    "cdn.org",
    "cdn.partnerCdn",
    "cdn.routingCluster",
    "cdn.site",
    "cdnId",
    "cdnid",
    "cdnreq",
    "class",
    "clientApp",
    "clientMovieId",
    "clientipver",
    "cluster",
    "codePath",
    "collector",
    "columnfamily",
    "command",
    "contentRegion",
    "contractId",
    "controller",
    "controller.operationalName",
    "controller.rollup",
    "country",
    "currentZone",
    "custom",
    "dao",
    "decision",
    "decisionType",
    "def",
    "detected",
    "device",
    "device.operationalName",
    "device.rollup",
    "device.rollup.2",
    "dialableService",
    "diff",
    "dltype",
    "drmtype",
    "error",
    "errorType",
    "esp",
    "exception",
    "failureLevel",
    "findDeviceKeysNoError",
    "findDeviceKeysPSK",
    "flavor",
    "geoequiv_changed_result",
    "geoequiv_used",
    "id",
    "includeType",
    "ip",
    "keyId",
    "keyMovieid",
    "keySet",
    "keyVersion",
    "keyid",
    "keyspace",
    "languageTag",
    "last7",
    "level",
    "manifestClusterName",
    "manufacturer",
    "maxBitRate",
    "memtype",
    "method",
    "missing",
    "mode",
    "model",
    "module",
    "movieId",
    "name",
    "nas_site",
    "nccprt",
    "newService",
    "nf.ami",
    "nf.app",
    "nf.asg",
    "nf.cluster",
    "nf.country",
    "nf.country.rollup",
    "nf.node",
    "nf.region",
    "nf.vmtype",
    "nf.zone",
    "niwsClientErrorCode",
    "nrdp",
    "op",
    "operation",
    "org",
    "permitted",
    "primary",
    "primary.org",
    "primary.partnerCdn",
    "primary.routingCluster",
    "primary.site",
    "processor",
    "profileId",
    "proto",
    "protocol",
    "provider",
    "quality",
    "reason",
    "recordset",
    "redirectFrom",
    "redirectTo",
    "redirecthn",
    "region",
    "reqhn",
    "request",
    "restClient",
    "result",
    "routeCluster",
    "routeClusterId",
    "selected",
    "selected.isPrimary",
    "selected.org",
    "selected.partnerCdn",
    "selected.routingCluster",
    "selected.site",
    "sequenceCheck",
    "sequenceCheck.deltaValue",
    "service",
    "shard",
    "shouldRedirect",
    "signupCountry",
    "site",
    "source",
    "stat",
    "statistic",
    "status",
    "streamState",
    "streamType",
    "subcounter",
    "success",
    "target",
    "target.operationalName",
    "target.rollup",
    "targetApp",
    "targetCountry",
    "targetZone",
    "testreason",
    "tracks",
    "type",
    "uiver",
    "unit",
    "updateType",
    "viewedText"
  )

  test("basic operations") {
    val empty = SmallHashMap.empty[String, String]
    val m1 = SmallHashMap("k1" -> "v1")
    val m2 = SmallHashMap("k1" -> "v1", "k2" -> "v2")

    assertEquals(empty.size, 0)
    assertEquals(m1.size, 1)
    assertEquals(m2.size, 2)

    assertEquals(m1("k1"), "v1")
    assert(m1.contains("k1"))
    intercept[NoSuchElementException] { m1("k2") }
    assert(!m1.contains("k2"))
    assertEquals(m2("k1"), "v1")
    assertEquals(m2("k2"), "v2")

    assertEquals(m1.get("k1"), Some("v1"))
    assertEquals(m1.get("k2"), None)
    assertEquals(m2.get("k1"), Some("v1"))
    assertEquals(m2.get("k2"), Some("v2"))

    val s = m2.toSet
    m2.foreach { t =>
      assert(s.contains(t))
    }

    assertEquals(m2 - "k2", m1)
  }

  test("retains type after removal of key") {
    val m1 = SmallHashMap("k1" -> "v1")
    val m2 = SmallHashMap("k1" -> "v1", "k2" -> "v2")
    assertEquals(m2 - "k2", m1)
    assert((m2 - "k2").isInstanceOf[SmallHashMap[?, ?]])
  }

  test("remove key not in map") {
    val m1 = SmallHashMap("k1" -> "v1", "k2" -> "v2")
    assertEquals(m1 - "k3", m1)
    assert((m1 - "k3").isInstanceOf[SmallHashMap[?, ?]])
  }

  test("retains type after adding pair") {
    val m1 = SmallHashMap("k1" -> "v1")
    val m2 = SmallHashMap("k1" -> "v1", "k2" -> "v2")
    assertEquals(m1 + ("k2" -> "v2"), m2)
    assert((m1 + ("k2" -> "v2")).isInstanceOf[SmallHashMap[?, ?]])
  }

  test("empty map") {
    val m = SmallHashMap.empty[String, String]
    assertEquals(m.keySet, Set.empty[String])
    assertEquals(m.get("k1"), None)
    assertEquals(m.size, 0)
  }

  test("map with 1 pair") {
    val m = SmallHashMap("k1" -> "v1")
    assertEquals(m.keySet, Set("k1"))
    assertEquals(m.get("k1"), Some("v1"))
    assertEquals(m.get("k2"), None)
    assertEquals(m.size, 1)
  }

  test("keySet") {
    val m = SmallHashMap("k1" -> "v1", "k2" -> "v2")
    assertEquals(m.keySet, Set("k1", "k2"))
  }

  test("values") {
    val m = SmallHashMap("k1" -> "v1", "k2" -> "v2")
    assertEquals(m.values.toSet, Set("v1", "v2"))
  }

  test("toSet") {
    val m = SmallHashMap("k1" -> "v1", "k2" -> "v2")
    assertEquals(m.toSet, Set("k1" -> "v1", "k2" -> "v2"))
  }

  test("using builder") {
    val expected = SmallHashMap("k1" -> "v1", "k2" -> "v2")
    val actual = new SmallHashMap.Builder[String, String](4)
      .add("k1", "v1")
      .addAll(Map("k2" -> "v2"))
      .result
    assertEquals(expected, actual)
  }

  private def testNumCollisions(m: SmallHashMap[String, String]): Unit = {

    // printf("%d: %d collisions, %.2f probes%n", m.size, m.numCollisions, m.numProbesPerKey)
    assert(m.numProbesPerKey < m.size / 4)
  }

  // Search for strings that have the desired hash value and will result in a
  // collision.
  @scala.annotation.tailrec
  private def findStringWithHash(v: Int, n: Int): String = {
    val s = UUID.randomUUID().toString
    val h = Hash.absOrZero(s.hashCode) % n
    if (h == v) s else findStringWithHash(v, n)
  }

  test("numProbesPerKey with collision that is positioned > data length") {
    // empty map, size 0 capacity 42
    val builder = new SmallHashMap.Builder[String, String](42)
    builder.add(findStringWithHash(12, 42), "1")
    builder.add(findStringWithHash(12, 42), "2")
    val m = builder.result
    assertEquals(m.numCollisions, 1)
    assertEquals(m.numProbesPerKey, 0.5)
  }

  test("numCollisions 25") {
    val rkeys = Random.shuffle(keys)
    val m = SmallHashMap(rkeys.take(25).map(v => v -> v)*)
    testNumCollisions(m)
    rkeys.take(25).foreach { k =>
      assertEquals(m.get(k), Some(k))
    }
  }

  test("numCollisions 50") {
    val rkeys = Random.shuffle(keys)
    val m = SmallHashMap(rkeys.take(50).map(v => v -> v)*)
    testNumCollisions(m)
    rkeys.take(50).foreach { k =>
      assertEquals(m.get(k), Some(k))
    }
  }

  test("numCollisions all") {
    val rkeys = Random.shuffle(keys)
    val m = SmallHashMap(rkeys.map(v => v -> v)*)
    testNumCollisions(m)
    rkeys.foreach { k =>
      assertEquals(m.get(k), Some(k))
    }
  }

  test("equals and hashCode, different orders with gaps") {
    (0 until 1000).foreach { i =>
      val n = Random.nextInt(50)
      val data = (0 until n).map { _ =>
        val v = Random.nextInt()
        v.toString -> v.toString
      }
      val m1 = SmallHashMap(100, data.iterator)
      val m2 = SmallHashMap(100, Random.shuffle(data).iterator)
      assertEquals(m1.hashCode, m2.hashCode)
      assertEquals(m1, m2)
    }
  }

  test("equals and hashCode, different orders") {
    (0 until 1000).foreach { i =>
      val n = Random.nextInt(50)
      val data = (0 until n).map { _ =>
        val v = Random.nextInt()
        v.toString -> v.toString
      }
      val m1 = SmallHashMap(data)
      val m2 = SmallHashMap(Random.shuffle(data))
      assertEquals(m1.hashCode, m2.hashCode)
      assertEquals(m1, m2)
    }
  }

  test("equals and hashCode, collisions on random data") {
    val size = 10000
    val naive = new IntHashSet(0)
    val ref = new IntHashSet(0)
    (0 until size).foreach { i =>
      val n = Random.nextInt(50)
      val data = (0 until n).map { _ =>
        val v = Random.nextInt()
        v.toString -> v.toString
      }
      val m = SmallHashMap(Random.shuffle(data))
      naive.add(m.hashCode)
      ref.add(scala.util.hashing.MurmurHash3.mapHash(m))
    }
    check(size, naive.size, ref.size)
  }

  test("equals and hashCode, collisions on realistic data") {
    val size = 10000
    val naive = new IntHashSet(0)
    val ref = new IntHashSet(0)
    (0 until size).foreach { i =>
      val m = SmallHashMap(
        "nf.app"     -> "atlas_backend",
        "nf.cluster" -> "atlas_backend-dev",
        "nf.asg"     -> "atlas_backend-dev-v001",
        "nf.stack"   -> "dev",
        "nf.region"  -> "us-east-1",
        "nf.zone"    -> "us-east-1e",
        "nf.node"    -> f"i-$i%017x",
        "nf.ami"     -> "ami-987654321",
        "nf.vmtype"  -> "r3.2xlarge",
        "name"       -> "jvm.gc.pause",
        "cause"      -> "Allocation_Failure",
        "action"     -> "end_of_major_GC",
        "statistic"  -> "totalTime"
      )
      naive.add(m.hashCode)
      ref.add(scala.util.hashing.MurmurHash3.mapHash(m))
    }
    check(size, naive.size, ref.size)
  }

  // This map seems to do poorly with the naive hash
  test("equals and hashCode, collisions on perf test data") {
    var size = 0
    val naive = new IntHashSet(0)
    val ref = new IntHashSet(0)
    for (i <- 0 until 150; j <- 0 until 1000) {
      size += 1
      val m = SmallHashMap(
        "nf.app"       -> "foo",
        "nf.cluster"   -> "foo-bar",
        "nf.asg"       -> "foo-bar-v000",
        "nf.stack"     -> "bar",
        "nf.region"    -> "us-east-1",
        "nf.zone"      -> "us-east-1a",
        "nf.vmtype"    -> "r3.2xlarge",
        "name"         -> f"test.metric.$j%08x",
        "nf.node"      -> f"$i%017x",
        "atlas.dstype" -> "gauge"
      )
      naive.add(m.hashCode)
      ref.add(scala.util.hashing.MurmurHash3.mapHash(m))
    }
    check(size, naive.size, ref.size)
  }

  private def check(size: Int, naive: Int, ref: Int): Unit = {
    assert(100.0 * ref / size >= 97.0)
    assert(100.0 * naive / size >= 97.0)
  }

  test("dataEquals") {
    val m1 = SmallHashMap("a" -> "1")
    val m2 = SmallHashMap("b" -> "2")
    assert(!m1.dataEquals(m2))
  }

  test("dataEquals with different sizes") {
    // dataEquals is internal and expects the sizes to match before being
    // called. For this test case we are verifying the case where the first
    // item in the two maps are different, but a lookup for the item from
    // the first map will work. However, the lookup of the item from the
    // second map will not.
    val m1 = SmallHashMap("a" -> "1")
    val m2 = SmallHashMap("a" -> "1", "c" -> "3")
    assert(!m1.dataEquals(m2))
  }

  test("javaMap: get") {
    val m = SmallHashMap("a" -> "1", "b" -> "2").asJavaMap
    assertEquals(m.get("a"), "1")
    assertEquals(m.get("b"), "2")
    assertEquals(m.get("c"), null)
  }

  test("javaMap: containsKey") {
    val m = SmallHashMap("a" -> "1", "b" -> "2").asJavaMap
    assert(m.containsKey("a"))
    assert(m.containsKey("b"))
    assert(!m.containsKey("c"))
  }

  test("javaMap: entrySet") {
    val entries = SmallHashMap("a" -> "1", "b" -> "2").asJavaMap.entrySet()
    assertEquals(entries.size(), 2)

    val it = entries.iterator()
    while (it.hasNext) {
      val entry = it.next()
      entry.getKey match {
        case "a" => assertEquals(entry.getValue, "1")
        case "b" => assertEquals(entry.getValue, "2")
      }
    }
  }
}
