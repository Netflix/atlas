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
package com.netflix.atlas.core.util


import org.scalatest.FunSuite

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

    assert(empty.size === 0)
    assert(m1.size === 1)
    assert(m2.size === 2)

    assert(m1("k1") === "v1")
    intercept[NoSuchElementException] { m1("k2") }
    assert(m2("k1") === "v1")
    assert(m2("k2") === "v2")

    assert(m1.get("k1") === Some("v1"))
    assert(m1.get("k2") === None)
    assert(m2.get("k1") === Some("v1"))
    assert(m2.get("k2") === Some("v2"))

    val s = m2.toSet
    m2.foreach { t => assert(s.contains(t)) }

    assert(m1 === m2 - "k2")
  }

  test("keySet") {
    val m = SmallHashMap("k1" -> "v1", "k2" -> "v2")
    assert(m.keySet === Set("k1", "k2"))
  }

  test("values") {
    val m = SmallHashMap("k1" -> "v1", "k2" -> "v2")
    assert(m.values.toSet === Set("v1", "v2"))
  }

  test("toSet") {
    val m = SmallHashMap("k1" -> "v1", "k2" -> "v2")
    assert(m.toSet === Set("k1" -> "v1", "k2" -> "v2"))
  }

  private def testNumCollisions(m: SmallHashMap[String, String]) {
    //printf("%d: %d collisions, %.2f probes%n", m.size, m.numCollisions, m.numProbesPerKey)
    assert(m.numProbesPerKey < m.size / 4)
  }

  test("numCollisions 25") {
    val rkeys = Random.shuffle(keys)
    val m = SmallHashMap(rkeys.take(25).map(v => v -> v): _*)
    testNumCollisions(m)
    rkeys.take(25).foreach { k => assert(m.get(k) === Some(k)) }
  }

  test("numCollisions 50") {
    val rkeys = Random.shuffle(keys)
    val m = SmallHashMap(rkeys.take(50).map(v => v -> v): _*)
    testNumCollisions(m)
    rkeys.take(50).foreach { k => assert(m.get(k) === Some(k)) }
  }

  test("numCollisions all") {
    val rkeys = Random.shuffle(keys)
    val m = SmallHashMap(rkeys.map(v => v -> v): _*)
    testNumCollisions(m)
    rkeys.foreach { k => assert(m.get(k) === Some(k)) }
  }
}
