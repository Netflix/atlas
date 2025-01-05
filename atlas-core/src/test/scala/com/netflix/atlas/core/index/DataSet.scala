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

import java.time.Duration
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.UUID

import com.netflix.atlas.core.model.*

object DataSet {

  private val ec2Bandwidth = Map(
    "m1.xlarge"  -> 1000,
    "m2.xlarge"  -> 300,
    "m2.2xlarge" -> 700,
    "m2.4xlarge" -> 1000,
    "m3.xlarge"  -> 1000,
    "m3.2xlarge" -> 1000,
    "c3.medium"  -> 300,
    "c3.large"   -> 300,
    "c3.xlarge"  -> 700,
    "c3.2xlarge" -> 1000,
    "c3.4xlarge" -> 2000,
    "c3.8xlarge" -> 2000,
    "r3.large"   -> 300,
    "r3.xlarge"  -> 700,
    "r3.2xlarge" -> 1000,
    "i2.large"   -> 300,
    "i2.xlarge"  -> 700,
    "i2.2xlarge" -> 1000,
    "i2.4xlarge" -> 2000,
    "i2.8xlarge" -> 2000
  )

  private def mkTags(
    app: String,
    node: String,
    stack: Option[String],
    version: Option[Int]
  ): Map[String, String] = {
    val cluster = stack match {
      case Some(s) => "%s-%s".format(app, s)
      case None    => app
    }
    val asg = version match {
      case Some(v) => "%s-v%03d".format(cluster, v)
      case None    => cluster
    }
    Map(
      TagKey.application      -> app,
      TagKey.cluster          -> cluster,
      TagKey.autoScalingGroup -> asg,
      TagKey.node             -> node,
      TagKey.legacy           -> "epic"
    )
  }

  /**
    * Some SPS-like wave metrics that have a daily trend.
    */
  def staticSps: List[TimeSeries] = {

    // size, min, max, noise
    val settings = Map(
      "silverlight" -> ((300, 50.0, 300.0, 5.0)),
      "xbox"        -> ((120, 40.0, 220.0, 5.0)),
      "wii"         -> ((111, 20.0, 240.0, 8.0)),
      "ps3"         -> ((220, 40.0, 260.0, 15.0)),
      "appletv"     -> ((10, 3.0, 40.0, 5.0)),
      "psvita"      -> ((3, 0.2, 1.2, 0.6))
    )

    val metrics = settings.flatMap { t =>
      val stack = Some(t._1)
      val conf = t._2
      (0 until conf._1).flatMap { i =>
        val node = "%s-%04x".format(t._1, i)
        val app = mkTags("nccp", node, stack, Some(42))
        val sps = app + ("name" -> "sps")

        val idealF = wave(conf._2, conf._3, Duration.ofDays(1))
        val ideal = idealF.withTags(sps + ("type" -> "ideal"))

        val lowNoiseF = noise(31, conf._4, idealF)
        val lowNoise = lowNoiseF.withTags(sps + ("type" -> "low-noise"))

        val highNoiseF = noise(31, 4.0 * conf._4, idealF)
        val highNoise = highNoiseF.withTags(sps + ("type" -> "high-noise"))

        List(ideal, lowNoise, highNoise).map { m =>
          m.withTags(m.tags + ("type2" -> m.tags("type").toUpperCase))
        }
      }
    }
    metrics.toList
  }

  def noisyWaveSeries: TimeSeries = {
    val idealF = wave(50.0, 300.0, Duration.ofDays(1))
    noise(31, 25.0, idealF)
  }

  def noisyWave: TimeSeries = {
    val noiseF = noisyWaveSeries
    val name = "requestPerSecond"
    val tags = mkTags("alerttest", "alert1", None, Some(42)) + ("name" -> name)
    noiseF.withTags(tags)
  }

  def waveWithOutages: TimeSeries = {
    val start1 = ZonedDateTime.of(2012, 1, 1, 5, 0, 0, 0, ZoneOffset.UTC).toInstant
    val end1 = ZonedDateTime.of(2012, 1, 1, 6, 38, 0, 0, ZoneOffset.UTC).toInstant

    val start2 = ZonedDateTime.of(2012, 2, 1, 7, 4, 0, 0, ZoneOffset.UTC).toInstant
    val end2 = ZonedDateTime.of(2012, 2, 1, 7, 5, 0, 0, ZoneOffset.UTC).toInstant

    val bad = constant(0)
    val ds1 = interval(noisyWaveSeries, bad, start1.toEpochMilli, end1.toEpochMilli)
    val ds2 = interval(ds1, bad, start2.toEpochMilli, end2.toEpochMilli)

    val name = "name" -> "requestsPerSecond"
    val tags = mkTags("alerttest", "alert1", None, Some(42)) + name
    ds2.withTags(tags)
  }

  def cpuSpikes: TimeSeries = {
    val start1 = ZonedDateTime.of(2012, 1, 1, 5, 0, 0, 0, ZoneOffset.UTC).toInstant

    val start2 = ZonedDateTime.of(2012, 2, 1, 7, 4, 0, 0, ZoneOffset.UTC).toInstant
    val end2 = ZonedDateTime.of(2012, 2, 1, 7, 5, 0, 0, ZoneOffset.UTC).toInstant

    val normal = noise(31, 4.0, constant(20))
    val bad = noise(31, 20.0, constant(80))
    val ds1 = interval(normal, bad, start1.toEpochMilli, end2.toEpochMilli)
    val ds2 = interval(ds1, bad, start2.toEpochMilli, end2.toEpochMilli)

    val name = "name" -> "ssCpuUser"
    val tags = mkTags("alerttest", "alert1", None, Some(42)) + name
    ds2.withTags(tags)
  }

  def discoveryStatusUp: TimeSeries = {
    val start1 = ZonedDateTime.of(2012, 1, 1, 5, 0, 0, 0, ZoneOffset.UTC).toInstant
    val end1 = ZonedDateTime.of(2012, 1, 1, 6, 0, 0, 0, ZoneOffset.UTC).toInstant

    val normal = constant(1)
    val bad = constant(0)
    val ds = interval(normal, bad, start1.toEpochMilli, end1.toEpochMilli)

    val name = "name" -> "DiscoveryStatus_UP"
    val tags = mkTags("alerttest", "alert1", None, Some(42)) + name
    ds.withTags(tags)
  }

  def discoveryStatusDown: TimeSeries = {
    val start1 = ZonedDateTime.of(2012, 1, 1, 5, 0, 0, 0, ZoneOffset.UTC).toInstant
    val end1 = ZonedDateTime.of(2012, 1, 1, 6, 0, 0, 0, ZoneOffset.UTC).toInstant

    val normal = constant(0)
    val bad = constant(1)
    val ds = interval(normal, bad, start1.toEpochMilli, end1.toEpochMilli)

    val name = "name" -> "DiscoveryStatus_DOWN"
    val tags = mkTags("alerttest", "alert1", None, Some(42)) + name
    ds.withTags(tags)
  }

  val step = 60000

  def constant(v: Double): TimeSeries = {
    TimeSeries(Map("name" -> v.toString), new FunctionTimeSeq(DsType.Gauge, step, _ => v))
  }

  def noise(size: Int, noise: Double, series: TimeSeries): TimeSeries = {

    // A fixed set of random offsets that will get applied to values from the
    // wrapped time series.
    val offsets = {
      val r = new java.util.Random(42)
      Array.fill(size) {
        val v = noise * r.nextDouble()
        if (r.nextBoolean()) v else -1.0 * v
      }
    }

    def f(t: Long): Double = {
      val i = (t % size).toInt
      val offset = offsets(i)
      val v = series.data(t) + offset
      if (v < 0.0) 0.0 else v
    }
    TimeSeries(Map("name" -> "noise"), new FunctionTimeSeq(DsType.Gauge, step, f))
  }

  def wave(min: Double, max: Double, wavelength: Duration): TimeSeries = {
    val lambda = 2 * scala.math.Pi / wavelength.toMillis

    def f(t: Long): Double = {
      val amp = (max - min) / 2.0
      val yoffset = min + amp
      amp * scala.math.sin(t * lambda) + yoffset
    }
    TimeSeries(Map("name" -> "wave"), new FunctionTimeSeq(DsType.Gauge, step, f))
  }

  def interval(ts1: TimeSeries, ts2: TimeSeries, s: Long, e: Long): TimeSeries = {

    def f(t: Long): Double = {
      val ts = if (t >= s && t < e) ts2 else ts1
      ts.data(t)
    }
    TimeSeries(Map("name" -> "interval"), new FunctionTimeSeq(DsType.Gauge, step, f))
  }

  def finegrainWave(min: Int, max: Int, hours: Int): TimeSeries = {
    wave(min, max, Duration.ofHours(hours))
  }

  def simpleWave(min: Int, max: Int): TimeSeries = {
    wave(min, max, Duration.ofDays(1))
  }

  def simpleWave(max: Int): TimeSeries = {
    simpleWave(0, max)
  }

  /**
    * Some metrics with problems that are used to test alerting.
    */
  def staticAlertSet: List[TimeSeries] = {
    smallStaticSet ::: List(waveWithOutages, cpuSpikes, discoveryStatusUp, discoveryStatusDown)
  }

  /**
    * Returns a static list of metrics that can be used as a test set.
    */
  def smallStaticSet: List[TimeSeries] = staticSps

  /**
    * Returns static list with 100k metrics per cluster.
    */
  def largeStaticSet(n: Int): List[TimeSeries] = {

    // size, min, max, noise
    val settings = Map(
      "silverlight" -> ((300, 50.0, 300.0, 5.0)),
      "xbox"        -> ((120, 40.0, 220.0, 5.0)),
      "wii"         -> ((111, 20.0, 240.0, 8.0)),
      "ps3"         -> ((220, 40.0, 260.0, 15.0)),
      "appletv"     -> ((10, 3.0, 40.0, 5.0)),
      "psvita"      -> ((3, 0.2, 1.2, 0.6))
    )

    val metrics = settings.flatMap { t =>
      val stack = Some(t._1)
      val conf = t._2
      (0 until conf._1).flatMap { i =>
        val node = "%s-%04x".format(t._1, i)
        val app = mkTags("nccp", node, stack, Some(42))
        (0 until n).map { j =>
          val sps = app + ("name" -> ("sps_" + j))

          val idealF = wave(conf._2, conf._3, Duration.ofDays(1))
          idealF.withTags(sps ++ Seq("type" -> "ideal", "type2" -> "IDEAL"))
        }
      }
    }
    metrics.toList
  }

  /**
    * Returns static list with legacy metrics, only have a cluster and large names.
    */
  def largeLegacySet(n: Int): List[TimeSeries] = {
    val metrics = (0 until n).map { _ =>
      val name = UUID.randomUUID.toString
      val tags = Map("nf.cluster" -> "silverlight", "name" -> name)
      val idealF = wave(50.0, 300.0, Duration.ofDays(1))
      idealF.withTags(tags)
    }
    metrics.toList
  }

  def constants: List[TimeSeries] = {
    val metrics = ec2Bandwidth.map {
      case (vmtype, bandwidth) =>
        val tags = Map("name" -> "ec2.networkBandwidth", "nf.vmtype" -> vmtype)
        constant(bandwidth).withTags(tags)
    }
    metrics.toList
  }

  /**
    * Returns a data set with a given name.
    */
  def get(name: String): List[TimeSeries] = name match {
    case "alert"     => staticAlertSet
    case "small"     => smallStaticSet
    case "sps"       => staticSps
    case "constants" => constants
    case _           => throw new NoSuchElementException(name)
  }
}
