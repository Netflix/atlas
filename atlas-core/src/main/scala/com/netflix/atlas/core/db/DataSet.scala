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
package com.netflix.atlas.core.db

import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime

import com.netflix.atlas.core.model.*
import com.netflix.atlas.core.util.TimeWave
import com.netflix.spectator.api.histogram.PercentileBuckets

private[db] object DataSet {

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
      TagKey.node             -> node
    )
  }

  /**
    * Some SPS-like wave metrics that have a daily trend.
    */
  def staticSps: List[TimeSeries] = {

    // stack -> size, min, max, noise
    val settings = Map(
      "silverlight" -> ((30, 50.0, 300.0, 5.0)),
      "xbox"        -> ((12, 40.0, 220.0, 5.0)),
      "wii"         -> ((11, 20.0, 240.0, 8.0)),
      "ps3"         -> ((22, 40.0, 260.0, 15.0)),
      "appletv"     -> ((5, 3.0, 40.0, 5.0)),
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

  /**
    * Some SPS-like wave metrics modeled like a timer in spectator.
    */
  def staticSpsTimer: List[TimeSeries] = {

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
        val sps = app + ("name" -> "playback.startLatency")

        val idealF = wave(conf._2, conf._3, Duration.ofDays(1))
        val highNoiseF = noise(31, 4.0 * conf._4, idealF)
        val highNoise = highNoiseF.withTags(sps + ("statistic" -> "count"))

        val exists = constant(1.0).withTags(app + ("name" -> "poller.asg.instance"))

        val isUp = if (i % 2 == 0) 1.0 else 0.0
        val up = constant(isUp).withTags(app + ("name" -> "DiscoveryStatus_nccp_UP"))
        exists :: up :: statistics(0.25, highNoise)
      }
    }
    metrics.toList
  }

  def statistics(maxValue: Double, series: TimeSeries): List[TimeSeries] = {

    // A fixed set of random offsets that will get applied to values from the
    // wrapped time series.
    val size = 41
    val offsets = {
      val r = new java.util.Random(series.tags("nf.node").hashCode)
      Array.fill(size) { maxValue * math.abs(r.nextGaussian()) }
    }

    def total(t: Long): Double = series.data(t) * offsets((t % size).toInt)

    def totalOfSquares(t: Long): Double = series.data(t) * offsets((t % size).toInt)

    def max(t: Long): Double = offsets((t % size).toInt)

    def stat(name: String, f: Long => Double): TimeSeries = {
      TimeSeries(series.tags + ("statistic" -> name), new FunctionTimeSeq(DsType.Gauge, step, f))
    }

    List(
      series,
      stat("totalTime", total),
      stat("totalOfSquares", totalOfSquares),
      stat("max", max)
    )
  }

  def percentiles(
    name: String,
    start: Instant,
    end: Instant,
    series: List[TimeSeries]
  ): List[TimeSeries] = {
    var usedBuckets = Set.empty[Int]
    series.foreach(_.data.foreach(start.toEpochMilli, end.toEpochMilli) { (_, d) =>
      usedBuckets += PercentileBuckets.indexOf(d.toLong)
    })

    val rate = 1.0d / 60

    def counts(idx: Int): Long => Double =
      ts => rate * series.count(s => PercentileBuckets.indexOf(s.data(ts).toLong) == idx)

    def bucketSeries(bucket: Int): TimeSeries =
      TimeSeries(
        Map("name" -> name, TagKey.percentile -> f"D$bucket%04X"),
        new FunctionTimeSeq(DsType.Gauge, step, counts(bucket))
      )

    usedBuckets.map(bucketSeries).toList
  }

  def noisyWaveSeries: TimeSeries = {
    val idealF = wave(50.0, 300.0, Duration.ofDays(1))
    noise(31, 25.0, idealF)
  }

  def noisyWaveSeries2h: TimeSeries = {
    val idealF = wave(50.0, 300.0, Duration.ofMinutes(137))
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

    val start3 = ZonedDateTime.of(2012, 1, 2, 4, 22, 0, 0, ZoneOffset.UTC).toInstant
    val end3 = ZonedDateTime.of(2012, 1, 2, 6, 0, 0, 0, ZoneOffset.UTC).toInstant

    val input = noisyWaveSeries
    val bad = constant(0)
    val ds1 = interval(input, bad, start1.toEpochMilli, end1.toEpochMilli)
    val ds2 = interval(ds1, bad, start2.toEpochMilli, end2.toEpochMilli)
    val ds3 = interval(ds2, noisyWaveSeries2h, start3.toEpochMilli, end3.toEpochMilli)

    val name = "name" -> "requestsPerSecond"
    val tags = mkTags("alerttest", "alert1", None, Some(42)) + name
    ds3.withTags(tags)
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

  def requestLatency: List[TimeSeries] = {
    val start = ZonedDateTime.of(2012, 1, 1, 5, 0, 0, 0, ZoneOffset.UTC).toInstant
    val end = ZonedDateTime.of(2012, 2, 1, 7, 5, 0, 0, ZoneOffset.UTC).toInstant
    val name = "name" -> "requestLatency"

    // size, min, max, noise
    val settings = Map(
      "silverlight" -> ((300, 500.0, 600.0, 5.0)),
      "xbox"        -> ((120, 400.0, 520.0, 5.0)),
      "wii"         -> ((111, 200.0, 440.0, 8.0))
    )

    val metrics = settings.toList.map {
      case (stack, conf) =>
        val (size, _, max, noiseFactor) = conf
        val tags = mkTags("nccp", s"$stack-node", Some(stack), Some(42)) + name
        noise(size, noiseFactor, constant(max)).withTags(tags)
    }

    metrics ++ percentiles("requestLatency", start, end, metrics)
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

  // For the sample data sets it doesn't matter much what the step size is, just use
  // a minute
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
    val sin = TimeWave.get(wavelength, step)

    def f(t: Long): Double = {
      val amp = (max - min) / 2.0
      val yoffset = min + amp
      amp * sin(t) + yoffset
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

  /**
    * Some metrics with problems that are used to test alerting.
    */
  def staticAlertSet: List[TimeSeries] = {
    smallStaticSet ::: staticSpsTimer ::: requestLatency ::: List(
      waveWithOutages,
      cpuSpikes,
      discoveryStatusUp,
      discoveryStatusDown
    )
  }

  /**
    * Returns a static list of metrics that can be used as a test set.
    */
  def smallStaticSet: List[TimeSeries] = staticSps

  /**
    * Returns a data set with a given name.
    */
  def get(name: String): List[TimeSeries] = name match {
    case "alert" => staticAlertSet
    case "small" => smallStaticSet
    case _       => throw new NoSuchElementException(name)
  }
}
