/*
 * Copyright 2014-2026 Netflix, Inc.
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

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import com.netflix.atlas.core.model.*
import com.netflix.atlas.core.util.TimeWave
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.Utils
import com.netflix.spectator.api.histogram.PercentileBuckets
import com.netflix.spectator.api.patterns.DistinctCountSketch

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

  /**
    * Concurrent viewers for a simulated live streaming event, published the way a
    * [[com.netflix.spectator.api.patterns.DistinctCountSketch]] would publish it: one max gauge
    * per register, tagged with `statistic=distinct` and a `distinct=R##` register id. Use with
    * `:approx-distinct` to get the number of concurrent viewers, or
    * `:approx-distinct-cumulative` for the number of unique viewers so far.
    *
    * The event runs once a day starting at [[eventStartHour]]:00 UTC:
    *
    *   - 30m ramp up to 1M concurrent viewers
    *   - 2h holding at 1M, with 5% of the audience replaced every 15m
    *   - 30m climb to 2M for the main event
    *   - 30m holding at 2M
    *   - 10m drop back to the baseline as the event ends
    *
    * Because viewers churn, the number of unique viewers over the event is larger than the
    * number watching at any one time, which is the difference between the two operators.
    */
  def liveEventViewers: List[TimeSeries] = {
    val registers = DistinctCountSketch.REGISTERS
    viewerDevices.flatMap {
      case (device, _, _) =>
        (0 until registers).map { r =>
          def f(t: Long): Double = viewerRegisters(device, t)(r)
          val tags = Map(
            TagKey.application -> "streaming",
            "name"             -> "viewers.concurrent",
            "device"           -> device,
            "statistic"        -> "distinct",
            TagKey.distinct    -> "R%02X".format(r)
          )
          TimeSeries(tags, new FunctionTimeSeq(DsType.Gauge, step, f))
        }
    }
  }

  /** Hour of the day, UTC, at which the simulated event starts. */
  private val eventStartHour = 20

  /**
    * Devices the audience is split across, the share of the audience on each, and the id the
    * device's viewers are numbered from.
    *
    * The estimate a sketch produces depends on how the particular set of ids happens to hash.
    * With 64 registers that is worth more than 10% either way for a given set, enough that
    * starting at zero shows the two million viewer peak as 2.3M and makes the shares of the
    * devices look wrong relative to each other. The bases below are not special beyond being
    * ones where the estimates land close to the intended figures, within about 5%, so the
    * sample data illustrates the operators rather than the sampling error.
    */
  private val viewerDevices = List(
    ("tv", 0.55, 270000000L),
    ("phone", 0.30, 240000000L),
    ("laptop", 0.15, 160000000L)
  )

  private val viewerShares: Map[String, Double] =
    viewerDevices.map(d => d._1 -> d._2).toMap

  private val peakViewers = 2000000L
  private val plateauViewers = 1000000L

  /** Viewers always watching, so the metric is not empty outside of the event. */
  private val baselineViewers = 60000L

  /** Number of ids covered by one pre-computed set of registers. */
  private val viewerBlockSize = 1000

  private val churnSteps = 8
  private val churnStepSize = plateauViewers / 20 // 5% of the plateau audience

  // Ranges are kept on block boundaries so a range is always an exact set of blocks. Without
  // that the partial blocks at each end get merged whole and a small range, such as the
  // baseline audience, ends up counting the ids on either side of it as well.
  private def scaled(v: Long, share: Double): Long = {
    math.round(v * share / viewerBlockSize) * viewerBlockSize
  }

  // Each device numbers its viewers from its own base, so the ranges never overlap and the
  // ungrouped estimate is the union of the devices. The event churns through more ids than are
  // ever watching at once, so a device's space has to cover the peak plus everything the churn
  // retires, with the baseline audience on top of that. The parts are scaled and summed the
  // same way `watching` computes them; scaling the sum instead would round differently and
  // leave the event overlapping the baseline.
  private val viewerIdsPerDevice: Map[String, Long] = {
    viewerDevices.map {
      case (device, share, _) =>
        device -> (scaled(peakViewers, share) +
          churnSteps * scaled(churnStepSize, share) +
          scaled(baselineViewers, share))
    }.toMap
  }

  /**
    * Registers for a range of ids, built by recording them into a real sketch. Sketches merge
    * by taking the max of each register, so the registers for a set of ids can be built from
    * the registers of any partition of it. That is what lets the ranges below be assembled
    * from pre-computed blocks rather than re-recording millions of ids for every interval.
    */
  private def sketchRegisters(base: Long, lo: Long, hi: Long): Array[Double] = {
    val registry = new DefaultRegistry()
    val sketch = DistinctCountSketch.get(registry, registry.createId("block"))
    var i = lo
    while (i < hi) {
      sketch.record(base + i)
      i += 1
    }
    val regs = new Array[Double](DistinctCountSketch.REGISTERS)
    registry.gauges.forEach { g =>
      val id = Utils.getTagValue(g.id, TagKey.distinct)
      if (id != null) regs(Integer.parseInt(id.substring(1), 16)) = g.value()
    }
    regs
  }

  private lazy val viewerBlocks: Map[String, Array[Array[Double]]] = {
    viewerDevices.map {
      case (device, _, base) =>
        val blocks = (viewerIdsPerDevice(device) / viewerBlockSize).toInt
        device -> Array.tabulate(blocks) { b =>
          sketchRegisters(base, b.toLong * viewerBlockSize, (b + 1).toLong * viewerBlockSize)
        }
    }.toMap
  }

  /** Merge the registers for the blocks covering `[lo, hi)` of the device's id space. */
  private def mergeBlocks(into: Array[Double], device: String, lo: Long, hi: Long): Unit = {
    if (hi > lo) {
      val blocks = viewerBlocks(device)
      // Only blocks fully inside the range are merged, so an unaligned bound rounds the range
      // in rather than pulling in ids that are not watching.
      val first = ((lo + viewerBlockSize - 1) / viewerBlockSize).toInt
      val last = (hi / viewerBlockSize).toInt - 1
      require(last < blocks.length, s"$device range [$lo, $hi) is outside the id space")
      var b = first
      while (b <= last) {
        val regs = blocks(b)
        var i = 0
        while (i < regs.length) {
          if (regs(i) > into(i)) into(i) = regs(i)
          i += 1
        }
        b += 1
      }
    }
  }

  /**
    * Ids watching at time `t`, as an offset range within the device's id space. Ids are handed
    * out in order, so `hi` is everyone who has ever joined and `lo` advances as the churn
    * retires the earliest arrivals.
    */
  private def watching(share: Double, t: Long): (Long, Long) = {
    val minutes = (t % 86400000L - eventStartHour * 3600000L).toDouble / 60000.0
    val plateau = scaled(plateauViewers, share)
    val peak = scaled(peakViewers, share)
    val stepSize = scaled(churnStepSize, share)

    if (minutes < 0 || minutes >= 220) {
      // Outside of the event, only the baseline audience is watching.
      (0L, 0L)
    } else if (minutes < 30) {
      (0L, math.round(plateau * minutes / 30.0))
    } else if (minutes < 150) {
      // Holding at the plateau, replacing a slice of the audience every 15 minutes.
      val completed = ((minutes - 30) / 15).toInt
      val retired = stepSize * completed
      (retired, plateau + retired)
    } else {
      val retired = stepSize * churnSteps
      if (minutes < 180) {
        // Climbing to the peak for the main event.
        val extra = (peak - plateau) * (minutes - 150) / 30.0
        (retired, plateau + retired + math.round(extra))
      } else if (minutes < 210) {
        (retired, peak + retired)
      } else {
        // Event over, everyone leaves within ten minutes. The event audience drains all the
        // way out: the baseline audience is a separate range that is always merged in, so
        // leaving a baseline sized slice of the event behind would count it twice.
        val left = math.round(peak * (minutes - 210) / 10.0)
        (math.min(retired + left, peak + retired), peak + retired)
      }
    }
  }

  /**
    * Registers for the audience that is always watching. The baseline audience sits at the end
    * of the device's id space so the event can churn through the rest without retiring it, and
    * it does not vary with time, so it is merged once per device rather than per interval.
    */
  private lazy val viewerBaselineRegisters: Map[String, Array[Double]] = {
    viewerDevices.map {
      case (device, share, _) =>
        val size = viewerIdsPerDevice(device)
        val regs = new Array[Double](DistinctCountSketch.REGISTERS)
        mergeBlocks(regs, device, size - scaled(baselineViewers, share), size)
        device -> regs
    }.toMap
  }

  private def viewerRegisters(device: String, t: Long): Array[Double] = {
    val share = viewerShares(device)
    val (lo, hi) = watching(share, t)
    if (hi <= lo) {
      // Outside of the event nothing beyond the baseline is watching, so there is nothing to
      // merge and nothing worth caching.
      viewerBaselineRegisters(device)
    } else {
      viewerRegisterCache.get(
        device -> t,
        _ => {
          val regs = viewerBaselineRegisters(device).clone()
          mergeBlocks(regs, device, lo, hi)
          regs
        }
      )
    }
  }

  // Every register of a device is asked for the same set of intervals in turn, so the merged
  // registers for an interval are worth keeping rather than rebuilding them 64 times. The
  // series are shared across concurrent requests, so this has to be a thread safe cache.
  private val viewerRegisterCache: Cache[(String, Long), Array[Double]] = Caffeine
    .newBuilder()
    .maximumSize(16384)
    .build[(String, Long), Array[Double]]()

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
    smallStaticSet ::: staticSpsTimer ::: requestLatency ::: liveEventViewers ::: List(
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
