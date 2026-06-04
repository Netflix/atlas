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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import com.netflix.atlas.core.index.BatchUpdateTagIndex
import com.netflix.atlas.core.index.CachingTagIndex
import com.netflix.atlas.core.index.IndexStats
import com.netflix.atlas.core.index.RoaringTagIndex
import com.netflix.atlas.core.index.TagQuery
import com.netflix.atlas.core.model.Block
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.DatapointTuple
import com.netflix.atlas.core.model.DefaultSettings
import com.netflix.atlas.core.model.EvalContext
import com.netflix.atlas.core.model.ItemId
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.TaggedItem
import com.netflix.atlas.core.model.TimeSeries
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.Spectator
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

class MemoryDatabase(registry: Registry, config: Config) extends Database {

  /** How many metrics are being processed for transform queries. */
  private val queryMetrics = registry.counter("atlas.db.queryMetrics")

  /** How many blocks are being processed for transform queries. */
  private val queryBlocks = registry.counter("atlas.db.queryBlocks")

  /** How many blocks are being processed for transform queries. */
  private val aggrBlocks = registry.counter("atlas.db.aggrBlocks")

  /** How many lines are being returned for transform queries. */
  private val queryLines = registry.counter("atlas.db.queryLines")

  /** How many input datapoints are being processed for transform queries. */
  private val queryInputDatapoints = registry.counter("atlas.db.queryInputDatapoints")

  /** How many output datapoints are being processed for transform queries. */
  private val queryOutputDatapoints = registry.counter("atlas.db.queryOutputDatapoints")

  private val step = DefaultSettings.stepSize
  private val blockSize = config.getInt("block-size")
  private val numBlocks = config.getInt("num-blocks")
  private val testMode = config.getBoolean("test-mode")

  @volatile private var filter: Query = Query.True

  private val logger = LoggerFactory.getLogger(getClass)

  private val stats = new IndexStats(registry)

  val index: BatchUpdateTagIndex[BlockStoreItem] =
    new BatchUpdateTagIndex[BlockStoreItem](
      registry,
      { items =>
        new CachingTagIndex(new RoaringTagIndex(items, stats))
      }
    )

  // If the last update time for the index is older than the rebuild age force an update
  private val rebuildAge = config.getDuration("rebuild-frequency", TimeUnit.MILLISECONDS)

  private[db] val data = new ConcurrentHashMap[ItemId, BlockStoreItem]

  private val rebuildThread = new Thread(new RebuildTask, "MemoryDatabaseRebuildIndex")
  if (!testMode) rebuildThread.start()

  private final class RebuildTask extends Runnable {

    def run(): Unit = {
      while (true) {
        try {
          rebuild()
          Thread.sleep(1000)
        } catch {
          case e: Exception =>
            logger.warn("failed to rebuild index", e)
        }
      }
    }
  }

  /** Force an index rebuild if the last one is older than the configured age. */
  def rebuild(): Unit = {
    val now = registry.clock().wallTime()
    if (now - index.buildTime > rebuildAge) rebuildIndex()
  }

  /**
    * Clean up rolled-out stores and rebuild the metadata index from `data`.
    *
    * Index membership is derived directly from `data` -- the authoritative,
    * concurrently-maintained set of stores -- rather than carried independently in
    * the index. Cleaning up first and then building the index from the surviving
    * entries guarantees that a store removed here cannot linger in the index
    * (the orphaned-series symptom), since the index can only ever contain ids that
    * are currently present in `data`.
    */
  private[db] def rebuildIndex(): Unit = {
    val now = registry.clock().wallTime()
    val query = filter
    logger.info("rebuilding metadata index (filter={})", query)

    // Single pass: drop entries whose data has rolled out or that no longer match
    // the filter, and collect the survivors into the array used to rebuild the
    // index. Building the index from the surviving `data` entries is what keeps a
    // removed store from lingering in the index. RoaringTagIndex requires the
    // array sorted by id.
    val windowSize = numBlocks * blockSize * step
    val cutoff = now - windowSize
    val items = new java.util.ArrayList[BlockStoreItem](data.size)
    val iter = data.entrySet.iterator
    while (iter.hasNext) {
      val entry = iter.next()
      val item = entry.getValue
      item.blocks.cleanup(cutoff)
      val kept =
        if (item.blocks.hasData && query.matches(item.tags)) item
        else
          // Re-check under the bin lock so a concurrent update that revived the
          // store (and still matches the filter) is kept rather than removed.
          data.computeIfPresent(
            entry.getKey,
            (_, it) => if (it.blocks.hasData && query.matches(it.tags)) it else null
          )
      if (kept != null) items.add(kept)
    }
    val arr = items.toArray(new Array[BlockStoreItem](0))
    java.util.Arrays.sort(arr, RoaringTagIndex.IdComparator)
    index.rebuildIndex(arr)

    logger.info("done rebuilding metadata index, {} metrics", arr.length)
    TaggedItem.retain(_ > cutoff)
  }

  private def getOrCreateItem(id: ItemId, tags: Map[String, String]): BlockStoreItem = {
    var item = data.get(id)
    if (item == null) {
      item = data.computeIfAbsent(
        id,
        _ => BlockStoreItem.create(id, tags, new MemoryBlockStore(step, blockSize, numBlocks))
      )
    }
    item
  }

  /**
    * Guard against a concurrent `rebuildIndex` cleanup removing this entry between
    * the lookup and the write. The common path is a single lock-free `get` that
    * returns the same item (no-op); the locked `putIfAbsent` runs only in the rare
    * race where the entry was removed, re-inserting the just-written store so the
    * datapoint is not lost. (If a different store was already recreated for the id,
    * that newer store wins and this is a no-op.)
    */
  private def ensurePresent(id: ItemId, item: BlockStoreItem): Unit = {
    if (data.get(id) ne item) data.putIfAbsent(id, item)
  }

  def setFilter(query: Query): Unit = {
    filter = query
  }

  def update(id: ItemId, tags: Map[String, String], timestamp: Long, value: Double): Unit = {
    if (filter.matches(tags)) {
      val item = getOrCreateItem(id, tags)
      item.blocks.update(timestamp, value)
      ensurePresent(id, item)
    }
  }

  def update(dp: DatapointTuple): Unit = {
    update(dp.id, dp.tags, dp.timestamp, dp.value)
  }

  def update(ds: List[DatapointTuple]): Unit = {
    ds.foreach(update)
  }

  def rollup(dp: DatapointTuple): Unit = {
    val item = getOrCreateItem(dp.id, dp.tags)
    item.blocks.update(dp.timestamp, dp.value, rollup = true)
    ensurePresent(dp.id, item)
  }

  @scala.annotation.tailrec
  private def blockAggr(expr: DataExpr): Int = expr match {
    case by: DataExpr.GroupBy          => blockAggr(by.af)
    case _: DataExpr.All               => Block.Sum
    case _: DataExpr.Sum               => Block.Sum
    case _: DataExpr.Count             => Block.Count
    case _: DataExpr.Min               => Block.Min
    case _: DataExpr.Max               => Block.Max
    case DataExpr.Consolidation(af, _) => blockAggr(af)
  }

  private def executeImpl(context: EvalContext, expr: DataExpr): List[TimeSeries] = {
    val cfStep = context.step
    require(cfStep >= step, "step for query must be >= step for the database")
    require(cfStep % step == 0, "consolidated step must be multiple of db step")

    val query = TagQuery(Some(expr.query))
    val aggr = blockAggr(expr)
    val collector = AggregateCollector(expr)

    val multiple = (cfStep / step).asInstanceOf[Int]

    val bufStart = context.start
    val bufEnd = context.end - cfStep

    def newBuffer(tags: Map[String, String]): TimeSeriesBuffer = {
      TimeSeriesBuffer(tags, cfStep, bufStart, bufEnd)
    }

    // Accumulate the block counts locally and increment the counters once after
    // the loop. These counts scale with (matched series * blocks per series), so
    // for broad queries on a busy node this inner loop runs billions of times.
    // Calling Counter.increment() per block makes the counter bookkeeping
    // (expiry checks, last-mod updates, step rolls) a top CPU consumer. Avoid
    // per-iteration meter updates in hot paths like this.
    var numQueryBlocks = 0L
    var numAggrBlocks = 0L
    index.findItems(query).foreach { item =>
      item.blocks.blockList.foreach { b =>
        numQueryBlocks += 1
        // Check if the block has data for the desired time range
        val blockEnd = b.start + (b.size + 1) * step
        if (b.start <= bufEnd && blockEnd > bufStart - cfStep) {
          numAggrBlocks += 1
          collector.add(item.tags, List(b), aggr, expr.cf, multiple, newBuffer)
        }
      }
    }
    queryBlocks.increment(numQueryBlocks)
    aggrBlocks.increment(numAggrBlocks)

    val stats = collector.stats
    queryMetrics.increment(stats.inputLines)
    queryLines.increment(stats.outputLines)
    queryInputDatapoints.increment(stats.inputDatapoints)
    queryOutputDatapoints.increment(stats.outputDatapoints)

    val resultKeys = Query.exactKeys(expr.query) ++ expr.finalGrouping
    val vs = collector.result
      .map { t =>
        val resultTags = expr match {
          case _: DataExpr.All => t.tags
          case _               => t.tags.filter(t => resultKeys.contains(t._1))
        }
        DataExpr.withDefaultLabel(expr, t.withTags(resultTags))
      }
      .sortWith { _.label < _.label }
    finalValues(context, expr, vs)
  }

  private def finalValues(
    context: EvalContext,
    expr: DataExpr,
    vs: List[TimeSeries]
  ): List[TimeSeries] = {
    expr match {
      case _: DataExpr.AggregateFunction if vs.isEmpty => List(TimeSeries.noData(context.step))
      case _                                           => vs
    }
  }

  def execute(context: EvalContext, expr: DataExpr): List[TimeSeries] = {
    val offset = expr.offset.toMillis
    if (offset == 0) executeImpl(context, expr)
    else {
      val offsetContext = context.withOffset(expr.offset.toMillis)
      executeImpl(offsetContext, expr).map { t =>
        t.offset(offset)
      }
    }
  }
}

object MemoryDatabase {

  def apply(cfg: Config): MemoryDatabase = {
    new MemoryDatabase(Spectator.globalRegistry(), cfg.getConfig("atlas.core.db"))
  }
}
