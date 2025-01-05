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

  private val data = new ConcurrentHashMap[ItemId, BlockStore]

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

  def rebuild(): Unit = {
    val now = registry.clock().wallTime()
    if (now - index.buildTime > rebuildAge) {
      val query = filter
      logger.info("rebuilding metadata index (filter={})", query)
      val droppedItems = index.rebuildIndex(query)
      droppedItems.foreach(item => data.remove(item.id))

      // Remove entries if all data has rolled out
      val windowSize = numBlocks * blockSize * step
      val cutoff = now - windowSize
      val iter = data.entrySet.iterator
      while (iter.hasNext) {
        val entry = iter.next()
        entry.getValue.cleanup(cutoff)
        if (!entry.getValue.hasData) {
          iter.remove()
        }
      }
      logger.info("done rebuilding metadata index, {} metrics", index.size)

      TaggedItem.retain(_ > cutoff)
    }
  }

  private def getOrCreateBlockStore(id: ItemId, tags: Map[String, String]): BlockStore = {
    var blkStore = data.get(id)
    if (blkStore == null) {
      // Create a new block store and update the index
      blkStore = data.computeIfAbsent(id, _ => new MemoryBlockStore(step, blockSize, numBlocks))
      index.update(BlockStoreItem.create(id, tags, blkStore))
    }
    blkStore
  }

  def setFilter(query: Query): Unit = {
    filter = query
  }

  def update(id: ItemId, tags: Map[String, String], timestamp: Long, value: Double): Unit = {
    if (filter.matches(tags)) {
      val blkStore = getOrCreateBlockStore(id, tags)
      blkStore.update(timestamp, value)
    }
  }

  def update(dp: DatapointTuple): Unit = {
    update(dp.id, dp.tags, dp.timestamp, dp.value)
  }

  def update(ds: List[DatapointTuple]): Unit = {
    ds.foreach(update)
  }

  def rollup(dp: DatapointTuple): Unit = {
    val blkStore = getOrCreateBlockStore(dp.id, dp.tags)
    blkStore.update(dp.timestamp, dp.value, rollup = true)
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

    val end = context.end
    val multiple = (cfStep / step).asInstanceOf[Int]
    val s = context.start / cfStep
    val e = end / cfStep
    val bs = s * multiple
    val be = e * multiple

    val stepLength = be - bs + 1
    val cfStepLength = stepLength / multiple
    val bufStart = bs * step
    val bufEnd = bufStart + cfStepLength * cfStep - cfStep

    def newBuffer(tags: Map[String, String]): TimeSeriesBuffer = {
      TimeSeriesBuffer(tags, cfStep, bufStart, bufEnd)
    }

    index.findItems(query).foreach { item =>
      item.blocks.blockList.foreach { b =>
        queryBlocks.increment()
        // Check if the block has data for the desired time range
        val blockEnd = b.start + (b.size + 1) * step
        if (b.start <= be * step && blockEnd >= bs * step) {
          aggrBlocks.increment()
          collector.add(item.tags, List(b), aggr, expr.cf, multiple, newBuffer)
        }
      }
    }

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
