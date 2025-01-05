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
package com.netflix.atlas.lwc.events

import com.fasterxml.jackson.core.JsonGenerator
import com.netflix.atlas.core.util.FastGzipOutputStream
import com.netflix.atlas.json.Json
import com.netflix.spectator.api.Registry
import com.netflix.spectator.impl.Scheduler
import com.netflix.spectator.ipc.http.HttpClient
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import java.io.ByteArrayOutputStream
import java.net.URI
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.locks.ReentrantLock
import scala.util.Using

class RemoteLwcEventClient(registry: Registry, config: Config)
    extends AbstractLwcEventClient(registry.clock())
    with AutoCloseable
    with StrictLogging {

  import RemoteLwcEventClient.*

  private val scopedConfig = config.getConfig("atlas.lwc.events")
  private val configUri = URI.create(scopedConfig.getString("config-uri"))
  private val evalUri = URI.create(scopedConfig.getString("eval-uri"))

  private val configRefreshFrequency = scopedConfig.getDuration("config-refresh-frequency")
  private val heartbeatFrequency = scopedConfig.getDuration("heartbeat-frequency")

  private val bufferSize = scopedConfig.getInt("buffer-size")
  private val flushSize = scopedConfig.getInt("flush-size")
  private val batchSize = scopedConfig.getInt("batch-size")
  private val payloadSize = scopedConfig.getBytes("payload-size")

  private val buffer = new ArrayBlockingQueue[Event](bufferSize)

  private val sentEvents = registry.counter("lwc.events", "id", "sent")

  private val droppedQueueFull =
    registry.counter("lwc.events", "id", "dropped", "error", "queue-full")
  private val droppedSend = registry.counter("lwc.events", "id", "dropped", "error", "http")
  private val droppedTooBig = registry.counter("lwc.events", "id", "dropped", "error", "too-big")

  private val flushLock = new ReentrantLock()

  private var scheduler: Scheduler = _

  private var executorService: ExecutorService = _

  def start(): Unit = {
    if (scheduler == null) {
      scheduler = new Scheduler(registry, "LwcEventClient", 2)

      val refreshOptions = new Scheduler.Options()
        .withFrequency(Scheduler.Policy.FIXED_DELAY, configRefreshFrequency)
        .withStopOnFailure(false)
      scheduler.schedule(refreshOptions, () => refreshConfigs())

      val heartbeatOptions = new Scheduler.Options()
        .withFrequency(Scheduler.Policy.FIXED_DELAY, heartbeatFrequency)
        .withStopOnFailure(false)
      scheduler.schedule(heartbeatOptions, () => sendHeartbeat())

      executorService = createExecutorService
    }
  }

  def stop(): Unit = {
    if (scheduler != null) {
      scheduler.shutdown()
      scheduler = null
      executorService.shutdown()
    }
  }

  override def close(): Unit = {
    stop()
  }

  override def submit(id: String, event: LwcEvent): Unit = {
    if (!buffer.offer(Event(id, event)))
      droppedQueueFull.increment()
    if (buffer.size() > flushSize)
      flush()
  }

  private def refreshConfigs(): Unit = {
    try {
      val response = HttpClient.DEFAULT_CLIENT
        .get(configUri)
        .acceptGzip()
        .send()
        .decompress()

      if (response.status() == 200) {
        try {
          val subs = Json.decode[Expressions](response.entity())
          sync(Subscriptions.fromTypedList(subs.expressions))
        } catch {
          case e: Exception =>
            logger.warn("failed to process expressions payload", e)
        }
      } else if (response.status() >= 400) {
        logger.warn(s"failed to refresh expressions, status code ${response.status()}")
      }
    } catch {
      case e: Exception => logger.warn("failed to refresh expressions", e)
    }
  }

  private def sendHeartbeat(): Unit = {
    // Send explicit heartbeat used to flush data for time series
    process(LwcEvent.HeartbeatLwcEvent(registry.clock().wallTime()))
    if (!buffer.isEmpty)
      flush()
  }

  override protected def flush(): Unit = {
    if (flushLock.tryLock()) {
      try {
        val events = new java.util.ArrayList[Event](buffer.size())
        buffer.drainTo(events)
        if (!events.isEmpty) {
          import scala.jdk.CollectionConverters.*

          // Write out datapoints that need to be batched by timestamp
          val ds = events.asScala.collect {
            case Event(_, e: DatapointEvent) => e
          }.toList
          flushDatapoints(ds)

          // Write out other events for pass through
          val now = registry.clock().wallTime()
          batch(
            events.asScala.filterNot(_.isDatapoint).toList,
            es => send(EvalPayload(now, events = es))
          )
        }
      } finally {
        flushLock.unlock()
      }
    }
  }

  private[events] def batch(events: List[Event], sink: List[Event] => Unit): Unit = {
    var size = 0L
    val eventBatch = List.newBuilder[Event]
    val it = events.iterator
    while (it.hasNext) {
      val event = it.next()
      val estimatedSize = event.payload.estimatedSizeInBytes

      // Flush batch if size is met
      if (size + estimatedSize > payloadSize) {
        val es = eventBatch.result()
        eventBatch.clear()
        size = 0L
        if (es.nonEmpty) {
          sink(es)
        }
      }

      // Verify that a single message doesn't exceed the size
      if (estimatedSize > payloadSize) {
        logger.warn(s"dropping event with excessive size ($estimatedSize > $payloadSize)")
        droppedTooBig.increment()
      } else {
        size += estimatedSize
        eventBatch += event
      }
    }

    // Flush final batch
    val es = eventBatch.result()
    if (es.nonEmpty) {
      sink(es)
    }
  }

  private def flushDatapoints(events: List[DatapointEvent]): Unit = {
    events.groupBy(_.timestamp).foreach {
      case (timestamp, ds) =>
        ds.grouped(batchSize).foreach { vs =>
          send(EvalPayload(timestamp, metrics = vs))
        }
    }
  }

  protected def send(payload: EvalPayload): Unit = {
    val task: Runnable = () => {
      try {
        val response = HttpClient.DEFAULT_CLIENT
          .post(evalUri)
          .addHeader("Content-Encoding", "gzip")
          .withContent("application/x-jackson-smile", encodePayload(payload))
          .send()

        if (response.status() == 200) {
          val numEvents = payload.size
          logger.debug(s"sent $numEvents events")
          sentEvents.increment(numEvents)
        } else {
          logger.warn(s"failed to send events, status code ${response.status()}")
          droppedSend.increment(payload.size)
        }
      } catch {
        case e: Exception =>
          logger.warn("failed to send events", e)
          logger.trace(s"failed event payload: $payload", e)
          droppedSend.increment(payload.size)
      }
    }
    executorService.submit(task)
  }

  private def encodePayload(payload: EvalPayload): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    Using.resource(new FastGzipOutputStream(baos)) { out =>
      Using.resource(Json.newSmileGenerator(out)) { gen =>
        gen.writeStartObject()
        gen.writeNumberField("timestamp", payload.timestamp)
        gen.writeArrayFieldStart("metrics")
        payload.metrics.foreach(m => encodeMetric(m, gen))
        gen.writeEndArray()
        gen.writeArrayFieldStart("events")
        payload.events.foreach(e => encodeEvent(e, gen))
        gen.writeEndArray()
        gen.writeEndObject()
      }
    }
    baos.toByteArray
  }

  private def encodeMetric(event: DatapointEvent, gen: JsonGenerator): Unit = {
    gen.writeStartObject()
    gen.writeStringField("id", event.id)
    gen.writeObjectFieldStart("tags")
    event.tags.foreachEntry { (k, v) =>
      gen.writeStringField(k, v)
    }
    gen.writeEndObject()
    gen.writeNumberField("value", event.value)
    gen.writeFieldName("samples")
    Json.encode(gen, event.samples)
    gen.writeEndObject()
  }

  private def encodeEvent(event: Event, gen: JsonGenerator): Unit = {
    gen.writeStartObject()
    gen.writeStringField("id", event.id)
    gen.writeFieldName("payload")
    event.payload.encode(gen)
    gen.writeEndObject()
  }
}

object RemoteLwcEventClient {

  case class Expressions(expressions: List[Subscription])

  case class EvalPayload(
    timestamp: Long,
    metrics: List[DatapointEvent] = Nil,
    events: List[Event] = Nil
  ) {

    def size: Int = metrics.size + events.size
  }

  case class Event(id: String, payload: LwcEvent) {

    def isDatapoint: Boolean = {
      payload.isInstanceOf[DatapointEvent]
    }
  }

  /**
    * On JDK21 uses virtual threads, on earlier versions uses fixed thread pool based on
    * number of cores.
    */
  def createExecutorService: ExecutorService = {
    try {
      val cls = classOf[Executors]
      val method = cls.getMethod("newVirtualThreadPerTaskExecutor")
      method.invoke(null).asInstanceOf[ExecutorService]
    } catch {
      case _: Exception =>
        val numProcessors = Runtime.getRuntime.availableProcessors()
        val numThreads = Math.max(numProcessors / 2, 1)
        Executors.newFixedThreadPool(numThreads)
    }
  }
}
