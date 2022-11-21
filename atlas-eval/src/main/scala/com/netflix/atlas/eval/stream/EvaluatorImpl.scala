/*
 * Copyright 2014-2022 Netflix, Inc.
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
package com.netflix.atlas.eval.stream

import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.ws.BinaryMessage
import akka.http.scaladsl.model.ws.TextMessage
import akka.http.scaladsl.model.ws.WebSocketRequest
import akka.stream.FlowShape
import akka.stream.Materializer
import akka.stream.OverflowStrategy
import akka.stream.ThrottleMode
import akka.stream.scaladsl.Broadcast
import akka.stream.scaladsl.BroadcastHub
import akka.stream.scaladsl.FileIO
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.GraphDSL
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Merge
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.netflix.atlas.akka.ClusterOps
import com.netflix.atlas.akka.StreamOps
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.eval.model.AggrDatapoint
import com.netflix.atlas.eval.model.AggrValuesInfo
import com.netflix.atlas.eval.model.LwcExpression
import com.netflix.atlas.eval.model.LwcMessages
import com.netflix.atlas.eval.model.TimeGroup
import com.netflix.atlas.eval.stream.EurekaSource.Instance
import com.netflix.atlas.eval.stream.Evaluator.DataSource
import com.netflix.atlas.eval.stream.Evaluator.DataSources
import com.netflix.atlas.eval.stream.Evaluator.DatapointGroup
import com.netflix.atlas.eval.stream.Evaluator.MessageEnvelope
import com.netflix.atlas.json.Json
import com.netflix.atlas.json.JsonSupport
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.patterns.ThreadPoolMonitor
import com.typesafe.config.Config
import org.reactivestreams.Processor
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory

import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/**
  * Internal implementation details for [[Evaluator]]. Anything needing a stable API should
  * be using that class directly.
  */
private[stream] abstract class EvaluatorImpl(
  config: Config,
  registry: Registry,
  implicit val system: ActorSystem,
  implicit val materializer: Materializer
) {

  private val logger = LoggerFactory.getLogger(getClass)

  // Cached context instance used for things like expression validation.
  private val validationStreamContext = newStreamContext()

  // Timeout for DataSources unique operator: emit repeating DataSources after timeout exceeds
  private val uniqueTimeout: Long = config.getDuration("atlas.eval.stream.unique-timeout").toMillis

  // Version of the LWC Server API to use
  private val lwcapiVersion: Int = config.getInt("atlas.eval.stream.lwcapi-version")

  // Counter for message that cannot be parsed
  private val badMessages = registry.counter("atlas.eval.badMessages")

  // Number of threads to use for parsing payloads
  private val parsingNumThreads = math.max(Runtime.getRuntime.availableProcessors() / 2, 2)

  // Execution context to use for parsing payloads coming back from lwcapi service
  private val parsingEC = {
    val threadCount = new AtomicInteger()
    val factory = new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val name = s"AtlasEvalParsing-${threadCount.getAndIncrement()}"
        val thread = new Thread(r, name)
        thread.setDaemon(true)
        thread
      }
    }
    val executor = Executors.newFixedThreadPool(parsingNumThreads, factory)
    ThreadPoolMonitor.attach(
      registry,
      executor.asInstanceOf[ThreadPoolExecutor],
      "AtlasEvalParsing"
    )
    ExecutionContext.fromExecutor(executor)
  }

  private def newStreamContext(dsLogger: DataSourceLogger = (_, _) => ()): StreamContext = {
    new StreamContext(
      config,
      Http().superPool(),
      materializer,
      registry,
      dsLogger
    )
  }

  protected def validateImpl(ds: DataSource): Unit = {
    validationStreamContext.validateDataSource(ds).get
  }

  protected def writeInputToFileImpl(uri: String, file: Path, duration: Duration): Unit = {
    val d = FiniteDuration(duration.toMillis, TimeUnit.MILLISECONDS)
    writeInputToFileImpl(Uri(uri), file, d)
  }

  protected def writeInputToFileImpl(uri: Uri, file: Path, duration: FiniteDuration): Unit = {
    // Explicit type needed in 2.5.2, but not 2.5.0. Likely related to:
    // https://github.com/akka/akka/issues/22666
    val options = Set[OpenOption](
      StandardOpenOption.WRITE,
      StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING
    )

    // Carriage returns can cause a lot of confusion when looking at the file. Clean it up
    // to be more unix friendly before writing to the file.
    val sink = Flow[AnyRef]
      .map(v => ByteString(Json.encode(v)))
      .map(_.filterNot(_ == '\r'))
      .filterNot(_.isEmpty)
      .map(_ ++ ByteString("\n\n"))
      .toMat(FileIO.toPath(file, options))(Keep.right)

    val ds = DataSources.of(new DataSource("_", uri.toString()))
    val src = Source(List(ds))
      .via(createInputFlow(newStreamContext()))

    val ref = EvaluationFlows.run(src, sink)
    try {
      // If it is coming from a finite source like a file it will likely complete
      // before the timeout
      Await.ready(ref.value, duration)
    } catch {
      // For eureka sources they will go on forever, when the requested timeout
      // expires shutdown the stream and notify the user of any problems
      case _: TimeoutException =>
        ref.killSwitch.shutdown()
        Await.ready(ref.value, duration)
    }
  }

  protected def createPublisherImpl(uri: String): Publisher[JsonSupport] = {
    createPublisherImpl(Uri(uri))
  }

  protected def createPublisherImpl(uri: Uri): Publisher[JsonSupport] = {
    val ds = DataSources.of(new DataSource("_", uri.toString()))

    val source = uri.scheme match {
      case s if s.startsWith("http") =>
        // The repeat/throttle is needed to prevent the stream from shutting down when
        // the source is complete.
        Source.repeat(ds).throttle(1, 1.minute, 1, ThrottleMode.Shaping)
      case _ =>
        // For finite sources like a file it should shut down as soon as the data has
        // been processed
        Source.single(ds)
    }

    source
      .via(createProcessorFlow)
      .map(_.getMessage)
      .toMat(Sink.asPublisher(true))(Keep.right)
      .run()
  }

  protected def createStreamsProcessorImpl(): Processor[DataSources, MessageEnvelope] = {
    createStreamsFlow.toProcessor.run()
  }

  /**
    * Internal API, may change in future releases, should only used by internal components
    * to maximize throughput under heavy load by enabling operator fusion optimization.
    */
  def createStreamsFlow: Flow[DataSources, MessageEnvelope, NotUsed] = {
    Flow[DataSources]
      .map(dss => groupByHost(dss))
      // Emit empty DataSource if no more DataSource for a host, so that the sub-stream get the info
      .via(new FillRemovedKeysWith[String, DataSources](_ => DataSources.empty()))
      .flatMapMerge(Int.MaxValue, dssMap => Source(dssMap.toList))
      .groupBy(Int.MaxValue, _._1, true) // groupBy host
      .map(_._2) // keep only DataSources
      .via(createProcessorFlow)
      .mergeSubstreams
  }

  protected def groupByHost(dataSources: DataSources): scala.collection.Map[String, DataSources] = {
    dataSources.getSources.asScala
      .groupBy(getHost(_))
      .map { case (host, dsSet) => host -> new DataSources(dsSet.asJava) }
  }

  private def getHost(dataSource: DataSource): String = {
    if (dataSource.isLocal)
      "_"
    else
      Uri(dataSource.getUri).authority.host.address
  }

  private[stream] def createProcessorFlow: Flow[DataSources, MessageEnvelope, NotUsed] = {

    // Flow used for logging diagnostic messages
    val (queue, logSrc) = StreamOps
      .blockingQueue[MessageEnvelope](registry, "DataSourceLogger", 10)
      .toMat(BroadcastHub.sink(1))(Keep.both)
      .run()
    val dsLogger: DataSourceLogger = { (ds, msg) =>
      val env = new MessageEnvelope(ds.getId, msg)
      queue.offer(env)
    }

    // One manager per processor to ensure distinct stream ids on LWC
    val context = newStreamContext(dsLogger)

    val g = GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      // Split to 2 destinations: Input flow, Final Eval Step
      val datasources = builder.add(Broadcast[DataSources](2))

      // Combine the intermediate output and data sources for the final evaluation
      // step
      val finalEvalInput = builder.add(Merge[AnyRef](2))

      val intermediateEval = createInputFlow(context)
        .via(context.monitorFlow("10_InputBatches"))
        .via(new LwcToAggrDatapoint(context))
        .flatMapConcat { vs =>
          Source(vs.groupBy(_.step).map(_._2.toList))
        }
        .groupBy(Int.MaxValue, _.head.step, allowClosedSubstreamRecreation = true)
        .via(new TimeGrouped(context))
        .flatMapConcat(Source.apply)
        .mergeSubstreams
        .via(context.monitorFlow("11_GroupedDatapoints"))

      // Use "buffer + dropTail" to avoid back pressure of DataSources broadcast, so that the 2
      // sub streams will not block each other
      datasources
        .out(0)
        .buffer(1, OverflowStrategy.dropTail) ~> intermediateEval ~> finalEvalInput.in(0)
      datasources
        .out(1)
        .buffer(1, OverflowStrategy.dropTail) ~> finalEvalInput.in(1)

      // Overall to the outside it looks like a flow of DataSources to MessageEnvelope
      FlowShape(datasources.in, finalEvalInput.out)
    }

    // Final evaluation of the overall expression
    Flow[DataSources]
      .map(ReplayLogging.log)
      .map { s =>
        val validated = context.validate(s)
        context.setDataSources(validated)
        validated
      }
      .via(g)
      .flatMapConcat(s => Source(splitByStep(s)))
      .groupBy(Int.MaxValue, stepSize, allowClosedSubstreamRecreation = true)
      .via(new FinalExprEval(context.interpreter))
      .mergeSubstreams
      .via(context.monitorFlow("12_OutputSources"))
      .flatMapConcat(s => s)
      .via(context.monitorFlow("13_OutputMessages"))
      .via(new OnUpstreamFinish[MessageEnvelope](queue.complete()))
      .merge(logSrc, eagerComplete = false)
  }

  protected def createDatapointProcessorImpl(
    sources: DataSources
  ): Processor[DatapointGroup, MessageEnvelope] = {

    // There should be a single step size across all sources, this will throw otherwise
    val stepSize = sources.stepSize()

    // Flow used for logging diagnostic messages
    val (queue, logSrc) = StreamOps
      .blockingQueue[MessageEnvelope](registry, "DataSourceLogger", 10)
      .toMat(BroadcastHub.sink(1))(Keep.both)
      .run()
    val dsLogger: DataSourceLogger = { (ds, msg) =>
      val env = new MessageEnvelope(ds.getId, msg)
      queue.offer(env)
    }

    // Initialize context with fixed data sources
    val context = newStreamContext(dsLogger)
    context.validate(sources)
    context.setDataSources(sources)
    val interpreter = context.interpreter

    // Extract data expressions to reuse for creating time groups
    val exprs = sources.getSources.asScala
      .flatMap(ds => interpreter.eval(Uri(ds.getUri)))
      .flatMap(_.expr.dataExprs)
      .toList
      .distinct

    Flow[DatapointGroup]
      .map(g => toTimeGroup(stepSize, exprs, g, context))
      .merge(Source.single(sources), eagerComplete = false)
      .via(new FinalExprEval(interpreter))
      .flatMapConcat(s => s)
      .via(new OnUpstreamFinish[MessageEnvelope](queue.complete()))
      .merge(logSrc, eagerComplete = false)
      .toProcessor
      .run()
  }

  private def toTimeGroup(
    step: Long,
    exprs: List[DataExpr],
    group: DatapointGroup,
    context: StreamContext
  ): TimeGroup = {
    val aggrSettings = AggrDatapoint.AggregatorSettings(
      context.maxInputDatapointsPerExpression,
      context.maxIntermediateDatapointsPerExpression,
      context.registry
    )
    val valuesInfo = group.getDatapoints.asScala.zipWithIndex
      .flatMap {
        case (d, i) =>
          val tags = d.getTags.asScala.toMap
          exprs.filter(_.query.matches(tags)).map { expr =>
            // Restrict the tags to the common set for all matches to the data expression
            val keys = Query.exactKeys(expr.query) ++ expr.finalGrouping
            val exprTags = tags.filter(t => keys.contains(t._1))

            // Need to do the init for count aggregate
            val v = d.getValue
            val value = if (isCount(expr) && !v.isNaN) 1.0 else v

            // Position is used as source to avoid dedup of datapoints
            AggrDatapoint(group.getTimestamp, step, expr, i.toString, exprTags, value)
          }
      }
      .groupBy(_.expr)
      .map(t =>
        t._1 -> {
          val aggregator = AggrDatapoint.aggregate(t._2.toList, aggrSettings)

          aggregator match {
            case Some(aggr) if aggr.limitExceeded =>
              context.logDatapointsExceeded(group.getTimestamp, t._1)
              AggrValuesInfo(Nil, t._2.size)
            case Some(aggr) =>
              AggrValuesInfo(aggr.datapoints, t._2.size)
            case _ =>
              AggrValuesInfo(Nil, t._2.size)
          }
        }
      )
    TimeGroup(group.getTimestamp, step, valuesInfo)
  }

  @scala.annotation.tailrec
  private def isCount(expr: DataExpr): Boolean = {
    expr match {
      case by: DataExpr.GroupBy       => isCount(by.af)
      case cf: DataExpr.Consolidation => isCount(cf.af)
      case _: DataExpr.Count          => true
      case _                          => false
    }
  }

  /**
    * Extract the step size from DataSources or TimeGroup objects. This is needed to group
    * the objects so the FinalExprEval stage will only see a single step.
    */
  private def stepSize: PartialFunction[AnyRef, Long] = {
    case ds: DataSources => ds.stepSize()
    case grp: TimeGroup  => grp.step
    case v               => throw new IllegalArgumentException(s"unexpected value in stream: $v")
  }

  /**
    * Split a DataSources object into one object per distinct step size. Other objects will
    * be left unchanged. This allows the DataSources per step to be flattened into the stream
    * so we can group them by step along with the corresponding AggrDatapoint.
    */
  private def splitByStep(value: AnyRef): List[AnyRef] = value match {
    case ds: DataSources =>
      ds.getSources.asScala
        .groupBy(_.getStep.toMillis)
        .map {
          case (_, sources) =>
            new DataSources(sources.asJava)
        }
        .toList
    case _ =>
      List(value)
  }

  private[stream] def createInputFlow(
    context: StreamContext
  ): Flow[DataSources, List[AnyRef], NotUsed] = {

    val g = GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      // Split to 2 destinations: remote and local(File/Resources)
      val dataSourcesBroadcast = builder.add(Broadcast[DataSources](2))

      // Merge the data coming from remote and local before performing
      // the time grouping and aggregation
      val inputMerge = builder.add(Merge[List[AnyRef]](2))

      // Streams for remote (lwc-api cluster)
      val remoteFlow =
        createClusterLookupFlow(context).via(createClusterStreamFlow(context))

      // Streams for local
      val localFlow = Flow[DataSources]
        .flatMapMerge(Int.MaxValue, s => Source(s.getSources.asScala.toList))
        .flatMapMerge(Int.MaxValue, s => context.localSource(Uri(s.getUri)))
        .map(parseMessage)

      // Broadcast to remote/local flow, process and merge
      dataSourcesBroadcast.out(0).map(_.remoteOnly()) ~> remoteFlow ~> inputMerge.in(0)
      dataSourcesBroadcast.out(1).map(_.localOnly()) ~> localFlow ~> inputMerge.in(1)

      // Overall to the outside it looks like a flow of DataSources to ByteString data
      FlowShape(dataSourcesBroadcast.in, inputMerge.out)
    }

    Flow[DataSources].via(g)
  }

  private def createClusterLookupFlow(
    context: StreamContext
  ): Flow[DataSources, SourcesAndGroups, NotUsed] = {
    Flow[DataSources]
      .conflate((_, ds) => ds)
      .throttle(1, 1.second, 1, ThrottleMode.Shaping)
      .via(context.monitorFlow("00_DataSourceUpdates"))
      .via(new EurekaGroupsLookup(context, 30.seconds))
      .via(context.monitorFlow("01_EurekaGroups"))
      .flatMapMerge(Int.MaxValue, s => s)
      .via(context.monitorFlow("01_EurekaRefresh"))
  }

  // Streams via WebSocket API `/api/v1/subscribe`, from each instance of lwc-api cluster
  private def createClusterStreamFlow(
    context: StreamContext
  ): Flow[SourcesAndGroups, List[AnyRef], NotUsed] = {
    Flow[SourcesAndGroups]
      .via(StreamOps.unique())
      .flatMapConcat { sourcesAndGroups =>
        // Cluster message first: need to connect before subscribe
        val instances = sourcesAndGroups._2.groups.flatMap(_.instances).toSet
        val exprs = toExprSet(sourcesAndGroups._1, context.interpreter)
        val dataMap = instances.map(i => i -> exprs).toMap
        Source(
          List(
            ClusterOps.Cluster(instances),
            ClusterOps.Data(dataMap)
          )
        )
      }
      .via(ClusterOps.groupBy(createGroupByContext(context)))
      .mapAsync(parsingNumThreads) { msg =>
        // This step is placed after merge of streams so there is a single
        // use of the pool and it is easier to track the concurrent usage.
        // Order is preserved to avoid potentially re-ordering messages in
        // a way that could push out data with a newer timestamp first.
        Future(parseBatch(msg))(parsingEC)
      }
  }

  private def createGroupByContext(
    context: StreamContext
  ): ClusterOps.GroupByContext[Instance, Set[LwcExpression], ByteString] = {
    ClusterOps.GroupByContext(
      instance => createWebSocketFlow(instance),
      registry,
      queueSize = 10
    )
  }

  private def toExprSet(dss: DataSources, interpreter: ExprInterpreter): Set[LwcExpression] = {
    dss.getSources.asScala.flatMap { dataSource =>
      interpreter.eval(Uri(dataSource.getUri)).map { expr =>
        LwcExpression(expr.toString, dataSource.getStep.toMillis)
      }
    }.toSet
  }

  private def createWebSocketFlow(
    instance: EurekaSource.Instance
  ): Flow[Set[LwcExpression], ByteString, NotUsed] = {
    val base = instance.substitute("ws://{local-ipv4}:{port}")
    val id = UUID.randomUUID().toString
    val uri = s"$base/api/v$lwcapiVersion/subscribe/$id"
    val webSocketFlowOrigin = Http(system).webSocketClientFlow(WebSocketRequest(uri))
    Flow[Set[LwcExpression]]
      .via(StreamOps.unique(uniqueTimeout)) // Updating subscriptions only if there's a change
      .map { exprs =>
        BinaryMessage(LwcMessages.encodeBatch(exprs.toSeq))
      }
      .via(webSocketFlowOrigin)
      .flatMapConcat {
        case _: TextMessage =>
          throw new MatchError("text messages are not supported")
        case BinaryMessage.Strict(str) =>
          Source.single(str)
        case msg: BinaryMessage =>
          msg.dataStream.fold(ByteString.empty)(_ ++ _)
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  private def parseBatch(message: ByteString): List[AnyRef] = {
    try {
      ReplayLogging.log(message)
      LwcMessages.parseBatch(message)
    } catch {
      case e: Exception =>
        logger.warn(s"failed to process message [$message]", e)
        badMessages.increment()
        List.empty
    }
  }

  private def parseMessage(message: ByteString): List[AnyRef] = {
    try {
      ReplayLogging.log(message)
      List(LwcMessages.parse(message))
    } catch {
      case e: Exception =>
        val messageString = toString(message)
        logger.warn(s"failed to process message [$messageString]", e)
        badMessages.increment()
        List.empty
    }
  }

  private def toString(bytes: ByteString): String = {
    val builder = new StringBuilder()
    bytes.foreach { b =>
      val c = b & 0xFF
      if (isPrintable(c))
        builder.append(c.asInstanceOf[Char])
      else if (c <= 0xF)
        builder.append("\\x0").append(Integer.toHexString(c))
      else
        builder.append("\\x").append(Integer.toHexString(c))
    }
    builder.toString()
  }

  private def isPrintable(c: Int): Boolean = {
    c >= 32 && c < 127
  }
}
