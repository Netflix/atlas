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
package com.netflix.atlas.eval.stream

import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.http.scaladsl.model.ws.BinaryMessage
import org.apache.pekko.http.scaladsl.model.ws.TextMessage
import org.apache.pekko.http.scaladsl.model.ws.WebSocketRequest
import org.apache.pekko.stream.FlowShape
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.OverflowStrategy
import org.apache.pekko.stream.ThrottleMode
import org.apache.pekko.stream.scaladsl.Broadcast
import org.apache.pekko.stream.scaladsl.BroadcastHub
import org.apache.pekko.stream.scaladsl.FileIO
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.scaladsl.GraphDSL
import org.apache.pekko.stream.scaladsl.Keep
import org.apache.pekko.stream.scaladsl.Merge
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.eval.model.AggrDatapoint
import com.netflix.atlas.eval.model.AggrValuesInfo
import com.netflix.atlas.eval.model.LwcExpression
import com.netflix.atlas.eval.model.LwcMessages
import com.netflix.atlas.eval.model.TimeGroup
import com.netflix.atlas.eval.model.TimeGroupsTuple
import com.netflix.atlas.eval.stream.EddaSource.Instance
import com.netflix.atlas.eval.stream.Evaluator.DataSource
import com.netflix.atlas.eval.stream.Evaluator.DataSources
import com.netflix.atlas.eval.stream.Evaluator.DatapointGroup
import com.netflix.atlas.eval.stream.Evaluator.MessageEnvelope
import com.netflix.atlas.json.Json
import com.netflix.atlas.json.JsonSupport
import com.netflix.atlas.pekko.ClusterOps
import com.netflix.atlas.pekko.DiagnosticMessage
import com.netflix.atlas.pekko.StreamOps
import com.netflix.atlas.pekko.ThreadPools
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import org.reactivestreams.Processor
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

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

  // Calls out to a rewrite service in case URIs need mutating to pick the proper backend.
  private val dataSourceRewriter = DataSourceRewriter.create(config)

  // Cached context instance used for things like expression validation.
  private val validationStreamContext = newStreamContext(new ThrowingDSLogger)

  // URI pattern for web-socket request to backend
  private val backendUriPattern = config.getString("atlas.eval.stream.backend-uri-pattern")

  // Timeout for DataSources unique operator: emit repeating DataSources after timeout exceeds
  private val uniqueTimeout: Long = config.getDuration("atlas.eval.stream.unique-timeout").toMillis

  // Version of the LWC Server API to use
  private val lwcapiVersion: Int = config.getInt("atlas.eval.stream.lwcapi-version")

  // Should no data messages be emitted?
  private val enableNoDataMsgs = config.getBoolean("atlas.eval.stream.enable-no-data-messages")

  // Should subscription messages be compressed?
  private val compressSubMsgs = config.getBoolean("atlas.eval.stream.compress-sub-messages")

  // Counter for message that cannot be parsed
  private val badMessages = registry.counter("atlas.eval.badMessages")

  // Number of threads to use for parsing payloads
  private val parsingNumThreads = math.max(Runtime.getRuntime.availableProcessors() / 2, 2)

  // Execution context to use for parsing payloads coming back from lwcapi service
  private val parsingEC = ThreadPools.fixedSize(registry, "AtlasEvalParsing", parsingNumThreads)

  private def newStreamContext(
    dsLogger: DataSourceLogger = DataSourceLogger.Noop
  ): StreamContext = {
    new StreamContext(
      config,
      materializer,
      registry,
      dsLogger
    )
  }

  private def createStreamContextSource: (Source[MessageEnvelope, NotUsed], StreamContext) = {
    // Flow used for logging diagnostic messages
    val (queue, logSrc) = StreamOps
      .blockingQueue[MessageEnvelope](registry, "DataSourceLogger", 10)
      .toMat(BroadcastHub.sink(1))(Keep.both)
      .run()
    val dsLogger = DataSourceLogger.Queue(queue)

    // One manager per processor to ensure distinct stream ids on LWC
    val context = newStreamContext(dsLogger)
    logSrc -> context
  }

  protected def validateImpl(ds: DataSource): Unit = {
    val future = Source
      .single(DataSources.of(ds))
      .map(dss => dataSourceRewriter.rewrite(validationStreamContext, dss))
      .map(_.sources().asScala.map(validationStreamContext.validateDataSource).map(_.get))
      .toMat(Sink.head)(Keep.right)
      .run()
    Await.result(future, 60.seconds)
  }

  protected def writeInputToFileImpl(uri: String, file: Path, duration: Duration): Unit = {
    val d = FiniteDuration(duration.toMillis, TimeUnit.MILLISECONDS)
    writeInputToFileImpl(Uri(uri), file, d)
  }

  protected def writeInputToFileImpl(uri: Uri, file: Path, duration: FiniteDuration): Unit = {
    // Explicit type needed in 2.5.2, but not 2.5.0. Likely related to:
    // https://github.com/pekko/pekko/issues/22666
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

    val (logSrc, context) = createStreamContextSource
    source
      .via(createProcessorFlow(context))
      .via(new OnUpstreamFinish[MessageEnvelope](context.dsLogger.close()))
      .merge(logSrc, eagerComplete = false)
      .map(_.message)
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
    val (logSrc, context) = createStreamContextSource
    Flow[DataSources]
      .map(dss => dataSourceRewriter.rewrite(context, dss))
      .map(dss => groupByHost(dss))
      // Emit empty DataSource if no more DataSource for a host, so that the sub-stream get the info
      .via(new FillRemovedKeysWith[String, DataSources](_ => DataSources.empty()))
      .flatMapMerge(Int.MaxValue, dssMap => Source(dssMap.toList))
      .groupBy(Int.MaxValue, _._1, true) // groupBy host
      .map(_._2) // keep only DataSources
      .via(createProcessorFlow(context))
      .mergeSubstreams
      .via(new OnUpstreamFinish[MessageEnvelope](context.dsLogger.close()))
      .merge(logSrc, eagerComplete = false)
  }

  protected def groupByHost(dataSources: DataSources): scala.collection.Map[String, DataSources] = {
    dataSources.sources.asScala
      .groupBy(getHost)
      .map { case (host, dsSet) => host -> new DataSources(dsSet.asJava) }
  }

  private def getHost(dataSource: DataSource): String = {
    if (dataSource.isLocal)
      "_"
    else
      Uri(dataSource.uri).authority.host.address
  }

  private[stream] def createProcessorFlow(
    context: StreamContext
  ): Flow[DataSources, MessageEnvelope, NotUsed] = {

    val g = GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits.*

      // Split to 2 destinations: Input flow, Final Eval Step
      val datasources = builder.add(Broadcast[DataSources](2))

      // Combine the intermediate output and data sources for the final evaluation
      // step
      val finalEvalInput = builder.add(Merge[AnyRef](2))

      val intermediateEval = createInputFlow(context)
        .via(context.monitorFlow("10_InputBatches"))
        .via(new LwcToAggrDatapoint(context))
        .flatMapConcat { t =>
          Source(t.groupByStep)
        }
        .groupBy(Int.MaxValue, _.step, allowClosedSubstreamRecreation = true)
        .via(new TimeGrouped(context))
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
      .via(new FinalExprEval(context.interpreter, enableNoDataMsgs))
      .mergeSubstreams
      .via(context.monitorFlow("12_OutputSources"))
      .flatMapConcat(s => s)
      .via(context.monitorFlow("13_OutputMessages"))
  }

  protected def createDatapointProcessorImpl(
    sources: DataSources
  ): Processor[DatapointGroup, MessageEnvelope] = {

    // There should be a single step size across all sources, this will throw otherwise
    val stepSize = sources.stepSize()

    // Flow used for logging diagnostic messages
    val (logSrc, context) = createStreamContextSource

    // Initialize context with fixed data sources
    context.validate(sources)
    context.setDataSources(sources)
    val interpreter = context.interpreter

    // Extract data expressions to reuse for creating time groups
    val exprs = sources.sources.asScala
      .flatMap(ds => interpreter.eval(Uri(ds.uri)).exprs)
      .flatMap(_.expr.dataExprs)
      .toList
      .distinct

    Flow[DatapointGroup]
      .map(g => toTimeGroup(stepSize, exprs, g, context))
      .merge(Source.single(sources), eagerComplete = false)
      .via(new FinalExprEval(interpreter, enableNoDataMsgs))
      .flatMapConcat(s => s)
      .via(new OnUpstreamFinish[MessageEnvelope](context.dsLogger.close()))
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
    val valuesInfo = group.datapoints.asScala.zipWithIndex
      .flatMap {
        case (d, i) =>
          val tags = d.tags.asScala.toMap
          exprs.filter(_.query.matches(tags)).map { expr =>
            // Restrict the tags to the common set for all matches to the data expression
            val keys = Query.exactKeys(expr.query) ++ expr.finalGrouping
            val exprTags = tags.filter(t => keys.contains(t._1))

            // Need to do the init for count aggregate
            val v = d.value
            val value = if (isCount(expr) && !v.isNaN) 1.0 else v

            // Position is used as source to avoid dedup of datapoints
            AggrDatapoint(group.timestamp, step, expr, i.toString, exprTags, value)
          }
      }
      .groupBy(_.expr)
      .map(t =>
        t._1 -> {
          val aggregator = AggrDatapoint.aggregate(t._2.toList, aggrSettings)

          aggregator match {
            case Some(aggr) if aggr.limitExceeded =>
              context.logDatapointsExceeded(group.timestamp, t._1.toString)
              AggrValuesInfo(Nil, t._2.size)
            case Some(aggr) =>
              AggrValuesInfo(aggr.datapoints, t._2.size)
            case _ =>
              AggrValuesInfo(Nil, t._2.size)
          }
        }
      )
    TimeGroup(group.timestamp, step, valuesInfo)
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
    case ds: DataSources    => ds.stepSize()
    case t: TimeGroupsTuple => t.step
    case v                  => throw new IllegalArgumentException(s"unexpected value in stream: $v")
  }

  /**
    * Split a DataSources object into one object per distinct step size. Other objects will
    * be left unchanged. This allows the DataSources per step to be flattened into the stream
    * so we can group them by step along with the corresponding AggrDatapoint.
    */
  private def splitByStep(value: AnyRef): List[AnyRef] = value match {
    case ds: DataSources =>
      ds.sources.asScala
        .groupBy(_.step.toMillis)
        .map {
          case (_, sources) =>
            new DataSources(sources.asJava)
        }
        .toList
    case t: TimeGroupsTuple =>
      t.groupByStep
    case _ =>
      List(value)
  }

  private[stream] def createInputFlow(
    context: StreamContext
  ): Flow[DataSources, List[AnyRef], NotUsed] = {

    val g = GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits.*

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
        .flatMapMerge(Int.MaxValue, s => Source(s.sources.asScala.toList))
        .flatMapMerge(Int.MaxValue, s => context.localSource(Uri(s.uri)))
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
      .via(new EddaGroupsLookup(context, 30.seconds))
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
        val bytes = LwcMessages.encodeBatch(exprs.toSeq, compressSubMsgs)
        val dataMap = instances.map(i => i -> bytes).toMap
        Source(
          List(
            ClusterOps.Cluster(instances),
            ClusterOps.Data(dataMap)
          )
        )
      }
      // Repeat the last received element which will be the data map with the set
      // expressions to subscribe to. In the event of a connection failure the cluster
      // group by step will automatically reconnect, but the data message needs to be
      // resent. This ensures the most recent set of subscriptions will go out at a
      // regular cadence.
      .via(StreamOps.repeatLastReceived(5.seconds))
      .via(ClusterOps.groupBy(createGroupByContext))
      .mapAsync(parsingNumThreads) { msg =>
        // This step is placed after merge of streams so there is a single
        // use of the pool and it is easier to track the concurrent usage.
        // Order is preserved to avoid potentially re-ordering messages in
        // a way that could push out data with a newer timestamp first.
        Future(parseBatch(msg))(parsingEC)
      }
  }

  private def createGroupByContext: ClusterOps.GroupByContext[Instance, ByteString, ByteString] = {
    ClusterOps.GroupByContext(
      instance => createWebSocketFlow(instance),
      registry,
      queueSize = 10
    )
  }

  private def toExprSet(dss: DataSources, interpreter: ExprInterpreter): Set[LwcExpression] = {
    dss.sources.asScala.flatMap { dataSource =>
      val (exprType, exprs) = interpreter.parseQuery(Uri(dataSource.uri))
      exprs.map { expr =>
        val step = dataSource.step.toMillis
        LwcExpression(expr.toString, exprType, step)
      }
    }.toSet
  }

  private def createWebSocketFlow(
    instance: EddaSource.Instance
  ): Flow[ByteString, ByteString, NotUsed] = {
    val base = instance.substitute(backendUriPattern)
    val id = UUID.randomUUID().toString
    val uri = s"$base/api/v$lwcapiVersion/subscribe/$id"
    val webSocketFlowOrigin = Http(system).webSocketClientFlow(WebSocketRequest(uri))
    Flow[ByteString]
      .via(StreamOps.unique(uniqueTimeout)) // Updating subscriptions only if there's a change
      .map(BinaryMessage.apply)
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
    val builder = new java.lang.StringBuilder()
    bytes.foreach { b =>
      val c = b & 0xFF
      if (isPrintable(c))
        builder.append(c.asInstanceOf[Char])
      else if (c <= 0xF)
        builder.append("\\x0").append(Integer.toHexString(c))
      else
        builder.append("\\x").append(Integer.toHexString(c))
    }
    builder.toString
  }

  private def isPrintable(c: Int): Boolean = {
    c >= 32 && c < 127
  }

  private class ThrowingDSLogger extends DataSourceLogger {

    override def apply(ds: DataSource, msg: JsonSupport): Unit = {
      msg match {
        case dsg: DiagnosticMessage =>
          throw new IllegalArgumentException(dsg.message)
      }
    }

    override def close(): Unit = {}
  }
}
