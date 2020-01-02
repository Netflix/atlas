/*
 * Copyright 2014-2020 Netflix, Inc.
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
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri
import akka.stream.ActorMaterializer
import akka.stream.FlowShape
import akka.stream.ThrottleMode
import akka.stream.scaladsl.Broadcast
import akka.stream.scaladsl.FileIO
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.GraphDSL
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Merge
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.netflix.atlas.akka.StreamOps
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.eval.model.AggrDatapoint
import com.netflix.atlas.eval.model.TimeGroup
import com.netflix.atlas.eval.stream.Evaluator.DataSource
import com.netflix.atlas.eval.stream.Evaluator.DataSources
import com.netflix.atlas.eval.stream.Evaluator.DatapointGroup
import com.netflix.atlas.eval.stream.Evaluator.MessageEnvelope
import com.netflix.atlas.json.JsonSupport
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import org.reactivestreams.Processor
import org.reactivestreams.Publisher

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Internal implementation details for [[Evaluator]]. Anything needing a stable API should
  * be using that class directly.
  */
private[stream] abstract class EvaluatorImpl(
  config: Config,
  registry: Registry,
  implicit val system: ActorSystem
) {

  private implicit val materializer = ActorMaterializer()

  // Cached context instance used for things like expression validation.
  private val validationStreamContext = newStreamContext()

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
    val sink = Flow[ByteString]
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
      val result = Await.result(ref.value, duration)
      if (!result.wasSuccessful) throw result.getError
    } catch {
      // For eureka sources they will go on forever, when the requested timeout
      // expires shutdown the stream and notify the user of any problems
      case e: TimeoutException =>
        ref.killSwitch.shutdown()
        val result = Await.result(ref.value, duration)
        if (!result.wasSuccessful) throw result.getError
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
    createProcessorFlow.toProcessor.run()
  }

  private[stream] def createProcessorFlow: Flow[DataSources, MessageEnvelope, NotUsed] = {

    // Flow used for logging diagnostic messages
    val (queue, logSrc) = StreamOps
      .blockingQueue[MessageEnvelope](registry, "DataSourceLogger", 10)
      .toMat(Sink.asPublisher(true))(Keep.both)
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
        .map(ReplayLogging.log)
        .via(context.monitorFlow("10_InputLines"))
        .via(new LwcToAggrDatapoint(context))
        .via(context.monitorFlow("11_LwcDatapoints"))
        .groupBy(Int.MaxValue, _.step, allowClosedSubstreamRecreation = true)
        .via(new TimeGrouped(context, 50))
        .mergeSubstreams
        .via(context.monitorFlow("12_GroupedDatapoints"))

      datasources.out(0) ~> intermediateEval ~> finalEvalInput.in(0)
      datasources.out(1) ~> finalEvalInput.in(1)

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
      .flatMapConcat(s => s)
      .mergeSubstreams
      .via(context.monitorFlow("13_OutputMessages"))
      .via(new OnUpstreamFinish[MessageEnvelope](queue.complete()))
      .merge(Source.fromPublisher(logSrc), eagerComplete = false)
  }

  protected def createDatapointProcessorImpl(
    sources: DataSources
  ): Processor[DatapointGroup, MessageEnvelope] = {

    // There should be a single step size across all sources, this will throw otherwise
    val stepSize = sources.stepSize()

    // Flow used for logging diagnostic messages
    val (queue, logSrc) = StreamOps
      .blockingQueue[MessageEnvelope](registry, "DataSourceLogger", 10)
      .toMat(Sink.asPublisher(true))(Keep.both)
      .run()
    val dsLogger: DataSourceLogger = { (ds, msg) =>
      val env = new MessageEnvelope(ds.getId, msg)
      queue.offer(env)
    }

    // Initialize context with fixed data sources
    val context = newStreamContext(dsLogger)
    context.validate(sources)
    context.setDataSources(sources)

    // Extract data expressions to reuse for creating time groups
    val exprs = sources.getSources.asScala
      .flatMap(ds => context.interpreter.eval(Uri(ds.getUri)))
      .flatMap(_.expr.dataExprs)
      .toList
      .distinct

    Flow[DatapointGroup]
      .map(g => toTimeGroup(stepSize, exprs, g))
      .merge(Source.single(sources), eagerComplete = false)
      .via(new FinalExprEval(context.interpreter))
      .flatMapConcat(s => s)
      .via(new OnUpstreamFinish[MessageEnvelope](queue.complete()))
      .merge(Source.fromPublisher(logSrc), eagerComplete = false)
      .toProcessor
      .run()
  }

  private def toTimeGroup(step: Long, exprs: List[DataExpr], group: DatapointGroup): TimeGroup = {
    val values = group.getDatapoints.asScala.zipWithIndex
      .flatMap {
        case (d, i) =>
          val tags = d.getTags.asScala.toMap
          exprs.filter(_.query.matches(tags)).map { expr =>
            // Restrict the tags to the common set for all matches to the data expression
            val keys = Query.exactKeys(expr.query) ++ expr.finalGrouping
            val exprTags = tags.filterKeys(keys.contains).toMap

            // Need to do the init for count aggregate
            val v = d.getValue
            val value = if (isCount(expr) && !v.isNaN) 1.0 else v

            // Position is used as source to avoid dedup of datapoints
            AggrDatapoint(group.getTimestamp, step, expr, i.toString, exprTags, value)
          }
      }
      .groupBy(_.expr)
      .map(t => t._1 -> AggrDatapoint.aggregate(t._2.toList))
    TimeGroup(group.getTimestamp, step, values)
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
  ): Flow[DataSources, ByteString, NotUsed] = {

    val g = GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      // Split to 2 destinations: Eureka, File/Resources
      val datasources = builder.add(Broadcast[DataSources](2))

      // Groups need to be sent to subscription step for posting the set of uris
      // we are interested in and to the stream step for maintaining the connections
      // and getting all of the data
      val eurekaGroups = builder.add(Broadcast[SourcesAndGroups](2))

      // Combine the data coming from Eureka and File/Resources before performing
      // the time grouping and aggregation
      val intermediateInput = builder.add(Merge[ByteString](2))

      // Send DataSources and Eureka groups to subscription manager that does a
      // `POST /lwc/api/v1/subscribe` to update the set of subscriptions being sent
      // to each connection
      val eurekaLookup = Flow[DataSources]
        .conflate((_, ds) => ds)
        .throttle(1, 1.second, 1, ThrottleMode.Shaping)
        .via(context.monitorFlow("00_DataSourceUpdates"))
        .via(new EurekaGroupsLookup(context, 30.seconds))
        .via(context.monitorFlow("01_EurekaGroups"))
        .flatMapMerge(Int.MaxValue, s => s)
        .via(context.monitorFlow("01_EurekaRefresh"))
      datasources.out(0).map(_.remoteOnly()) ~> eurekaLookup ~> eurekaGroups.in
      eurekaGroups.out(0) ~> new SubscriptionManager(context)

      // Streams, `GET /lwc/api/v1/stream/$id`, from each instance of the Eureka groups
      val eurekaStream = Flow[SourcesAndGroups]
        .map(_._2)
        .via(new ConnectionManager(context))
        .via(context.monitorFlow("02_ConnectionSources"))
        .flatMapMerge(Int.MaxValue, s => Source(s))
        .flatMapMerge(Int.MaxValue, s => s)
      eurekaGroups.out(1) ~> eurekaStream ~> intermediateInput.in(0)

      // Streams for local URIs like files or resources
      val tmp = Flow[DataSources]
        .flatMapMerge(Int.MaxValue, s => Source(s.getSources.asScala.toList))
        .flatMapMerge(Int.MaxValue, s => context.localSource(Uri(s.getUri)))
      datasources.out(1).map(_.localOnly()) ~> tmp ~> intermediateInput.in(1)

      // Overall to the outside it looks like a flow of DataSources to ByteString data
      FlowShape(datasources.in, intermediateInput.out)
    }

    Flow[DataSources].via(g)
  }
}
