package com.netflix.atlas.eval.stream

import akka.Done
import akka.stream.scaladsl.Source

import scala.concurrent.Promise
import scala.util.Success

/**
  * Reference to a source and a promise that can be completed to stop the
  * source.
  */
case class SourceRef[T, M](source: Source[T, M], promise: Promise[Done]) {
  def stop(): Unit = promise.complete(Success(Done))
}
