package com.netflix.atlas.akka

import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.SourceQueueWithComplete
import com.netflix.spectator.api.Registry

class StreamOps {

  def sourceQueue[T](registry: Registry, size: Int, strategy: OverflowStrategy): SourceQueueWithComplete[T] = {
    Source.queue[T](size, strategy)
  }

}
