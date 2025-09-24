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
package com.netflix.atlas.pekko

import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.patterns.ThreadPoolMonitor

import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.ExecutionContext

/**
  * Helpers for creating execution contexts based around java thread pools.
  */
object ThreadPools {

  private class NamedThreadFactory(name: String) extends ThreadFactory {

    private val threadCount = new AtomicInteger()

    override def newThread(r: Runnable): Thread = {
      val threadName = s"$name-${threadCount.getAndIncrement()}"
      val thread = new Thread(r, threadName)
      thread.setDaemon(true)
      thread
    }
  }

  /**
    * Returns a thread factory with threads using the name as a prefix followed by a number
    * to uniquely identify a particular thread.
    */
  def threadFactory(name: String): ThreadFactory = {
    new NamedThreadFactory(name)
  }

  /**
    * Returns an execution context based around a fixed size thread pool.
    *
    * @param registry
    *     Registry to use for the thread pool monitor.
    * @param name
    *     Name used as a prefix on the threads and as an id for the metrics.
    * @param numThreads
    *     Size of the thread pool.
    */
  def fixedSize(registry: Registry, name: String, numThreads: Int): ExecutionContext = {
    val executor = Executors.newFixedThreadPool(numThreads, threadFactory(name))
    ThreadPoolMonitor.attach(
      registry,
      executor.asInstanceOf[ThreadPoolExecutor],
      name
    )
    ExecutionContext.fromExecutor(executor)
  }
}
