/*
 * Copyright 2012 Twitter Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.zipkin.collector

import com.twitter.app.App
import com.twitter.zipkin.AwaitableClosable
import com.twitter.zipkin.storage.WriteSpanStore
import com.twitter.util._
import com.twitter.finagle.stats.{DefaultStatsReceiver, StatsReceiver}
import com.twitter.zipkin.common.Span
import com.twitter.server.TwitterServer

/**
 * A basic collector from which to create a server. Your collector will extend this
 * and implement `newReceiver` and `newSpanStore`. The base collector glues those two
 * together.
 */
trait ZipkinCollectorFactory { self: App =>
  def newReceiver(stats: StatsReceiver, receive: Seq[Span] => Future[Unit]): SpanReceiver
  def newSpanStore(stats: StatsReceiver): WriteSpanStore

  def newCollector(
    stats: StatsReceiver = DefaultStatsReceiver.scope("collector")
  ): AwaitableClosable = {
    val store = newSpanStore(stats)
    val receiver = newReceiver(stats, store(_))
    AwaitableClosable.sequence(receiver, store)
  }
}

/**
 * A base collector that inserts a configurable queue between the receiver and store.
 */
trait ZipkinQueuedCollectorFactory extends ZipkinCollectorFactory { self: App =>
  val itemQueueMax = flag("zipkin.itemQueue.maxSize", 500, "max number of span items to buffer")
  val itemQueueConcurrency = flag("zipkin.itemQueue.concurrency", 10, "number of concurrent workers to process the write queue")

  def newQueue(store: Seq[Span] => Future[Unit], stats: StatsReceiver): ItemQueue[Seq[Span]] =
    new ItemQueue[Seq[Span]](
      itemQueueMax(), itemQueueConcurrency(), store(_), stats.scope("ItemQueue"))

  override def newCollector(
    stats: StatsReceiver = DefaultStatsReceiver.scope("collector")
  ): AwaitableClosable = {
    val store = newSpanStore(stats)
    val queue = newQueue(store(_), stats)
    val receiver = newReceiver(stats, queue.add(_))
    AwaitableClosable.sequence(receiver, queue, store)
  }
}
