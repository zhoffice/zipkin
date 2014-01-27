package com.twitter.zipkin.simple

import com.twitter.zipkin.storage.WriteSpanStore
import com.twitter.zipkin.storage.WriteSpanStore
import com.twitter.zipkin.AwaitableClosable
import com.twitter.zipkin.collector.{SpanReceiver, ZipkinQueuedCollectorFactory}
import com.twitter.zipkin.zookeeper.ZooKeeperClientFactory
import com.twitter.zipkin.cassandra.CassieSpanStoreFactory
import com.twitter.zipkin.query.ZipkinQueryServiceFactory
import com.twitter.zipkin.web.ZipkinWeb
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.{Await, Closable, Future}
import com.twitter.zipkin.common.Span
import com.twitter.server.TwitterServer
import com.twitter.zipkin.receiver.scribe.ScribeSpanReceiverFactory

object Main extends TwitterServer
  with ZipkinWeb
  with ZipkinQueryServiceFactory
  with CassieSpanStoreFactory
  with ScribeSpanReceiverFactory
  with ZooKeeperClientFactory
  with ZipkinQueuedCollectorFactory
{
  def newReceiver(stats: StatsReceiver, receive: Seq[Span] => Future[Unit]): SpanReceiver =
    newScribeSpanReceiver(receive, stats.scope("ScribeSpanReceiver"))

  def newSpanStore(stats: StatsReceiver): WriteSpanStore =
    newCassandraStore(stats.scope("CassieSpanStore"))

  def main() {
    val store = newCassandraStore(statsReceiver.scope("CassieSpanStore"))
    val receiver = newReceiver(statsReceiver, store(_))
    val query = newQueryService(store)
    val web = webServer(query)

    onExit { Closable.sequence(receiver, web, store).close() }
    Await.all(receiver, web, store)
  }
}
