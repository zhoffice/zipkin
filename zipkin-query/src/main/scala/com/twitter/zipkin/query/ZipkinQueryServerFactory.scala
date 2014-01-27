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
package com.twitter.zipkin.query

import com.twitter.app.App
import com.twitter.finagle.ListeningServer
import com.twitter.logging.Logger
import com.twitter.finagle.ThriftMux
import com.twitter.finagle.stats.{DefaultStatsReceiver, StatsReceiver}
import com.twitter.util.{Await, Future}
import com.twitter.zipkin.query.adjusters._
import com.twitter.zipkin.gen.Adjust
import com.twitter.zipkin.storage.{Aggregates, NullAggregates, SpanStore}
import com.twitter.server.TwitterServer

object ZipkinQueryServerDefaults {
  val AdjusterMap: Map[Adjust, Adjuster] = Map(
    Adjust.Nothing -> NullAdjuster,
    Adjust.TimeSkew -> new TimeSkewAdjuster()
  )
}

trait ZipkinQueryServiceFactory { self: App =>
  val queryServiceDurationBatchSize = flag("zipkin.queryService.durationBatchSize", 500, "max number of durations to pull per batch")

  def newQueryService(
    spanStore: SpanStore,
    aggregatesStore: Aggregates = new NullAggregates,
    adjusters: Map[Adjust, Adjuster] = ZipkinQueryServerDefaults.AdjusterMap
  ): ThriftQueryService = {
    new ThriftQueryService(
      spanStore, aggregatesStore, adjusters, queryServiceDurationBatchSize())
  }
}

//object ZipkinQueryServer extends TwitterServer with ZipkinQueryServiceFactory {
  //val queryServicePort = flag("zipkin.queryService.port", ":9411", "port for the query service to listen on")

  //def main() {
    //val server = ThriftMux.serveIface(queryServicePort(), newQueryService())
    //onExit { server.close() }
    //Await.ready(server)
  //}
//}
