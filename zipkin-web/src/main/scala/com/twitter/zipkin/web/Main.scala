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
package com.twitter.zipkin.web

import com.twitter.zipkin.AwaitableClosable
import com.twitter.conversions.time._
import com.twitter.finagle.http.HttpMuxer
import com.twitter.finagle.{Http, Thrift}
import com.twitter.server.TwitterServer
import com.twitter.app.App
import com.twitter.util.{Await, Future}
import com.twitter.zipkin.common.mustache.ZipkinMustache
import com.twitter.zipkin.gen.ZipkinQuery
import java.net.InetSocketAddress

trait ZipkinWeb { self: App =>
  import Handlers._

  private[this] val resourceDirs = Map(
    "/public/css"       -> "text/css",
    "/public/img"       -> "image/png",
    "/public/js"        -> "application/javascript",
    "/public/templates" -> "text/plain"
  )

  val webServerPort = flag("zipkin.web.port", new InetSocketAddress(8080), "Listening port for the zipkin web frontend")

  val webRootUrl = flag("zipkin.web.rootUrl", "http://localhost:8080/", "Url where the service is located")
  val webCacheResources = flag("zipkin.web.cacheResources", false, "cache resources (mustache, static sources, etc)")
  val webPinTtl = flag("zipkin.web.pinTtl", 30.days, "Length of time pinned traces should exist")

  // TODO: make this idomatic
  val webQueryClientLocation = flag("zipkin.queryClient.location", "127.0.0.1:9411", "Location of the query server")

  // TODO: ThriftMux
  lazy val webQueryClient: ZipkinQuery[Future] =
    Thrift.newIface[ZipkinQuery[Future]]("ZipkinQuery=" + webQueryClientLocation())

  def webServer(queryClient: ZipkinQuery[Future] = webQueryClient): AwaitableClosable = {
    ZipkinMustache.cache = webCacheResources()

    val muxer = Seq(
      ("/public/", handlePublic(resourceDirs, webCacheResources())),
      ("/", addLayout(webRootUrl()) andThen handleIndex(queryClient)),
      ("/traces/:id", addLayout(webRootUrl()) andThen handleTraces),
      ("/static", addLayout(webRootUrl()) andThen handleStatic),
      ("/aggregates", addLayout(webRootUrl()) andThen handleAggregates),
      ("/api/query", handleQuery(queryClient)),
      ("/api/services", handleServices(queryClient)),
      ("/api/spans", requireServiceName andThen handleSpans(queryClient)),
      ("/api/top_annotations", requireServiceName andThen handleTopAnnotations(queryClient)),
      ("/api/top_kv_annotations", requireServiceName andThen handleTopKVAnnotations(queryClient)),
      ("/api/dependencies", handleDependencies(queryClient)),
      ("/api/dependencies/?:startTime/?:endTime", handleDependencies(queryClient)),
      ("/api/get/:id", handleGetTrace(queryClient)),
      ("/api/trace/:id", handleGetTrace(queryClient)),
      ("/api/is_pinned/:id", handleIsPinned(queryClient)),
      ("/api/pin/:id/:state", handleTogglePin(queryClient, webPinTtl()))
    ).foldLeft(new HttpMuxer) { case (m , (p, handler)) =>
      val path = p.split("/").toList
      val handlePath = path.takeWhile { t => !(t.startsWith(":") || t.startsWith("?:")) }
      val suffix = if (p.endsWith("/") || p.contains(":")) "/" else ""

      m.withHandler(handlePath.mkString("/") + suffix,
        nettyToFinagle andThen
        renderPage andThen
        catchExceptions andThen
        checkPath(path) andThen
        handler)
    }

    AwaitableClosable.all(Http.serve(webServerPort(), muxer))
  }
}

object Main extends TwitterServer with ZipkinWeb {
  def main() {
    val server = webServer()
    Await.ready(server)
  }
}
