package com.twitter.zipkin.collector.processor

/*
 * Copyright 2012 Twitter Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
import com.twitter.zipkin.gen
import com.twitter.zipkin.common.{Span, Endpoint, Annotation}
import com.twitter.ostrich.stats.{Histogram, Distribution, Stats}
import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers._
import org.scalatest.mock.MockitoSugar._
import org.mockito.Mockito.{never, times, verify, when}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class OstrichServiceSpec extends WordSpec {
  val histogram = Histogram()
  histogram.add(10)
  val distribution = new Distribution(histogram)

  val prefix = "agg."

  "OstrichService" should {
    "add two metrics if server span" in {
      val agg = new OstrichService(prefix)

      val annotation1 = Annotation(10, gen.Constants.SERVER_RECV, Some(Endpoint(1, 2, "service")))
      val annotation2 = Annotation(20, gen.Constants.SERVER_SEND, Some(Endpoint(3, 4, "service")))
      val annotation3 = Annotation(30, "value3", Some(Endpoint(5, 6, "service")))

      val span = Span(12345, "methodcall", 666, None, List(annotation1, annotation2, annotation3), Nil)

      agg.apply(span)


      Stats.getMetrics()(prefix + "service") must equal (distribution)
      Stats.getMetrics()(prefix + "service.methodcall") must equal (distribution)
    }

    "not add client spans to metrics" in {
      val agg = new OstrichService(prefix)

      val annotation1 = Annotation(10, gen.Constants.CLIENT_SEND, Some(Endpoint(1, 2, "service")))
      val annotation2 = Annotation(20, gen.Constants.CLIENT_RECV, Some(Endpoint(3, 4, "service")))
      val annotation3 = Annotation(30, "value3", Some(Endpoint(5, 6, "service")))
      val annotation4 = Annotation(40, gen.Constants.SERVER_RECV, Some(Endpoint(1, 2, "service")))
      val annotation5 = Annotation(50, gen.Constants.SERVER_SEND, Some(Endpoint(3, 4, "service")))
      val annotation6 = Annotation(60, "value3", Some(Endpoint(5, 6, "service")))

      // only add one item to the histogram despite adding two spans
      histogram.add(10)

      val span = Span(12345, "methodcall", 666, None, List(annotation1, annotation2, annotation3), Nil)
      agg.apply(span)
      val span2 = Span(12346, "methodcall", 667, None, List(annotation4, annotation5, annotation6), Nil)
      agg.apply(span2)

      val serviceMetrics = Stats.getMetrics()(prefix + "service")
      val methodMetrics = Stats.getMetrics()(prefix + "service.methodcall")
      serviceMetrics must equal (distribution)
      methodMetrics must equal (distribution)
    }
  }
}
