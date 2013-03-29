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
package com.twitter.zipkin.web

import com.twitter.finagle.http.{Request => FinagleRequest}
import com.twitter.finatra.Request
import com.twitter.util.Time
import com.twitter.zipkin.common.{AnnotationType, BinaryAnnotation}
import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers._
import org.scalatest.mock.MockitoSugar._
import org.mockito.Mockito.{never, times, verify, when}
import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class QueryExtractorSpec extends WordSpec {

  def request(p: (String, String)*) = new Request(FinagleRequest(p:_*)) {
    routeParams = mutable.Map(p:_*)
  }

  "QueryExtractor" should {
    "require serviceName" in {
      val r = request()
      QueryExtractor(r) must be (None)
    }

    "parse params" in {
      val fmt = "MM-dd-yyyy HH:mm:ss"
      val t = Time.now
      val formatted = t.format(fmt)
      val r = request(
        "serviceName" -> "myService",
        "spanName" -> "mySpan",
        "endDatetime" -> formatted,
        "limit" -> "1000")
      val actual = QueryExtractor(r)
      actual must not be (None)

      actual.get.serviceName must equal ("myService")
      actual.get.spanName must not be (None)
      actual.get.spanName.get must equal ("mySpan")
      actual.get.endTs must equal (new SimpleDateFormat(fmt).parse(formatted).getTime * 1000)
      actual.get.limit must equal (1000)
    }

    "have defaults" when {
      "endDateTime is not supplied" in Time.withCurrentTimeFrozen { tc =>
        val t = Time.now
        val r = request("serviceName" -> "myService")
        val actual = QueryExtractor(r)
        actual must not be (None)
        actual.get.endTs must equal (t.sinceEpoch.inMicroseconds)
      }

      "limit is not supplied" in {
        val r = request("serviceName" -> "myService")
        val actual = QueryExtractor(r)
        actual must not be (None)
        actual.get.limit must be (Constants.DefaultQueryLimit)
      }
    }

    "parse spanName special cases" when {
      "all" in {
        val r = request("serviceName" -> "myService", "spanName" -> "all")
        val actual = QueryExtractor(r)
        actual must not be (None)
        actual.get.spanName must be (None)
      }

      "\"\"" in {
        val r = request("serviceName" -> "myService", "spanName" -> "")
        val actual = QueryExtractor(r)
        actual must not be (None)
        actual.get.spanName must be (None)
      }

      "valid" in {
        val r = request("serviceName" -> "myService", "spanName" -> "something")
        val actual = QueryExtractor(r)
        actual must not be (None)
        actual.get.spanName must not be (None)
        actual.get.spanName.get must equal ("something")
      }
    }

    "parse annotations" in {
      val r = request(
        "serviceName" -> "myService",
        "annotations[0]" -> "finagle.retry",
        "annotations[1]" -> "finagle.timeout")
      val actual = QueryExtractor(r)
      actual must not be (None)
      actual.get.annotations must not be (None)
      actual.get.annotations.get must equal (Seq("finagle.retry", "finagle.timeout"))
    }

    "parse key value annotations" in {
      val r = request(
        "serviceName" -> "myService",
        "keyValueAnnotations[0][key]" -> "http.responsecode",
        "keyValueAnnotations[0][val]" -> "500"
      )
      val actual = QueryExtractor(r)
      actual must not be (None)
      actual.get.binaryAnnotations must not be (None)
      actual.get.binaryAnnotations.get must equal (Seq(BinaryAnnotation("http.responsecode", ByteBuffer.wrap("500".getBytes), AnnotationType.String, None)))
    }
  }
}
