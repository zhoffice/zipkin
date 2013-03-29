package com.twitter.zipkin.web

import com.twitter.zipkin.common.{Annotation, BinaryAnnotation, Span}
import com.twitter.zipkin.conversions.json._
import com.codahale.jerkson.Json
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers._
import org.scalatest.mock.MockitoSugar._
import org.mockito.Mockito.{never, times, verify, when}

@RunWith(classOf[JUnitRunner])
class JsonSerializationSpec extends WordSpec {
  "Jerkson" should {
    "serialize" should {
      "span with no annotations" in {
        val s = Span(1L, "Unknown", 2L, None, List.empty[Annotation], List.empty[BinaryAnnotation], false)
        Json.generate(s.toJson) // must not produce exception
      }
    }
  }
}
