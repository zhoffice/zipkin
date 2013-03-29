package com.twitter.zipkin.conversions

import com.twitter.zipkin.common.{AnnotationType, BinaryAnnotation}
import com.twitter.zipkin.conversions.json._
import java.nio.ByteBuffer
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers._
import org.scalatest.mock.MockitoSugar._
import org.mockito.Mockito.{never, times, verify, when}

@RunWith(classOf[JUnitRunner])
class JsonSpec extends WordSpec {

  "JsonAdapter" should {
    "convert binary annotations" when {
      val key = "key"

      "bool" in {
        val trueAnnotation = BinaryAnnotation(key, ByteBuffer.wrap(Array[Byte](1)), AnnotationType.Bool, None)
        val falseAnnotation = BinaryAnnotation(key, ByteBuffer.wrap(Array[Byte](0)), AnnotationType.Bool, None)

        val trueConvert = trueAnnotation.toJson
        trueConvert.value must equal (true)

        val falseConvert = falseAnnotation.toJson
        falseConvert.value must equal (false)
      }

      "short" in {
        val ann = BinaryAnnotation(key, ByteBuffer.allocate(2).putShort(0, 5.toShort), AnnotationType.I16, None)
        val convert = ann.toJson
        convert.value must equal (5)
      }

      "int" in {
        val ann = BinaryAnnotation(key, ByteBuffer.allocate(4).putInt(0, 6), AnnotationType.I32, None)
        val convert = ann.toJson
        convert.value must equal (6)
      }

      "long" in {
        val ann = BinaryAnnotation(key, ByteBuffer.allocate(8).putLong(0, 99999999999L), AnnotationType.I64, None)
        val convert = ann.toJson
        convert.value must equal (99999999999L)
      }

      "double" in {
        val ann = BinaryAnnotation(key, ByteBuffer.allocate(8).putDouble(0, 1.3496), AnnotationType.Double, None)
        val convert = ann.toJson
        convert.value must equal (1.3496)
      }

      "string" in {
        val ann = BinaryAnnotation(key, ByteBuffer.wrap("HELLO!".getBytes), AnnotationType.String, None)
        val convert = ann.toJson
        convert.value must equal ("HELLO!")
      }
    }
  }
}
