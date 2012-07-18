package com.twitter.zipkin.hadoop

import com.twitter.zipkin.gen
import com.twitter.zipkin.gen.{BinaryAnnotation, Span, Annotation}
import com.twitter.scalding.DefaultDateRangeJob
import com.twitter.scalding.{Tsv, DefaultDateRangeJob, Job, Args}
import sources.{SpanSource1, SpanSource}
import scala.collection.JavaConverters._
import collection.immutable.HashSet
import java.sql.Timestamp
import java.text._

class GetSpansInHour(args : Args) extends Job(args) with DefaultDateRangeJob {

  val input = args.list("considered_date")

  val startDate = augmentString(input(0)).toLong
  val endDate = augmentString(input(1)).toLong

  println(input)

  val preprocessed =
    SpanSource()
      .read
      .mapTo(0 -> ('id, 'annotations) ) { s : Span => (s.id, s.annotations.toList) }
      .filter('annotations) { al : List[Annotation] =>
          if (al.length > 0) {
            val ts = al(0).timestamp
            ts > startDate && ts < endDate
          } else {
            false
          }
      }
      .flatMap('annotations -> 'timestamp) {al : List[Annotation] =>
        val ts : Option[Long] = if (al.length > 0) Some(al(0).timestamp) else None
        ts
      }
      .project('id, 'timestamp)
      .groupBy('id){ _.toList[Long]('timestamp -> 'tsList)}
      .write(Tsv(args("output")))
}