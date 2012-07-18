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
      .flatMap('annotations -> 'timestamp) { al : List[Annotation] =>
        if (al.length == 0) None
        else {
          val start = al(0).timestamp
          al.foreach {a : Annotation => if (a.timestamp < start) start == a.timestamp }
          Some(start)
        }
      }.filter('timestamp) { ts : Long =>
        ts > startDate && ts < endDate
      }.project('id, 'timestamp)
      .groupBy('id){ _.toList[Long]('timestamp -> 'tsList)}
      .write(Tsv(args("output")))
}