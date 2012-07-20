package com.twitter.zipkin.hadoop

import com.twitter.zipkin.gen.{Span, Annotation}
import com.twitter.scalding.{Tsv, DefaultDateRangeJob, Job, Args}
import sources.SpanSource

class GetSpansInHour(args : Args) extends Job(args) with DefaultDateRangeJob {

  val input = args.list("considered_date")

  val startDate = augmentString(input(0)).toLong
  val endDate = augmentString(input(1)).toLong

  val preprocessed =
    SpanSource()
      .read
      .mapTo(0 -> ('id, 'annotations) ) { s: Span => (s.id, s.annotations.toList) }
      .flatMap('annotations -> 'timestamp) { al: List[Annotation] =>
        al.map(_.timestamp).sortBy(t => t).headOption
      }.filter('timestamp) { ts: Long =>
        ts > startDate && ts < endDate
      }.project('id, 'timestamp)
      .write(Tsv(args("output")))
}