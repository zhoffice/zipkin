package com.twitter.zipkin.hadoop

import com.twitter.zipkin.gen.{Span, Annotation}
import com.twitter.scalding.{Tsv, DefaultDateRangeJob, Job, Args}
import sources.SpanSource

class GrepSpanIdInHour(args: Args) extends Job(args) with DefaultDateRangeJob {

  val grepById = args.required("id").toLong

  val preprocessed =
    SpanSource()
      .read
      .mapTo(0 ->('id)) { s: Span => (s.id) }
      .filter('id) { id: Long => grepById == id }
      .project('id)
      .write(Tsv(args("output")))
}