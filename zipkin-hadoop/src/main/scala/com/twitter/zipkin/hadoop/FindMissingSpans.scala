package com.twitter.zipkin.hadoop

import com.twitter.zipkin.gen.{BinaryAnnotation, Span, Annotation}
import com.twitter.scalding.{Tsv, DefaultDateRangeJob, Job, Args}
import sources.{PrepNoNamesSpanSource, SpanSource}

class FindMissingSpans(args: Args) extends Job(args) with DefaultDateRangeJob {

  val preprocessed = SpanSource()
    .read
    .mapTo(0 -> ('trace_id, 'id, 'parent_id, 'annotations, 'binary_annotations))
      { s: Span => (s.trace_id, s.id, s.parent_id, s.annotations.toList, s.binary_annotations.toList) }
    .groupAll( _.size('count) )
    .write(Tsv(args("output")))
}
