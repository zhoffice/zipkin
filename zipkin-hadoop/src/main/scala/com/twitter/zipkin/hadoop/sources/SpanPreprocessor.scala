package com.twitter.zipkin.hadoop.sources

import com.twitter.scalding.TDsl._
import com.twitter.scalding.{Args, TypedPipe, UtcDateRangeJob, Job}
import com.twitter.zipkin.gen.{Annotation, BinaryAnnotation, Span}
import com.twitter.zipkin.gen
import scala.collection.JavaConverters._

/**
 * Job that merges all spans that have the same trace ID, span ID, and parent span ID
 */
class SpanPreprocessor(args: Args) extends Job(args) with UtcDateRangeJob {
  val timeGranularity: TimeGranularity = TimeGranularity.Hour

  SpanSource(timeGranularity).read.typed('in, 'out) { spans: TypedPipe[Span] =>
    spans
      .groupBy { span => (span.trace_id, span.id, span.parent_id) }
      .mapValues { span => (span.annotations, span.binary_annotations) }
      .reduce { case (left: (List[Annotation], List[BinaryAnnotation]), right: (List[Annotation], List[BinaryAnnotation])) =>
        ((left._1 ++ right._1).asJava, (left._2 ++ right._2).asJava)
      }
      .map { case ((traceId, spanId, parentId), (annotations, binaryAnnotations)) =>
        val span = new gen.Span(traceId, "", spanId, annotations, binaryAnnotations)
        if (parentId != 0) {
          span.setParent_id(parentId)
        }
        span
      }
  }.write(PrepNoNamesSpanSource(timeGranularity))
}
