package com.twitter.zipkin.hadoop

import com.twitter.scalding._
import com.twitter.zipkin.hadoop.sources.FixedSpanSource
import java.sql.Timestamp
import java.text.SimpleDateFormat

class GetSpansInFile(args : Args) extends Job(args) {

  val filename = args.required("filename")
  val input = args.list("considered_date")

  val startDate = augmentString(input(0)).toLong
  val endDate = augmentString(input(1)).toLong

  val words = Tsv(filename)
    .read
    .mapTo((0, 1) -> ('id, 'timestamp)) { data: (Long, Long) => data }
    .filter('timestamp) { ts : Long => ts > startDate && ts < endDate }
    .write(Tsv(args("output")))
}