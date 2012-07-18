package com.twitter.zipkin.hadoop

import com.twitter.scalding._
import com.twitter.zipkin.hadoop.sources.FixedSpanSource
import java.sql.Timestamp
import java.text.SimpleDateFormat

class GetSpansInFile(args : Args) extends Job(args) {

  val filename = args.required("filename")
  val input = args.list("considered_date")

  val d = new SimpleDateFormat()
  val startDate = augmentString(input(0)).toLong
  val endDate = augmentString(input(1)).toLong


  val words = TextLine(filename)
    .read
    .mapTo('line -> ('service, 'timestamp)) { line : String =>
      val text = line.split(" ")
      (text(0), augmentString(text(1)).toLong)
    }.filter('timestamp) { ts : Long =>
      ts > startDate && ts < endDate
    }.groupBy('service){ _.toList[Long]('timestamp -> 'tsList)}
    .write(Tsv(args("output")))
}