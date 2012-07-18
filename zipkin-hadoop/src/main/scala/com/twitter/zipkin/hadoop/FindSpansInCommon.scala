package com.twitter.zipkin.hadoop

import com.twitter.scalding._
import com.twitter.zipkin.hadoop.sources.FixedSpanSource
import java.sql.Timestamp
import java.text.SimpleDateFormat
import cascading.pipe.joiner.LeftJoin

class FindSpansInCommon(args : Args) extends Job(args) {

  val file = TextLine(args.required("file"))
  val logs = TextLine(args.required("logs"))

  val infoFromFile = file
    .read
    .mapTo('line -> ('service, 'timestamp)) { line : String =>
    val text = line.split("\t")
    //      val ts = new Timestamp( augmentString(text(1)).toLong / 1000)
    (text(0), text(1))
  }

  val infoFromLogs = logs
    .read
    .mapTo('line -> ('service_logs, 'timestamp_logs)) { line : String =>
    val text = line.split("\t")
    //      val ts = new Timestamp( augmentString(text(1)).toLong / 1000)
    (text(0), text(1))
  }

  val result = infoFromFile
    .joinWithSmaller('service -> 'service_logs, infoFromLogs, joiner = new LeftJoin())
    .filter('service_logs){ s : String => s == null }
    .write(Tsv(args("output")))
}