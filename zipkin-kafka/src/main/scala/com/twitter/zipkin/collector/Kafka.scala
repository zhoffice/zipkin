package com.twitter.zipkin.collector

import com.twitter.finagle.Service
import com.twitter.util.Future
import com.twitter.zipkin.common.Span
import com.twitter.zipkin.gen
import kafka.producer.{ProducerData, Producer}
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}

class Kafka(
  kafka: Producer[String, gen.Span],
  topic: String = "zipkin",
  statsReceiver: StatsReceiver = NullStatsReceiver
) extends Service[Span, Unit] {

  def apply(req: Span): Future[Unit] = {
    statsReceiver.counter("sample_receive").incr()
    val producerData = new ProducerData[String, gen.Span](topic, Seq(ThriftAdapter(req)))
    Future {
      kafka.send(producerData)
    } onSuccess { (_) =>
      statsReceiver.counter("sample_success").incr()
    }
  }
}

