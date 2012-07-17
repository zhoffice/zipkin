package com.twitter.zipkin.collector.processor

/*
 * Copyright 2012 Twitter Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

import com.twitter.zipkin.common.Span
import com.twitter.util.Future
import com.twitter.logging.Logger

/**
 * Adds server side duration data to ostrich, which
 * in turn can be sent to a monitoring system where it can be queried.
 */
class LogSpanIdProcessor extends ProcessorFilter[Seq[Span], Seq[Span]] {

  val logger = Logger.get("spanid")

  def apply(spans: Seq[Span]): Seq[Span] = {
    spans foreach { span =>
      logger.info(span.id + " " + span.firstAnnotation.map(_.timestamp).getOrElse(0))
    }
    spans
  }

  def shutdown() {}
}