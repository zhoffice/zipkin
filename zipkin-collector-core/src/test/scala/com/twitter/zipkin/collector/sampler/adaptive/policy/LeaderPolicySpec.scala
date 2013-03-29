/*
 * Copyright 2012 Twitter Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.zipkin.collector.sampler.adaptive.policy

import com.twitter.zipkin.collector.sampler.adaptive.BoundedBuffer
import com.twitter.zipkin.config.sampler.AdjustableRateConfig
import com.twitter.zipkin.config.sampler.adaptive.ZooKeeperAdaptiveSamplerConfig
import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers._
import org.scalatest.mock.MockitoSugar._
import org.mockito.Mockito.{never, times, verify, when}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

class LeaderPolicySpec extends WordSpec {

  val _config = mock[ZooKeeperAdaptiveSamplerConfig]
  val _sampleRate = mock[AdjustableRateConfig]
  val _storageRequestRate = mock[AdjustableRateConfig]

  "LeaderPolicy" should {
    "compose with filter" in {
      val default = 0.25
      val filter = new ValidLatestValueFilter
      val policy = new PassLeaderPolicy[BoundedBuffer](default)
      val buf = new BoundedBuffer { val maxLength = 5 }
      val composed: LeaderPolicy[BoundedBuffer] = filter andThen policy


      composed(buf) must equal (None) // buf empty, so invalid value
      buf.update(-1)
      composed(buf) must equal (None) // invalid value
      buf.update(1)
      composed(buf) must equal (Some(default))

    }
  }
}
