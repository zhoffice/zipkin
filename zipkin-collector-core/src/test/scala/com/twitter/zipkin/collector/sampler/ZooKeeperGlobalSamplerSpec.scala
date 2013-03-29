package com.twitter.zipkin.collector.sampler

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

import com.twitter.zipkin.config.sampler.AdjustableRateConfig
import org.specs.mock.{ClassMocker, JMocker}
import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers._
import org.scalatest.mock.MockitoSugar._
import org.mockito.Mockito.{never, times, verify, when}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ZooKeeperGlobalSamplerSpec extends WordSpec {
  "Sample" should {

    "keep 10% of traces" in {
      val sampleRate = 0.1
      val zkConfig = mock[AdjustableRateConfig]

      when(zkConfig.get).thenReturn(sampleRate)

      val sampler = new ZooKeeperGlobalSampler(zkConfig)

      sampler(Long.MinValue) must equal (false)
      sampler(-1) must equal (true)
      sampler(0) must equal (true)
      sampler(1) must equal (true)
      sampler(Long.MaxValue) must equal (false)
    }

    "drop all traces" in {
      val zkConfig = mock[AdjustableRateConfig]
      when(zkConfig.get).thenReturn(0)

      val sampler = new ZooKeeperGlobalSampler(zkConfig)

      sampler(Long.MinValue) must equal (false)
      sampler(Long.MinValue + 1)
      -5000 to 5000 foreach { i =>
        sampler(i) must equal (false)
      }
      sampler(Long.MaxValue) must equal (false)
    }

    "keep all traces" in {
      val zkConfig = mock[AdjustableRateConfig]
      when(zkConfig.get).thenReturn(1)

      val sampler = new ZooKeeperGlobalSampler(zkConfig)

      sampler(Long.MinValue) must equal (true)
      -5000 to 5000 foreach { i =>
        sampler(i) must equal (true)
      }
      sampler(Long.MinValue) must equal (true)
    }

  }
}
