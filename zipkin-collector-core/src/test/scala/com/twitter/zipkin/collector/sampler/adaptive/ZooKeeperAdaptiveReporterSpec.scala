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
package com.twitter.zipkin.collector.sampler.adaptive

import com.twitter.common.zookeeper.{Group, ZooKeeperClient}
import com.twitter.zipkin.config.sampler.adaptive.ZooKeeperAdaptiveSamplerConfig
import com.twitter.zipkin.config.sampler.AdjustableRateConfig
import com.twitter.conversions.time._
import com.twitter.ostrich.stats.Counter
import com.twitter.util.Timer
import org.apache.zookeeper.{CreateMode, ZooKeeper}
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.data.Stat
import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers._
import org.scalatest.mock.MockitoSugar._
import org.mockito.Mockito.{never, times, atLeast, verify, when}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ZooKeeperAdaptiveReporterSpec extends WordSpec {

  val samplerTimer = mock[Timer]

  "ZooKeeperAdaptiveReporter" should {
    val zk = mock[ZooKeeper]
    val _zkClient = mock[ZooKeeperClient]
    val _gm = mock[Group.Membership]
    val _path = ""
    val _updateInterval = 5.seconds
    val _reportInterval = 30.seconds
    val _reportWindow = 1.minute

    val _config = new ZooKeeperAdaptiveSamplerConfig {
      var client = _zkClient
      var sampleRate: AdjustableRateConfig = null
      var storageRequestRate: AdjustableRateConfig = null
      var taskTimer = samplerTimer

      reportPath = _path
    }

    val _counter = mock[Counter]

    val stat = mock[Stat]
    val nullStat: Stat = null

    val _memberId = "1"
    val fullPath = _path + "/" + _memberId

    def adaptiveReporter = new ZooKeeperAdaptiveReporter {
      val config         = _config
      val gm             = _gm
      val counter        = _counter
      val updateInterval = _updateInterval
      val reportInterval = _reportInterval
      val reportWindow   = _reportWindow

      memberId = _memberId
    }

    "create ephemeral node" in {
      val reporter = adaptiveReporter

      when(_gm.getMemberId).thenReturn(_memberId)
      when(_zkClient.get).thenReturn(zk)
      when(zk.exists(fullPath, false)).thenReturn(nullStat)

      reporter.report()

      verify(_gm,times(1)).getMemberId
      verify(_zkClient,atLeast(1)).get
      verify(zk,times(1)).exists(fullPath, false)
      verify(zk,times(1)).create(fullPath, "0".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
    }

    "report" in {
      val reporter = adaptiveReporter

      when(_gm.getMemberId).thenReturn(_memberId)
      when(_zkClient.get).thenReturn(zk)
      when(zk.exists(fullPath, false)).thenReturn(stat)

      when(_counter.apply()).thenReturn(0)
      when(_counter.apply()).thenReturn(5)

      reporter.update()
      reporter.report()

      reporter.update()
      reporter.report()

      verify(_gm,atLeast(2)).getMemberId
      verify(_zkClient,atLeast(2)).get
      verify(zk,atLeast(2)).exists(fullPath, false)

      verify(_counter,times(2)).apply()

      verify(zk,atLeast(1)).setData(fullPath, "0.0".getBytes, -1)
      //verify(zk,atLeast(1)).setData(fullPath, "5.0".getBytes, -1)
    }
  }
}
