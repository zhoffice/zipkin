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

import com.twitter.common.zookeeper.{ZooKeeperClient, Group}
import com.twitter.conversions.time._
import com.twitter.util.Timer
import com.twitter.zipkin.collector.sampler.adaptive.policy.LeaderPolicy
import com.twitter.zipkin.config.sampler.adaptive.ZooKeeperAdaptiveSamplerConfig
import com.twitter.zipkin.config.sampler.AdjustableRateConfig
import org.apache.zookeeper.ZooKeeper
import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers._
import org.scalatest.mock.MockitoSugar._
import org.mockito.Mockito.{never, times, verify, when}
import org.mockito.Matchers.any
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class ZooKeeperAdaptiveLeaderSpec extends WordSpec {
  val samplerTimer = mock[Timer]

  "ZooKeeperAdaptiveLeader" should {
    val _zk                       = mock[ZooKeeper]
    val _zkClient                 = mock[ZooKeeperClient]
    val _buf                      = mock[BoundedBuffer]
    val _reportGroup              = mock[Group]
    val _leaderPolicy             = mock[LeaderPolicy[BoundedBuffer]]

    val _config = mock[ZooKeeperAdaptiveSamplerConfig]
    val _sampleRateConfig = mock[AdjustableRateConfig]

    val _leaderGroup: Group = null
    val _windowSize = 30.minutes
    val _windowSufficient = 10.minutes
    val _pollInterval = 1.minute

    val _reportPath = "/twitter/service/zipkin/adaptivesampler/report"

    def adaptiveLeader: ZooKeeperAdaptiveLeader =
      new ZooKeeperAdaptiveLeader {
        val config                      = _config
        val reportGroup                 = _reportGroup
        val leaderGroup                 = _leaderGroup
        val bufferSize                  = _windowSize
        val windowSufficient            = _windowSufficient
        val pollInterval                = _pollInterval
        val leaderPolicy                = _leaderPolicy

        override lazy val buf = _buf
      }

    "update" in {
      val leader = adaptiveLeader

      val ids = Seq[String]("1", "2", "3")
      val sum = 600.0
      val expectedUpdate = sum.toLong

      when(_config.client).thenReturn(_zkClient)
      when(_zkClient.get).thenReturn(_zk)
      when(_reportGroup.getMemberIds).thenReturn(ids.asJava)
      ids.foreach { id =>
        when(_reportGroup.getMemberPath(id)).thenReturn((_reportPath + "/" + id))
      }

      when(_zk.getData(_reportPath + "/1", true, null)).thenReturn("100".getBytes)
      when(_zk.getData(_reportPath + "/2", true, null)).thenReturn("200".getBytes)
      when(_zk.getData(_reportPath + "/3", true, null)).thenReturn("300".getBytes)

      leader.update()
      verify(_buf, times(1)).update(expectedUpdate)
    }

    "do nothing if policy returns None" in {
      val leader = adaptiveLeader

      when(_leaderPolicy.apply(Some(_buf))).thenReturn(None)

      leader.lead()
    }

    "adjust sample rate if policy returns valid option" in {
      val leader = adaptiveLeader
      val newSampleRate = 0.1

      when(_config.sampleRate).thenReturn(_sampleRateConfig)
      when(_leaderPolicy.apply(Some(_buf))).thenReturn(Some(newSampleRate))
      leader.lead()
      verify(_sampleRateConfig, times(1)).set(newSampleRate)
      verify(_leaderPolicy, times(1)).notifyChange(newSampleRate)
    }

    "truncate" in {
      ZooKeeperAdaptiveLeader.truncate(0.1110) must equal (0.111)
      ZooKeeperAdaptiveLeader.truncate(0.1111) must equal (0.111)
      ZooKeeperAdaptiveLeader.truncate(0.1115) must equal (0.111)
      ZooKeeperAdaptiveLeader.truncate(0.1119) must equal (0.111)
      ZooKeeperAdaptiveLeader.truncate(0.1120) must equal (0.112)
    }
  }
}
