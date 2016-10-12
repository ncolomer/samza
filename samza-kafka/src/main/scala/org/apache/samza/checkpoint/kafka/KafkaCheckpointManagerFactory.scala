/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.checkpoint.kafka

import org.apache.samza.util.Logging
import kafka.utils.{ZkUtils, ZKStringSerializer}
import org.I0Itec.zkclient.ZkClient
import org.apache.samza.SamzaException
import org.apache.samza.checkpoint.CheckpointManager
import org.apache.samza.checkpoint.CheckpointManagerFactory
import org.apache.samza.config.Config
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.config.KafkaConfig.Config2Kafka
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.util.{ KafkaUtil, ClientUtilTopicMetadataStore }
import java.util.Properties
import scala.collection.JavaConversions._
import org.apache.kafka.clients.producer.KafkaProducer

object KafkaCheckpointManagerFactory {
  /**
   * Version number to track the format of the checkpoint log
   */
  val CHECKPOINT_LOG_VERSION_NUMBER = 1

  val INJECTED_PRODUCER_PROPERTIES = Map(
    "acks" -> "all",
    // Forcibly disable compression because Kafka doesn't support compression
    // on log compacted topics. Details in SAMZA-586.
    "compression.type" -> "none")

  // Set the checkpoint topic configs to have a very small segment size and
  // enable log compaction. This keeps job startup time small since there 
  // are fewer useless (overwritten) messages to read from the checkpoint 
  // topic.
  def getCheckpointTopicProperties(config: Config) = {
    val segmentBytes = config
      .getCheckpointSegmentBytes
      .getOrElse("26214400")

    (new Properties /: Map(
      "cleanup.policy" -> "compact",
      "segment.bytes" -> segmentBytes)) { case (props, (k, v)) => props.put(k, v); props }
  }
}

class KafkaCheckpointManagerFactory extends CheckpointManagerFactory with Logging {
  import KafkaCheckpointManagerFactory._

  def getCheckpointManager(config: Config, registry: MetricsRegistry): CheckpointManager = {
    val clientId = KafkaUtil.getClientId("samza-checkpoint-manager", config)
    val systemName = config
      .getCheckpointSystem
      .getOrElse(throw new SamzaException("no system defined for Kafka's checkpoint manager."))
    val producerConfig = config.getKafkaSystemProducerConfig(
      systemName,
      clientId,
      INJECTED_PRODUCER_PROPERTIES)
    val consumerConfig = config.getKafkaSystemConsumerConfig(systemName, clientId)
    val replicationFactor = config.getCheckpointReplicationFactor.getOrElse("3").toInt
    val socketTimeout = consumerConfig.socketTimeoutMs
    val bufferSize = consumerConfig.socketReceiveBufferBytes
    val fetchSize = consumerConfig.fetchMessageMaxBytes // must be > buffer size

    val connectProducer = () => {
      new KafkaProducer[Array[Byte], Array[Byte]](producerConfig.getProducerProperties)
    }
    val zkConnect = Option(consumerConfig.zkConnect)
      .getOrElse(throw new SamzaException("no zookeeper.connect defined in config"))
    val connectZk = () => {
      ZkUtils.createZkClient(zkConnect, 6000, 6000)
    }
    val jobName = config.getName.getOrElse(throw new SamzaException("Missing job name in configs"))
    val jobId = config.getJobId.getOrElse("1")
    val bootstrapServers = producerConfig.bootsrapServers
    val metadataStore = new ClientUtilTopicMetadataStore(bootstrapServers, clientId, socketTimeout)
    val checkpointTopic = getTopic(jobName, jobId)

    // Find out the SSPGrouperFactory class so it can be included/verified in the key
    val systemStreamPartitionGrouperFactoryString = config.getSystemStreamPartitionGrouperFactory

    new KafkaCheckpointManager(
      clientId,
      checkpointTopic,
      systemName,
      replicationFactor,
      socketTimeout,
      bufferSize,
      fetchSize,
      metadataStore,
      connectProducer,
      connectZk,
      systemStreamPartitionGrouperFactoryString,
      checkpointTopicProperties = getCheckpointTopicProperties(config))
  }

  private def getTopic(jobName: String, jobId: String) =
    "__samza_checkpoint_ver_%d_for_%s_%s" format (CHECKPOINT_LOG_VERSION_NUMBER, jobName.replaceAll("_", "-"), jobId.replaceAll("_", "-"))
}
