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

import kafka.admin.AdminUtils
import kafka.common.{InvalidMessageSizeException, UnknownTopicOrPartitionException}
import kafka.integration.KafkaServerTestHarness
import kafka.message.InvalidMessageException
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.samza.checkpoint.Checkpoint
import org.apache.samza.config.{KafkaProducerConfig, MapConfig}
import org.apache.samza.container.TaskName
import org.apache.samza.container.grouper.stream.GroupByPartitionFactory
import org.apache.samza.serializers.CheckpointSerde
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.util.{ClientUtilTopicMetadataStore, TopicMetadataStore}
import org.apache.samza.{Partition, SamzaException}
import org.junit.Assert._
import org.junit.{Before, Test}

import scala.collection.JavaConversions._
import scala.collection._

object TestKafkaCheckpointManager {
  val systemStreamPartitionGrouperFactoryString = classOf[GroupByPartitionFactory].getCanonicalName

  val checkpointTopic = "checkpoint-topic"
  val serdeCheckpointTopic = "checkpoint-topic-invalid-serde"

  val zkConnectionTimeout = 6000
  val zkSessionTimeout = 6000

  val partition = new Partition(0)
  val cp1 = new Checkpoint(Map(new SystemStreamPartition("kafka", "topic", partition) -> "123"))
  val cp2 = new Checkpoint(Map(new SystemStreamPartition("kafka", "topic", partition) -> "12345"))
}

class TestKafkaCheckpointManager extends KafkaServerTestHarness {
  import TestKafkaCheckpointManager._

  override def generateConfigs(): scala.Seq[KafkaConfig] = TestUtils
    .createBrokerConfigs(3, zkConnect, enableControlledShutdown = true)
    .map(KafkaConfig.fromProps)

  var metadataStore: TopicMetadataStore = null

  def producerConfig = {
    val config = Map[String, AnyRef](
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
      "acks" -> "all",
      ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION -> new Integer(1),
      ProducerConfig.RETRIES_CONFIG -> new Integer(java.lang.Integer.MAX_VALUE - 1)
    ) ++ KafkaCheckpointManagerFactory.INJECTED_PRODUCER_PROPERTIES
    new KafkaProducerConfig("kafka", "i001", config)
  }

  @Before
  override def setUp() = {
    super.setUp()
    metadataStore = new ClientUtilTopicMetadataStore(brokerList, "some-job-name")
  }

  @Test
  def testCheckpointShouldBeNullIfCheckpointTopicDoesNotExistShouldBeCreatedOnWriteAndShouldBeReadableAfterWrite {
    val kcm = getKafkaCheckpointManager
    val taskName = new TaskName(partition.toString)
    kcm.register(taskName)
    kcm.start
    // check that log compaction is enabled.
    val zkUtils = ZkUtils(zkConnect, 6000, 6000, isZkSecurityEnabled = false)
    val topicConfig = AdminUtils.fetchAllTopicConfigs(zkUtils)(checkpointTopic)
    zkUtils.close
    assertEquals("compact", topicConfig.get("cleanup.policy"))
    assertEquals("26214400", topicConfig.get("segment.bytes"))
    // read before topic exists should result in a null checkpoint
    var readCp = kcm.readLastCheckpoint(taskName)
    assertNull(readCp)
    // create topic the first time around
    kcm.writeCheckpoint(taskName, cp1)
    readCp = kcm.readLastCheckpoint(taskName)
    assertEquals(cp1, readCp)
    // should get an exception if partition doesn't exist
    try {
      readCp = kcm.readLastCheckpoint(new TaskName(new Partition(1).toString))
      fail("Expected a SamzaException, since only one partition (partition 0) should exist.")
    } catch {
      case e: SamzaException => None // expected
      case _: Exception => fail("Expected a SamzaException, since only one partition (partition 0) should exist.")
    }
    // writing a second message should work, too
    kcm.writeCheckpoint(taskName, cp2)
    readCp = kcm.readLastCheckpoint(taskName)
    assertEquals(cp2, readCp)
    kcm.stop
  }

  @Test
  def testUnrecoverableKafkaErrorShouldThrowKafkaCheckpointManagerException {
    val exceptions = List("InvalidMessageException", "InvalidMessageSizeException", "UnknownTopicOrPartitionException")
    exceptions.foreach { exceptionName =>
      val kcm = getKafkaCheckpointManagerWithInvalidSerde(exceptionName)
      val taskName = new TaskName(partition.toString)
      kcm.register(taskName)
      kcm.start
      kcm.writeCheckpoint(taskName, cp1)
      // because serde will throw unrecoverable errors, it should result a KafkaCheckpointException
      try {
        kcm.readLastCheckpoint(taskName)
        fail("Expected a KafkaCheckpointException.")
      } catch {
        case e: KafkaCheckpointException => None
        }
      kcm.stop
    }
  }

  private def getKafkaCheckpointManager = new KafkaCheckpointManager(
    clientId = "some-client-id",
    checkpointTopic = checkpointTopic,
    systemName = "kafka",
    replicationFactor = 3,
    socketTimeout = 30000,
    bufferSize = 64 * 1024,
    fetchSize = 300 * 1024,
    metadataStore = metadataStore,
    connectProducer = () => new KafkaProducer(producerConfig.getProducerProperties),
    connectZk = () => ZkUtils(zkConnect, 6000, 6000, isZkSecurityEnabled = false).zkClient,
    systemStreamPartitionGrouperFactoryString = systemStreamPartitionGrouperFactoryString,
    checkpointTopicProperties = KafkaCheckpointManagerFactory.getCheckpointTopicProperties(new MapConfig(Map[String, String]())))

  // inject serde. Kafka exceptions will be thrown when serde.fromBytes is called
  private def getKafkaCheckpointManagerWithInvalidSerde(exception: String) = new KafkaCheckpointManager(
    clientId = "some-client-id-invalid-serde",
    checkpointTopic = serdeCheckpointTopic,
    systemName = "kafka",
    replicationFactor = 3,
    socketTimeout = 30000,
    bufferSize = 64 * 1024,
    fetchSize = 300 * 1024,
    metadataStore = metadataStore,
    connectProducer = () => new KafkaProducer(producerConfig.getProducerProperties),
    connectZk = () => ZkUtils(zkConnect, 6000, 6000, isZkSecurityEnabled = false).zkClient,
    systemStreamPartitionGrouperFactoryString = systemStreamPartitionGrouperFactoryString,
    serde = new InvalideSerde(exception),
    checkpointTopicProperties = KafkaCheckpointManagerFactory.getCheckpointTopicProperties(new MapConfig(Map[String, String]())))

  class InvalideSerde(exception: String) extends CheckpointSerde {
    override def fromBytes(bytes: Array[Byte]): Checkpoint = {
      exception match {
        case "InvalidMessageException" => throw new InvalidMessageException
        case "InvalidMessageSizeException" => throw new InvalidMessageSizeException
        case "UnknownTopicOrPartitionException" => throw new UnknownTopicOrPartitionException
      }
    }
  }

}
