/*
 *
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
 *
 */

package org.apache.samza.system.kafka

import java.util.Properties

import kafka.admin.AdminUtils
import kafka.common.ErrorMapping
import kafka.consumer.{Consumer, ConsumerConfig}
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.{ZkUtils, TestUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.security.JaasUtils
import org.apache.samza.Partition
import org.apache.samza.config.KafkaProducerConfig
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import org.apache.samza.system.{SystemStreamMetadata, SystemStreamPartition}
import org.apache.samza.util.{ClientUtilTopicMetadataStore, ExponentialSleepStrategy, KafkaUtil, TopicMetadataStore}
import org.junit.Assert._
import org.junit.{Ignore, AfterClass, BeforeClass, Test}

import scala.collection.JavaConversions._

object TestKafkaSystemAdmin extends KafkaServerTestHarness {
  val TOPIC = "input"
  val TOTAL_PARTITIONS = 50
  val REPLICATION_FACTOR = 2
  val zkSecure = JaasUtils.isZkSecurityEnabled()

  protected def numBrokers: Int = 3

  var producer: KafkaProducer[Array[Byte], Array[Byte]] = null
  var producerConfig: KafkaProducerConfig = null
  var brokers: String = null

  var metadataStore: TopicMetadataStore = null

  def generateConfigs() = {
    val props = TestUtils.createBrokerConfigs(numBrokers, zkConnect, true)
    props.map(KafkaConfig.fromProps)
  }

  @BeforeClass
  override def setUp {
    super.setUp

    val config = new java.util.HashMap[String, Object]()

    brokers = brokerList.split(",").map(p => "localhost" + p).mkString(",")

    config.put("bootstrap.servers", brokers)
    config.put("acks", "all")
    config.put("serializer.class", "kafka.serializer.StringEncoder")

    producerConfig = new KafkaProducerConfig("kafka", "i001", config)

    producer = new KafkaProducer[Array[Byte], Array[Byte]](producerConfig.getProducerProperties)
    metadataStore = new ClientUtilTopicMetadataStore(brokers, "some-job-name")
  }

  @AfterClass
  override def tearDown {
    super.tearDown
  }

  def createTopic {
    AdminUtils.createTopic(
      zkUtils,
      TOPIC,
      TOTAL_PARTITIONS,
      REPLICATION_FACTOR)
  }

  def validateTopic(expectedPartitionCount: Int) {
    var done = false
    var retries = 0
    val maxRetries = 100

    while (!done && retries < maxRetries) {
      try {
        val topicMetadataMap = TopicMetadataCache.getTopicMetadata(Set(TOPIC), "kafka", metadataStore.getTopicInfo)
        val topicMetadata = topicMetadataMap(TOPIC)
        val errorCode = topicMetadata.errorCode

        KafkaUtil.maybeThrowException(errorCode)

        done = expectedPartitionCount == topicMetadata.partitionsMetadata.size
      } catch {
        case e: Exception =>
          System.err.println("Got exception while validating test topics. Waiting and retrying.", e)
          retries += 1
          Thread.sleep(500)
      }
    }

    if (retries >= maxRetries) {
      fail("Unable to successfully create topics. Tried to validate %s times." format retries)
    }
  }

  def getConsumerConnector = {
    val props = new Properties

    props.put("zookeeper.connect", zkConnect)
    props.put("group.id", "test")
    props.put("auto.offset.reset", "smallest")

    val consumerConfig = new ConsumerConfig(props)
    Consumer.create(consumerConfig)
  }

}

/**
 * Test creates a local ZK and Kafka cluster, and uses it to create and test
 * topics for to verify that offset APIs in SystemAdmin work as expected.
 */
@Ignore
class TestKafkaSystemAdmin {
  import TestKafkaSystemAdmin._

  val systemName = "test"
  // Provide a random zkAddress, the system admin tries to connect only when a topic is created/validated
  val systemAdmin = new KafkaSystemAdmin(systemName, brokers, connectZk = () => ZkUtils.createZkClient(zkConnect, 6000, 6000))

  def testShouldAssembleMetadata {
    val oldestOffsets = Map(
      new SystemStreamPartition("test", "stream1", new Partition(0)) -> "o1",
      new SystemStreamPartition("test", "stream2", new Partition(0)) -> "o2",
      new SystemStreamPartition("test", "stream1", new Partition(1)) -> "o3",
      new SystemStreamPartition("test", "stream2", new Partition(1)) -> "o4")
    val newestOffsets = Map(
      new SystemStreamPartition("test", "stream1", new Partition(0)) -> "n1",
      new SystemStreamPartition("test", "stream2", new Partition(0)) -> "n2",
      new SystemStreamPartition("test", "stream1", new Partition(1)) -> "n3",
      new SystemStreamPartition("test", "stream2", new Partition(1)) -> "n4")
    val upcomingOffsets = Map(
      new SystemStreamPartition("test", "stream1", new Partition(0)) -> "u1",
      new SystemStreamPartition("test", "stream2", new Partition(0)) -> "u2",
      new SystemStreamPartition("test", "stream1", new Partition(1)) -> "u3",
      new SystemStreamPartition("test", "stream2", new Partition(1)) -> "u4")
    val metadata = KafkaSystemAdmin.assembleMetadata(oldestOffsets, newestOffsets, upcomingOffsets)
    assertNotNull(metadata)
    assertEquals(2, metadata.size)
    assertTrue(metadata.contains("stream1"))
    assertTrue(metadata.contains("stream2"))
    val stream1Metadata = metadata("stream1")
    val stream2Metadata = metadata("stream2")
    assertNotNull(stream1Metadata)
    assertNotNull(stream2Metadata)
    assertEquals("stream1", stream1Metadata.getStreamName)
    assertEquals("stream2", stream2Metadata.getStreamName)
    val expectedSystemStream1Partition0Metadata = new SystemStreamPartitionMetadata("o1", "n1", "u1")
    val expectedSystemStream1Partition1Metadata = new SystemStreamPartitionMetadata("o3", "n3", "u3")
    val expectedSystemStream2Partition0Metadata = new SystemStreamPartitionMetadata("o2", "n2", "u2")
    val expectedSystemStream2Partition1Metadata = new SystemStreamPartitionMetadata("o4", "n4", "u4")
    val stream1PartitionMetadata = stream1Metadata.getSystemStreamPartitionMetadata
    val stream2PartitionMetadata = stream2Metadata.getSystemStreamPartitionMetadata
    assertEquals(expectedSystemStream1Partition0Metadata, stream1PartitionMetadata.get(new Partition(0)))
    assertEquals(expectedSystemStream1Partition1Metadata, stream1PartitionMetadata.get(new Partition(1)))
    assertEquals(expectedSystemStream2Partition0Metadata, stream2PartitionMetadata.get(new Partition(0)))
    assertEquals(expectedSystemStream2Partition1Metadata, stream2PartitionMetadata.get(new Partition(1)))
  }

  @Test
  def testShouldGetOldestNewestAndNextOffsets {

    // Create an empty topic with 50 partitions, but with no offsets.
    createTopic
    validateTopic(50)

    // Verify the empty topic behaves as expected.
    var metadata = systemAdmin.getSystemStreamMetadata(Set(TOPIC))
    assertEquals(1, metadata.size)
    assertNotNull(metadata(TOPIC))
    // Verify partition count.
    var sspMetadata = metadata(TOPIC).getSystemStreamPartitionMetadata
    assertEquals(50, sspMetadata.size)
    // Empty topics should have null for earliest/latest offset.
    assertNull(sspMetadata.get(new Partition(0)).getOldestOffset)
    assertNull(sspMetadata.get(new Partition(0)).getNewestOffset)
    // Empty Kafka topics should have a next offset of 0.
    assertEquals("0", sspMetadata.get(new Partition(0)).getUpcomingOffset)

    // Add a new message to one of the partitions, and verify that it works as 
    // expected.
    producer.send(new ProducerRecord(TOPIC, 48, "key1".getBytes, "val1".getBytes)).get()
    metadata = systemAdmin.getSystemStreamMetadata(Set(TOPIC))
    assertEquals(1, metadata.size)
    val streamName = metadata.keySet.head
    assertEquals(TOPIC, streamName)
    sspMetadata = metadata(streamName).getSystemStreamPartitionMetadata
    // key1 gets hash-mod'd to partition 48.
    assertEquals("0", sspMetadata.get(new Partition(48)).getOldestOffset)
    assertEquals("0", sspMetadata.get(new Partition(48)).getNewestOffset)
    assertEquals("1", sspMetadata.get(new Partition(48)).getUpcomingOffset)
    // Some other partition should be empty.
    assertNull(sspMetadata.get(new Partition(3)).getOldestOffset)
    assertNull(sspMetadata.get(new Partition(3)).getNewestOffset)
    assertEquals("0", sspMetadata.get(new Partition(3)).getUpcomingOffset)

    // Add a second message to one of the same partition.
    producer.send(new ProducerRecord(TOPIC, 48, "key1".getBytes, "val2".getBytes)).get()
    metadata = systemAdmin.getSystemStreamMetadata(Set(TOPIC))
    assertEquals(1, metadata.size)
    assertEquals(TOPIC, streamName)
    sspMetadata = metadata(streamName).getSystemStreamPartitionMetadata
    // key1 gets hash-mod'd to partition 48.
    assertEquals("0", sspMetadata.get(new Partition(48)).getOldestOffset)
    assertEquals("1", sspMetadata.get(new Partition(48)).getNewestOffset)
    assertEquals("2", sspMetadata.get(new Partition(48)).getUpcomingOffset)

    // Validate that a fetch will return the message.
    val connector = getConsumerConnector
    var stream = connector.createMessageStreams(Map(TOPIC -> 1)).get(TOPIC).get.get(0).iterator
    var message = stream.next
    var text = new String(message.message, "UTF-8")
    connector.shutdown
    // First message should match the earliest expected offset.
    assertEquals(sspMetadata.get(new Partition(48)).getOldestOffset, message.offset.toString)
    assertEquals("val1", text)
    // Second message should match the earliest expected offset.
    message = stream.next
    text = new String(message.message, "UTF-8")
    assertEquals(sspMetadata.get(new Partition(48)).getNewestOffset, message.offset.toString)
    assertEquals("val2", text)
  }

  @Test
  def testNonExistentTopic {

    val initialOffsets = systemAdmin.getSystemStreamMetadata(Set("non-existent-topic"))
    val metadata = initialOffsets.getOrElse("non-existent-topic", fail("missing metadata"))
    assertEquals(metadata, new SystemStreamMetadata("non-existent-topic", Map(
      new Partition(0) -> new SystemStreamPartitionMetadata(null, null, "0"))))
  }

  @Test
  def testOffsetsAfter {
    val ssp1 = new SystemStreamPartition("test-system", "test-stream", new Partition(0))
    val ssp2 = new SystemStreamPartition("test-system", "test-stream", new Partition(1))
    val offsetsAfter = systemAdmin.getOffsetsAfter(Map(
      ssp1 -> "1",
      ssp2 -> "2"))
    assertEquals("2", offsetsAfter(ssp1))
    assertEquals("3", offsetsAfter(ssp2))
  }

  class KafkaSystemAdminWithTopicMetadataError extends KafkaSystemAdmin("test", brokers, connectZk = () => ZkUtils.createZkClient(zkConnect, 6000, 6000)) {
    import kafka.api.TopicMetadata

    // Simulate Kafka telling us that the leader for the topic is not available
    override def getTopicMetadata(topics: Set[String]) = {
      val topicMetadata = TopicMetadata(topic = "quux", partitionsMetadata = Seq(), errorCode = ErrorMapping.LeaderNotAvailableCode)
      Map("quux" -> topicMetadata)
    }
  }

  @Test
  def testShouldRetryOnTopicMetadataError {
    val systemAdmin = new KafkaSystemAdminWithTopicMetadataError
    val retryBackoff = new ExponentialSleepStrategy.Mock(maxCalls = 3)
    try {
      systemAdmin.getSystemStreamMetadata(Set("quux"), retryBackoff)
      fail("expected CallLimitReached to be thrown")
    } catch {
      case e: ExponentialSleepStrategy.CallLimitReached => ()
    }
  }
}
