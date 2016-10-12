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

package org.apache.samza.system.kafka

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.errors.RecordTooLargeException
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.samza.SamzaException
import org.apache.samza.system.{OutgoingMessageEnvelope, SystemStream}
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions.intercept


class TestKafkaSystemProducer {

  val someMessage = new OutgoingMessageEnvelope(new SystemStream("test", "test"), "test".getBytes)

  @Test
  def testKafkaProducer {
    val systemProducer = new KafkaSystemProducer(systemName = "test",
                                           getProducer = () => { new MockProducer(true, new ByteArraySerializer, new ByteArraySerializer) },
                                           metrics = new KafkaSystemProducerMetrics)
    systemProducer.register("test")
    systemProducer.start
    systemProducer.send("test", someMessage)
    assertEquals(1, systemProducer.producer.asInstanceOf[MockProducer[Array[Byte], Array[Byte]]].history().size())
    systemProducer.stop
  }

  @Test
  def testKafkaProducerUsingMockKafkaProducer {
    val mockProducer = new MockKafkaProducer(1, "test", 1)
    val systemProducer = new KafkaSystemProducer(systemName = "test",
                                                 getProducer = () => mockProducer,
                                                 metrics = new KafkaSystemProducerMetrics)
    systemProducer.register("test")
    systemProducer.start()
    systemProducer.send("test", someMessage)
    assertEquals(1, mockProducer.getMsgsSent)
    systemProducer.stop()
  }

  @Test
  def testKafkaProducerBufferedSend {
    val msg1 = new OutgoingMessageEnvelope(new SystemStream("test", "test"), "a".getBytes)
    val msg2 = new OutgoingMessageEnvelope(new SystemStream("test", "test"), "b".getBytes)
    val msg3 = new OutgoingMessageEnvelope(new SystemStream("test", "test"), "c".getBytes)

    val mockProducer = new MockKafkaProducer(1, "test", 1)
    val systemProducer = new KafkaSystemProducer(systemName = "test",
                                                 getProducer = () => mockProducer,
                                                 metrics = new KafkaSystemProducerMetrics)
    systemProducer.register("test")
    systemProducer.start()
    systemProducer.send("test", msg1)

    mockProducer.setShouldBuffer(true)
    systemProducer.send("test", msg2)
    systemProducer.send("test", msg3)
    assertEquals(1, mockProducer.getMsgsSent)

    val sendThread: Thread = mockProducer.startDelayedSendThread(2000)
    sendThread.join()

    assertEquals(3, mockProducer.getMsgsSent)
    systemProducer.stop()
  }

  @Test
  def testKafkaProducerFlushSuccessful {
    val msg1 = new OutgoingMessageEnvelope(new SystemStream("test", "test"), "a".getBytes)
    val msg2 = new OutgoingMessageEnvelope(new SystemStream("test", "test"), "b".getBytes)
    val msg3 = new OutgoingMessageEnvelope(new SystemStream("test", "test"), "c".getBytes)

    val mockProducer = new MockKafkaProducer(1, "test", 1)
    val systemProducer = new KafkaSystemProducer(systemName = "test",
                                                 getProducer = () => mockProducer,
                                                 metrics = new KafkaSystemProducerMetrics)
    systemProducer.register("test")
    systemProducer.start()
    systemProducer.send("test", msg1)

    mockProducer.setShouldBuffer(true)
    systemProducer.send("test", msg2)
    systemProducer.send("test", msg3)
    assertEquals(1, mockProducer.getMsgsSent)
    mockProducer.startDelayedSendThread(2000)
    systemProducer.flush("test")
    assertEquals(3, mockProducer.getMsgsSent)
    systemProducer.stop()
  }

  @Test
  def testKafkaProducerFlushWithException {
    val msg1 = new OutgoingMessageEnvelope(new SystemStream("test", "test"), "a".getBytes)
    val msg2 = new OutgoingMessageEnvelope(new SystemStream("test", "test"), "b".getBytes)
    val msg3 = new OutgoingMessageEnvelope(new SystemStream("test", "test"), "c".getBytes)
    val msg4 = new OutgoingMessageEnvelope(new SystemStream("test", "test"), "d".getBytes)

    val mockProducer = new MockKafkaProducer(1, "test", 1)
    val systemProducer = new KafkaSystemProducer(systemName = "test",
                                                 getProducer = () => mockProducer,
                                                 metrics = new KafkaSystemProducerMetrics)
    systemProducer.register("test")
    systemProducer.start()
    systemProducer.send("test", msg1)

    mockProducer.setShouldBuffer(true)
    systemProducer.send("test", msg2)
    mockProducer.setErrorNext(true, new RecordTooLargeException())
    systemProducer.send("test", msg3)
    systemProducer.send("test", msg4)

    assertEquals(1, mockProducer.getMsgsSent)
    mockProducer.startDelayedSendThread(2000)
    val thrown = intercept[SamzaException] {
      systemProducer.flush("test")
    }
    assertTrue(thrown.isInstanceOf[SamzaException])
    assertEquals(2, mockProducer.getMsgsSent)
    systemProducer.stop()
  }

  @Test
  def testKafkaProducerExceptions {
    val msg1 = new OutgoingMessageEnvelope(new SystemStream("test", "test"), "a".getBytes)
    val msg2 = new OutgoingMessageEnvelope(new SystemStream("test", "test"), "b".getBytes)
    val msg3 = new OutgoingMessageEnvelope(new SystemStream("test", "test"), "c".getBytes)
    val msg4 = new OutgoingMessageEnvelope(new SystemStream("test", "test"), "d".getBytes)

    val mockProducer = new MockKafkaProducer(1, "test", 1)
    val producer = new KafkaSystemProducer(systemName =  "test",
                                           getProducer = () => mockProducer,
                                           metrics = new KafkaSystemProducerMetrics)
    producer.register("test")
    producer.start()
    producer.send("test", msg1)
    producer.send("test", msg2)
    producer.send("test", msg3)
    mockProducer.setErrorNext(true, new RecordTooLargeException())

    val thrown = intercept[SamzaException] {
       producer.send("test", msg4)
    }
    assertTrue(thrown.isInstanceOf[SamzaException])
    assertTrue(thrown.getCause.isInstanceOf[RecordTooLargeException])
    assertEquals(true, producer.sendFailed.get())
    assertEquals(3, mockProducer.getMsgsSent)
    producer.stop()
  }

  @Test
  def testKafkaProducerFlushMsgsWhenStop {
    val msg1 = new OutgoingMessageEnvelope(new SystemStream("test", "test"), "a".getBytes)
    val msg2 = new OutgoingMessageEnvelope(new SystemStream("test", "test"), "b".getBytes)
    val msg3 = new OutgoingMessageEnvelope(new SystemStream("test", "test"), "c".getBytes)
    val msg4 = new OutgoingMessageEnvelope(new SystemStream("test2", "test"), "d".getBytes)

    val mockProducer = new MockKafkaProducer(1, "test", 1)
    val systemProducer = new KafkaSystemProducer(systemName = "test",
                                                 getProducer = () => mockProducer,
                                                 metrics = new KafkaSystemProducerMetrics)
    systemProducer.register("test")
    systemProducer.register("test2")
    systemProducer.start()
    systemProducer.send("test", msg1)

    mockProducer.setShouldBuffer(true)
    systemProducer.send("test", msg2)
    systemProducer.send("test", msg3)
    systemProducer.send("test2", msg4)
    assertEquals(1, mockProducer.getMsgsSent)
    mockProducer.startDelayedSendThread(2000)
    assertEquals(1, mockProducer.getMsgsSent)
    systemProducer.stop()
    assertEquals(4, mockProducer.getMsgsSent)
  }
}
