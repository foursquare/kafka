/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka7.javaapi.producer

import kafka7.producer.SyncProducerConfig
import kafka7.javaapi.message.ByteBufferMessageSet

class SyncProducer(syncProducer: kafka7.producer.SyncProducer) {

  def this(config: SyncProducerConfig) = this(new kafka7.producer.SyncProducer(config))

  val underlying = syncProducer

  def send(topic: String, partition: Int, messages: ByteBufferMessageSet) {
    import kafka7.javaapi.Implicits._
    underlying.send(topic, partition, messages)
  }

  def send(topic: String, messages: ByteBufferMessageSet): Unit = send(topic,
                                                                       kafka7.api.ProducerRequest.RandomPartition,
                                                                       messages)

  def multiSend(produces: Array[kafka7.javaapi.ProducerRequest]) {
    import kafka7.javaapi.Implicits._
    val produceRequests = new Array[kafka7.api.ProducerRequest](produces.length)
    for(i <- 0 until produces.length)
      produceRequests(i) = new kafka7.api.ProducerRequest(produces(i).topic, produces(i).partition, produces(i).messages)
    underlying.multiSend(produceRequests)
  }

  def close() {
    underlying.close
  }
}
