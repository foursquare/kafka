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
package kafka7.javaapi

import kafka7.serializer.Encoder
import kafka7.producer.async.QueueItem
import kafka7.utils.Logging

private[javaapi] object Implicits extends Logging {
  implicit def javaMessageSetToScalaMessageSet(messageSet: kafka7.javaapi.message.ByteBufferMessageSet):
     kafka7.message.ByteBufferMessageSet = messageSet.underlying

  implicit def scalaMessageSetToJavaMessageSet(messageSet: kafka7.message.ByteBufferMessageSet):
     kafka7.javaapi.message.ByteBufferMessageSet = {
    new kafka7.javaapi.message.ByteBufferMessageSet(messageSet.getBuffer, messageSet.getInitialOffset,
                                                   messageSet.getErrorCode)
  }

  implicit def toJavaSyncProducer(producer: kafka7.producer.SyncProducer): kafka7.javaapi.producer.SyncProducer = {
    debug("Implicit instantiation of Java Sync Producer")
    new kafka7.javaapi.producer.SyncProducer(producer)
  }

  implicit def toSyncProducer(producer: kafka7.javaapi.producer.SyncProducer): kafka7.producer.SyncProducer = {
    debug("Implicit instantiation of Sync Producer")
    producer.underlying
  }

  implicit def toScalaEventHandler[T](eventHandler: kafka7.javaapi.producer.async.EventHandler[T])
       : kafka7.producer.async.EventHandler[T] = {
    new kafka7.producer.async.EventHandler[T] {
      override def init(props: java.util.Properties) { eventHandler.init(props) }
      override def handle(events: Seq[QueueItem[T]], producer: kafka7.producer.SyncProducer, encoder: Encoder[T]) {
        import collection.JavaConversions._
        eventHandler.handle(seqAsJavaList(events), producer, encoder)
      }
      override def close { eventHandler.close }
    }
  }

  implicit def toJavaEventHandler[T](eventHandler: kafka7.producer.async.EventHandler[T])
    : kafka7.javaapi.producer.async.EventHandler[T] = {
    new kafka7.javaapi.producer.async.EventHandler[T] {
      override def init(props: java.util.Properties) { eventHandler.init(props) }
      override def handle(events: java.util.List[QueueItem[T]], producer: kafka7.javaapi.producer.SyncProducer,
                          encoder: Encoder[T]) {
        import collection.JavaConversions._
        eventHandler.handle(asScalaBuffer(events), producer, encoder)
      }
      override def close { eventHandler.close }
    }
  }

  implicit def toScalaCbkHandler[T](cbkHandler: kafka7.javaapi.producer.async.CallbackHandler[T])
      : kafka7.producer.async.CallbackHandler[T] = {
    new kafka7.producer.async.CallbackHandler[T] {
      import collection.JavaConversions._
      override def init(props: java.util.Properties) { cbkHandler.init(props)}
      override def beforeEnqueue(data: QueueItem[T] = null.asInstanceOf[QueueItem[T]]): QueueItem[T] = {
        cbkHandler.beforeEnqueue(data)
      }
      override def afterEnqueue(data: QueueItem[T] = null.asInstanceOf[QueueItem[T]], added: Boolean) {
        cbkHandler.afterEnqueue(data, added)
      }
      override def afterDequeuingExistingData(data: QueueItem[T] = null): scala.collection.mutable.Seq[QueueItem[T]] = {
        cbkHandler.afterDequeuingExistingData(data)
      }
      override def beforeSendingData(data: Seq[QueueItem[T]] = null): scala.collection.mutable.Seq[QueueItem[T]] = {
        asScalaBuffer(cbkHandler.beforeSendingData(seqAsJavaList(data)))
      }
      override def lastBatchBeforeClose: scala.collection.mutable.Seq[QueueItem[T]] = {
        asScalaBuffer(cbkHandler.lastBatchBeforeClose)
      }
      override def close { cbkHandler.close }
    }
  }

  implicit def toJavaCbkHandler[T](cbkHandler: kafka7.producer.async.CallbackHandler[T])
      : kafka7.javaapi.producer.async.CallbackHandler[T] = {
    new kafka7.javaapi.producer.async.CallbackHandler[T] {
      import collection.JavaConversions._
      override def init(props: java.util.Properties) { cbkHandler.init(props)}
      override def beforeEnqueue(data: QueueItem[T] = null.asInstanceOf[QueueItem[T]]): QueueItem[T] = {
        cbkHandler.beforeEnqueue(data)
      }
      override def afterEnqueue(data: QueueItem[T] = null.asInstanceOf[QueueItem[T]], added: Boolean) {
        cbkHandler.afterEnqueue(data, added)
      }
      override def afterDequeuingExistingData(data: QueueItem[T] = null)
      : java.util.List[QueueItem[T]] = {
        seqAsJavaList(cbkHandler.afterDequeuingExistingData(data))
      }
      override def beforeSendingData(data: java.util.List[QueueItem[T]] = null)
      : java.util.List[QueueItem[T]] = {
        seqAsJavaList(cbkHandler.beforeSendingData(asScalaBuffer(data)))
      }
      override def lastBatchBeforeClose: java.util.List[QueueItem[T]] = {
        seqAsJavaList(cbkHandler.lastBatchBeforeClose)
      }
      override def close { cbkHandler.close }
    }
  }

  implicit def toMultiFetchResponse(response: kafka7.javaapi.MultiFetchResponse): kafka7.api.MultiFetchResponse =
    response.underlying

  implicit def toJavaMultiFetchResponse(response: kafka7.api.MultiFetchResponse): kafka7.javaapi.MultiFetchResponse =
    new kafka7.javaapi.MultiFetchResponse(response.buffer, response.numSets, response.offsets)
}
