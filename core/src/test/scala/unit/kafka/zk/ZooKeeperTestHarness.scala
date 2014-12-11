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

package kafka7.zk

import org.scalatest.junit.JUnit3Suite
import org.I0Itec.zkclient.ZkClient
import kafka7.utils.ZKStringSerializer

trait ZooKeeperTestHarness extends JUnit3Suite {
  val zkConnect: String
  var zookeeper: EmbeddedZookeeper = null
  var zkClient: ZkClient = null

  override def setUp() {
    zookeeper = new EmbeddedZookeeper(zkConnect)
    zkClient = new ZkClient(zookeeper.connectString)
    zkClient.setZkSerializer(ZKStringSerializer)
    super.setUp
  }

  override def tearDown() {
    super.tearDown
    zkClient.close()
    zookeeper.shutdown()
  }

}
