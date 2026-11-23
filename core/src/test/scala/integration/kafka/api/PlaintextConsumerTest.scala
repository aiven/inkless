/**
  * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
  * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
  * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
  * License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package kafka.api

import java.util
import java.time.Duration
import kafka.server.GroupProtocolAndMaybeTopicTypeProvider
import kafka.utils.TestInfoUtils
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Tag, Timeout}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ArgumentsSource

@Tag("inkless")
@Timeout(600)
class PlaintextConsumerTest extends BaseConsumerTest {

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNamesAndTopicType)
  @ArgumentsSource(classOf[GroupProtocolAndMaybeTopicTypeProvider])
  def testOffsetRelatedWhenTimeoutZero(groupProtocol: String, topicType: String): Unit = {
    val consumer = createConsumer()
    val result1 = consumer.beginningOffsets(util.List.of(tp), Duration.ZERO)
    assertNotNull(result1)
    assertEquals(0, result1.size())

    val result2 = consumer.endOffsets(util.List.of(tp), Duration.ZERO)
    assertNotNull(result2)
    assertEquals(0, result2.size())

    val result3 = consumer.offsetsForTimes(java.util.Map.of(tp, 0), Duration.ZERO)
    assertNotNull(result3)
    assertEquals(1, result3.size())
    assertNull(result3.get(tp))
  }
}
