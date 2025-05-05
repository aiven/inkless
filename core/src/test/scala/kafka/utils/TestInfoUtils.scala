/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.utils

import java.lang.reflect.Method
import java.util
import java.util.{Optional, Collections}
import org.junit.jupiter.api.TestInfo
import org.apache.kafka.clients.consumer.GroupProtocol
import org.junit.jupiter.api.Tag

class EmptyTestInfo extends TestInfo {
  override def getDisplayName: String = ""
  override def getTags: util.Set[String] = Collections.emptySet()
  override def getTestClass: Optional[Class[_]] = Optional.empty()
  override def getTestMethod: Optional[Method] = Optional.empty()
}

object TestInfoUtils {
  
  final val TestWithParameterizedQuorumAndGroupProtocolNames = "{displayName}.quorum={0}.groupProtocol={1}"

  final val TestWithParameterizedGroupProtocolNames = "{displayName}.groupProtocol={0}"

  def isShareGroupTest(testInfo: TestInfo): Boolean = {
    testInfo.getDisplayName.contains("kip932")
  }

  def maybeGroupProtocolSpecified(testInfo: TestInfo): Option[GroupProtocol] = {
    if (testInfo.getDisplayName.contains("groupProtocol=classic"))
      Some(GroupProtocol.CLASSIC)
    else if (testInfo.getDisplayName.contains("groupProtocol=consumer"))
      Some(GroupProtocol.CONSUMER)
    else
      None
  }

  /**
   * Returns whether transaction version 2 is enabled.
   * When no parameter is provided, the default returned is true.
   */
  def isTransactionV2Enabled(testInfo: TestInfo): Boolean = {
    !testInfo.getDisplayName.contains("isTV2Enabled=false")
  }

  /**
   * Returns whether eligible leader replicas version 1 is enabled.
   * When no parameter is provided, the default returned is false.
   */
  def isEligibleLeaderReplicasV1Enabled(testInfo: TestInfo): Boolean = {
    testInfo.getDisplayName.contains("isELRV1Enabled=true")
  }

  /**
   * Returns whether the test should use Inkless topics, which is true when all of the following are true:
   * <ul>
   *   <li>-Dkafka.log.inkless.enable is unspecified or =true
   *   <li>The test or class has the "inkless" tag
   *   <li>the test or class does not have has the "noinkless" tag
   * </ul>
   * This does not use the typical test harness parameterization to avoid regular merge conflicts.
   */
  def isInklessEnabled(testInfo: TestInfo): Boolean = {
    val propertyEnabled = System.getProperty("kafka.log.inkless.enable", "").toBooleanOption.getOrElse(true)

    val hasInklessTag = testInfo.getTestMethod
      .filter(method => method.isAnnotationPresent(classOf[Tag]) &&
        method.getAnnotation(classOf[Tag]).value().contains("inkless"))
      .isPresent || testInfo.getTags.contains("inkless")

    val hasNoInklessTag = testInfo.getTestMethod
      .filter(method => method.isAnnotationPresent(classOf[Tag]) &&
        method.getAnnotation(classOf[Tag]).value().contains("noinkless"))
      .isPresent || testInfo.getTags.contains("noinkless")

    propertyEnabled && hasInklessTag && !hasNoInklessTag
  }
}
